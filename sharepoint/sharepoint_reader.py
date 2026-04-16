
"""SharePoint files reader."""
import logging
import requests
import os
import re
import json
from pathlib import Path
from saia_ingest.utils import get_configuration, load_json_file
from sharepoint.site_reader import SiteReader
from sharepoint.sharepoint_item import SharepointFileItem
import threading


logger = logging.getLogger(__name__)

class SharePointReader:
    """SharePoint reader.

    Reads SharePoint sites information acording to the configuration given.

    Args:
        connection_information (Dict): Objecto with information to connet to sharepoint.
        metadata_path (str): Path to the metadata directory for loading existing eTags.
    """
    def __init__(
        self,
        connection_information,
        metadata_path=None,
    ) -> None:        
        self.access_token = self._retrive_access_token(connection_information)
        self.sites ={}
        self.sites_lock = threading.Lock()
        self.item_generator = None
        self.items_lock = threading.Lock()
        self.metadata_path = metadata_path
        self.etag_index = {}

    @classmethod
    def class_name(cls) -> str:
        return "SharePointReader"

    def _retrive_access_token(self, connection_information) -> str:
        """
        Gets the access_token for accessing file from SharePoint.

        Returns:
            str: The access_token for accessing the file.

        Raises:
            ValueError: If there is an error in obtaining the access_token.
        """
        
        client_id = get_configuration(connection_information, 'client_id')
        client_secret = get_configuration(connection_information, 'client_secret')
        tenant_id = get_configuration(connection_information, 'tenant_id')
        
        authority = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "resource": "https://graph.microsoft.com/",
        }

        response = requests.post(
            url=authority,
            data=payload,
        )

        response_json = response.json()
        
        if (response.status_code != 200) or ("access_token" not in response_json):
            raise ValueError(response_json["error_description"])
        
        return response_json["access_token"]

    def retrieve_site_information(self, requested_site):
        new_site = SiteReader(self.access_token, requested_site)
        with self.sites_lock:
            self.sites[requested_site["name"]] = new_site
    
    def _load_existing_etags(self, metadata_path):
        """Load existing eTags from metadata files to detect unchanged files.
        
        Args:
            metadata_path (str): Path to the directory containing metadata files.
            
        Returns:
            dict: Dictionary mapping file ID to eTag {file_id: etag}
        """
        etag_index = {}
        duplicate_count = 0
        
        if not metadata_path or not os.path.exists(metadata_path):
            logger.info("No metadata path provided or path doesn't exist - processing all files")
            return etag_index
        
        metadata_folder = Path(metadata_path)
        
        for json_file in metadata_folder.glob("*.metadata"):
            try:
                with json_file.open('r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Extract SharePoint metadata section
                    if "sharepoint" in data:
                        sharepoint_data = data["sharepoint"]
                        
                        if "eTag" in sharepoint_data and "id" in sharepoint_data:
                            file_etag = sharepoint_data["eTag"]
                            file_id = sharepoint_data["id"]
                            
                            if file_id in etag_index:
                                duplicate_count += 1
                                logger.warning(f"Duplicate file ID {file_id} detected in metadata")
                            else:
                                etag_index[file_id] = file_etag
                                
            except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
                logger.warning(f"Error reading metadata file {json_file}: {e}")
        
        logger.info(f"Loaded {len(etag_index)} eTags from metadata directory")
        if duplicate_count > 0:
            logger.warning(f"{duplicate_count} duplicate file IDs found in metadata")
            
        return etag_index
            
    def init_sharepoint_item_generator(self):
        # Load existing eTags before initializing the generator
        if self.metadata_path:
            self.etag_index = self._load_existing_etags(self.metadata_path)
        self.item_generator = self._get_file_items_generator()
        
    def init_file_system_item_generator(self, path,failed_status):
        self.item_generator = self._generate_items_from_file_system(path, failed_status)
        
    def _get_file_items_generator(self):
        """Generate file items, filtering out unchanged files based on eTag comparison.
        
        Yields:
            SharepointFileItem: File items that are new or have been modified.
        """
        filtered_count = 0
        total_count = 0
        
        for site in self.sites:
            for item in self.sites[site].get_site_file_items_generator():
                total_count += 1
                
                # Check if file has unchanged eTag
                if item.id in self.etag_index:
                    existing_etag = self.etag_index[item.id]
                    if existing_etag == item.etag:
                        # File unchanged - skip processing
                        filtered_count += 1
                        logger.debug(f"Skipping unchanged file: {item.name} (ID: {item.id})")
                        continue
                    else:
                        # eTag changed - file was modified
                        logger.debug(f"File modified: {item.name} (ID: {item.id}, old eTag: {existing_etag}, new eTag: {item.etag})")
                
                yield item
        
        logger.info(f"Filtered {filtered_count} unchanged files out of {total_count} total files")
                
    def _generate_items_from_file_system(self, path, failed_status):
        regex = re.compile(r".*\.metadata$")
        for root, _, files in os.walk(path):
            for file in files:
                if regex.match(file):
                    json_item = load_json_file(os.path.join(root, file))
                    saia_information = json_item["saia"]
                    if saia_information["indexStatus"] in failed_status:
                        sharepoint_information = json_item["sharepoint"]
                        information = sharepoint_information
                        information["status"] = saia_information["indexStatus"]
                        yield SharepointFileItem(self.access_token, information)
    
    def get_next_item(self):
        with self.items_lock:
            try:
                return next(self.item_generator)
            except StopIteration:
                return None

