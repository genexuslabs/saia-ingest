
"""SharePoint files reader."""
import logging
import requests
import os
import re
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
    """
    def __init__(
        self,
        connection_information,
    ) -> None:        
        self.access_token = self._retrive_access_token(connection_information)
        self.sites ={}
        self.sites_lock = threading.Lock()
        self.item_generator = None
        self.items_lock = threading.Lock()        

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
            
    def init_sharepoint_item_generator(self):
        self.item_generator = self._get_file_items_generator()
        
    def init_file_system_item_generator(self, path,failed_status):
        self.item_generator = self._generate_items_from_file_system(path, failed_status)
        
    def _get_file_items_generator(self):
        for site in self.sites:
            for item in self.sites[site].get_site_file_items_generator():
                yield item
                
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

