
"""SharePoint files reader."""
import re
import logging
import os
import json
from typing import Any, Dict, List, Optional
from datetime import datetime
import requests
from saia_ingest.utils import change_file_extension, get_yaml_config
from urllib.parse import quote

logger = logging.getLogger(__name__)

CODE_TO_CHAR = {
    '_x007e_': '~', '_x0021_': '!', '_x0040_': '@', '_x0023_': '#',
    '_x0024_': '$', '_x0025_': '%', '_x005E_': '^', '_x0026_': '&',
    '_x002a_': '*', '_x0028_': '(', '_x0029_': ')', '_x002B_': '+',
    '_x002D_': '–', '_x003D_': '=', '_x007B_': '{', '_x007D_': '}',
    '_x003A_': ':', '_x0022_': '"', '_x007C_': '|', '_x003B_': ';',
    '_x0027_': '\'', '_x005C_': '\\', '_x003C_': '<', '_x003E_': '>',
    '_x003F_': '?', '_x002C_': ',', '_x002E_': '.', '_x002F_': '/',
    '_x0060_': '`', '_x0020_': ' '
}

class SharePointReader:
    """SharePoint reader.


    Reads folders from the SharePoint site.

    Args:
        client_id (str): The Application ID for the app registered in Microsoft Azure Portal.
            The application must also be configured with MS Graph permissions "Files.ReadAll", "Sites.ReadAll" and BrowserSiteLists.Read.All.
        client_secret (str): The application secret for the app registered in Azure.
        tenant_id (str): Unique identifier of the Azure Active Directory Instance.
        sharepoint_sites_names (Optional[Dict[str,str]]): A dictionary with Sites names as keys and a list of drives where to search for documents.
        sharepoint_metadata_policy (Optional[Dict[str, List[str]]]): Indicates how to select the metadata to save. If include key has True value, the
                                                                    values in the fileds key are keys from the custom metadata to be included. If the
                                                                    include key is False, they are exluded.
    """

    client_id: str = None
    client_secret: str = None
    tenant_id: str = None
    sharepoint_sites_names: Optional[str] = None
    sharepoint_drives_names: Optional[str] = None
    _sharepoint_folder_ids: Optional[Dict] = None

    _authorization_headers: str = None
    _site_id_with_host_name: str = None
    _drives_id_endpoint: str = None
    _drives_ids: str = None

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        sharepoint_sites_names: Optional[str] = None,
        sharepoint_drives_names: Optional[str] = None,
        sharepoint_metadata_policy: Optional[Dict[str, List[str]]] = None,
        sharepoint_metadata_extension:Optional[str] = '.metadata'
    ) -> None:        
        self.client_id=client_id
        self.client_secret=client_secret
        self.tenant_id=tenant_id
        self.sharepoint_sites_names=sharepoint_sites_names
        self.sharepoint_folder_path=sharepoint_drives_names
        self._sharepoint_folder_ids = {}
        self.sharepoint_metadata_policy = sharepoint_metadata_policy
        self.sharepoint_metadata_extension = sharepoint_metadata_extension

    @classmethod
    def class_name(cls) -> str:
        return "SharePointReader"

    def _get_access_token(self) -> str:
        """
        Gets the access_token for accessing file from SharePoint.

        Returns:
            str: The access_token for accessing the file.

        Raises:
            ValueError: If there is an error in obtaining the access_token.
        """
        authority = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "resource": "https://graph.microsoft.com/",
        }

        response = requests.post(
            url=authority,
            data=payload,
        )

        response_json = response.json()
        if response.status_code == 200 and "access_token" in response_json:
            token = response_json["access_token"]
            self._authorization_headers = {"Authorization": f"Bearer {token}"}
            return token

        else:
            logger.error(response_json["error"])
            raise ValueError(response_json["error_description"])

    def _get_folder_info_endpoint(self,drive_name: str, folder_id:str) -> str:
        """
        Retrieves the enpoint's URL where to get the files.

        Args:
            drive_name (str): The name of the SharePoint site.
            folder_id (str): id of the folder where to get the files. Default 'root'
        Returns:
            str: enpoint URL.

        Raises:
            Exception: If the drive_name is not found.
        """
        folder = f"items/{folder_id}" if (folder_id != '') else 'root'
        if not drive_name in self._drives_ids:
            raise ValueError(
                        f"The specified drive name {drive_name} does not exist in this site."
                    )       
        return f"{self._drives_id_endpoint}/{self._drives_ids[drive_name]}/{folder}/children"

    def _get_site_id_with_host_name(self, sharepoint_site_name:str) -> str:
        """
        Retrieves the site ID of a SharePoint site using the provided site name.

        Args:
            sharepoint_site_name (str): The name of the SharePoint site.

        Returns:
            str: The ID of the SharePoint site.

        Raises:
            Exception: If the specified SharePoint site is not found.
        """
        site_information_endpoint = (
            f"https://graph.microsoft.com/v1.0/sites?search={sharepoint_site_name}"
        )

        response = requests.get(
            url=site_information_endpoint,
            headers=self._authorization_headers,
        )
        response_json = response.json()
        if response.status_code == 200 and "value" in response_json:
            if (
                len(response_json["value"]) > 0
                and "id" in response_json["value"][0]
            ):
                self._site_id_with_host_name = response_json["value"][0]["id"]
                self._drives_id_endpoint = f"https://graph.microsoft.com/v1.0/sites/{self._site_id_with_host_name}/drives"
                return response_json["value"][0]["id"]
            else:
                raise ValueError(
                    f"The specified sharepoint site {sharepoint_site_name} is not found."
                )
        else:
            if "error_description" in response_json:
                logger.error(response_json["error"])
                raise ValueError(response_json["error_description"])
            raise ValueError(response_json["error"])

    def _get_drives_id(self) -> str:
        """
        Retrieves the drive ID of the SharePoint site.

        Returns:
            str: The ID of the SharePoint site drive.

        Raises:
            ValueError: If there is an error in obtaining the drive ID.
        """

        response = requests.get(
            url=self._drives_id_endpoint,
            headers=self._authorization_headers,
        )

        if response.status_code == 200 and "value" in response.json():
            if (
                len(response.json()["value"]) > 0
            ):
                ret = {}
                for value in response.json()["value"]:
                    ret[value["name"]] = value["id"]
                return ret
            else:
                raise ValueError(
                    "Error occurred while fetching the drives for the sharepoint site."
                )
        else:
            logger.error(response.json()["error"])
            raise ValueError(response.json()["error_description"])

    def _get_sharepoint_folder_id(self, drive_id, folder_path: str) -> str:
        """
        Retrieves the folder ID of the SharePoint site.

        Args:
            folder_path (str): The path of the folder in the SharePoint site's drive.
            drive_id  (str): SharePoint site's drive id.
        Returns:
            str: The ID of the SharePoint site folder.
        """
        if not folder_path == '':
            folder_id_endpoint = (
                f"{self._drives_id_endpoint}/{drive_id}/root:/{folder_path}"
            )

            response = requests.get(
                url=folder_id_endpoint,
                headers=self._authorization_headers,
            )

            if response.status_code == 200 and "id" in response.json():
                return response.json()["id"]
            else:
                raise ValueError(response.json()["error"])
        return ''
    
    def _download_files_and_extract_metadata_from_endpoint(
        self,
        sharepoint_drive_name: str,
        folder_info_endpoint: str,
        download_dir: str,
        deph: int = 0,
    ) -> Dict[str, str]:
        """
        Downloads files from the specified folder ID and extracts metadata.

        Args:
            sharepoint_drive_name: (str): The name of the SharePoint drive inside the site.
            folder_id (str): The ID of the folder from which the files should be downloaded.
            download_dir (str): The directory where the files should be downloaded.
            deph (int): Number of subfolders levels to retrieve. 

        Returns:
            Dict[str, str]: A dictionary containing the metadata of the downloaded files.

        Raises:
            ValueError: If there is an error in downloading the files.
        """
        
        response = requests.get(
            url=folder_info_endpoint,
            headers=self._authorization_headers,
        )

        if response.status_code == 200:
            data = response.json()
            metadata = {}
            for item in data["value"]:
                if deph > 0 and "folder" in item:
                    sub_folder_download_dir = os.path.join(download_dir, item["name"])
                    subfolder_metadata = self._download_files_and_extract_metadata_from_endpoint(
                        folder_info_endpoint=self._get_folder_info_endpoint(item["id"]),
                        download_dir=sub_folder_download_dir,
                        include_subfolders= deph - 1,
                    )

                    metadata.update(subfolder_metadata)

                elif "file" in item:
                    file_metadata = self._download_file(item, sharepoint_drive_name, download_dir)
                    metadata.update(file_metadata)
            logger.info(f"Download finished.")
            return metadata
        else:
            logger.error(response.json()["error"])
            raise ValueError(response.json()["error"])

    def _download_file_by_url(self, item: Dict[str, Any], download_path: str) -> str:
        """
        Downloads the file from the provided URL.

        Args:
            item (Dict[str, Any]): Dictionary containing file metadata.
            download_path (str): The path where the files should be downloaded.

        Returns:
            str: The path of the downloaded file in the temporary directory.
        """
        # Get the download URL for the file.
        file_download_url = item["@microsoft.graph.downloadUrl"]

        response = requests.get(file_download_url)

        with open(download_path, "wb") as f:
            f.write(response.content)

        return download_path
    
    def decode_unicode(self, data):
        if isinstance(data, dict):
            return {key: self.decode_unicode(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.decode_unicode(item) for item in data]
        elif isinstance(data, str):
            return data.encode().decode('unicode_escape')
        else:
            return data

    def _extract_metadata_for_file(self, sharepoint_drive_name, item: Dict[str, Any], metadata_path: str) -> Dict[str, str]:
        """
        Extracts metadata related to the file.

        Parameters:
        sharepoint_drive_name: (str): The name of the SharePoint drive inside the site.
        sharepoint_translation_needed (str): List of files that needs to be mapped to other values.
        - item (Dict[str, str]): Dictionary containing file metadata.

        Returns:
        - Dict[str, str]: A dictionary containing the extracted metadata.
        """
        # Extract the required metadata for file.
        id = item['id']
        response = requests.get(
            url=f"https://graph.microsoft.com/v1.0/drives/{self._drives_ids[sharepoint_drive_name]}/items/{id}/listItem?expand=fields",
            headers=self._authorization_headers,
        )
        response_text = response.text
        for code, char in CODE_TO_CHAR.items():
            response_text = response_text.replace(code, char)

        response_json = json.loads(response_text)
        parent_reference = response_json.get("parentReference")

        metadata =  {
            "file_id": id,
            "name": item.get("name"),
            "url": quote(response_json.get("webUrl"), safe=':/'),
            "parent_id": parent_reference.get("id"),
            "eTag": item.get("eTag")[1:-1],
            "lastModifiedDateTime": response_json.get("lastModifiedDateTime")
        }
        if self.sharepoint_metadata_policy is None:
            return metadata

        # Define the regex pattern to match '&(amp;)+'
        pattern = r'&(amp;)+'

        fields = {key: re.sub(pattern, '&', str(value)) for key, value in response_json.get("fields").items() if (key in self.sharepoint_metadata_policy['fields']) == self.sharepoint_metadata_policy['include_fields']}
                
        
        metadata.update(fields)
        fields_to_rename = self.sharepoint_metadata_policy['field_rename']
        
        for field in fields_to_rename:
            if field in metadata:
                metadata[fields_to_rename[field]['new_name']] = metadata[field]
                if fields_to_rename[field]['delete_old']:
                    del metadata[field]

        fields_to_translate = [value for value in self.sharepoint_metadata_policy['translation_needed'] if value in metadata]
        
        for field in fields_to_translate:
            codes = get_yaml_config(f"{self.sharepoint_metadata_policy['translation_folder']}\\{field}.yaml")
            metadata[field] = codes.get(metadata[field], '')

        for date in self.sharepoint_metadata_policy['date_fields']:
            if date in metadata:
                metadata[date] = datetime.strptime(metadata[date], self.sharepoint_metadata_policy['date_input_format']).strftime(self.sharepoint_metadata_policy['date_output_format'])
        
        metadata_path
        
        with open(metadata_path , "w") as f:
            f.write(json.dumps(metadata, indent=2))
        with open(metadata_path+'.raw' , "w") as f:
            f.write(json.dumps(response.json(), indent=2))
        return metadata

    def _download_file(
        self,
        item: Dict[str, Any],
        sharepoint_drive_name: str,
        download_dir: str,
    ):
        metadata = {}
        
        file_path = os.path.join(download_dir, item['name'])
        metadata_path = change_file_extension(file_path, self.sharepoint_metadata_extension)
        download_required = False
        # Create the directory if it does not exist and save the file.
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                try:
                    data = json.load(f)
                    download_required = data['eTag'] != item['eTag'][1:-1]
                except json.JSONDecodeError:
                    print(f"Error decoding JSON in file: {file_path}")
        if (not os.path.exists(metadata_path)) or download_required:
            self._download_file_by_url(item, file_path)
            metadata[file_path] = self._extract_metadata_for_file(sharepoint_drive_name, item, metadata_path)
        else:
            logger.info(f"{item['name']} already exist in its last version.")
        return metadata

    def download_files_from_folder(
        self,
        sharepoint_site_name: str,
        sharepoint_drive_name: str,
        depth: int,
        sharepoint_folder_path: Optional[str] = '',
        download_dir: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Downloads files from the specified folder and returns the metadata for the downloaded files.

        Args:
            download_dir (str): The directory where the files should be downloaded.
            sharepoint_site_name (str): The name of the SharePoint site.
            sharepoint_drive_name: (str): The name of the SharePoint drive inside the site.
            sharepoint_folder_path (str): The path of the folder in the sharepoint_drive_name.
            deph (int): Number of subfolders levels to retrieve. 

        Returns:
            Dict[str, str]: A dictionary containing the metadata of the downloaded files.

        """
        
        logger.info(f"Downloading files from {sharepoint_drive_name}/{sharepoint_folder_path} to {download_dir}")
        
        if not self._authorization_headers:
            self._get_access_token()

        self._get_site_id_with_host_name(
            sharepoint_site_name
        )

        self._drives_ids = self._get_drives_id()
        
        sharepoint_folder_id = self._sharepoint_folder_ids[sharepoint_folder_path] if (
            sharepoint_folder_path in self._sharepoint_folder_ids.keys()) else self._get_sharepoint_folder_id(
                sharepoint_drive_name, sharepoint_folder_path
            )

        #Check this.
        folder_info_endpoint = self._get_folder_info_endpoint(sharepoint_drive_name, sharepoint_folder_id)
                
        return self._download_files_and_extract_metadata_from_endpoint(
            sharepoint_drive_name, folder_info_endpoint, download_dir, depth
        )

    def download_file_by_id(
        self,
        sharepoint_drive_name: str,
        sharepoint_file_id: str,
        download_dir: str = None,
    ) -> Dict[str, str]:
        """
        Download file with specific id.

        Args:
            download_dir (str): The directory where the files should be downloaded.
            sharepoint_file_id (str): The id of the file to be downloaded.
            sharepoint_file_name (str): The name of the file to be downloaded.

        Returns:
            Dict[str, str]: A dictionary containing the metadata of the downloaded file.

        """
        logger.info(f"Downloading files with id '{sharepoint_file_id}' to {download_dir}")
        
        if not self._authorization_headers:
            self._get_access_token()

        if not self._drives_ids:
            self._drives_ids = self._get_drives_id()
        
        file_url = f"{self._drives_id_endpoint}/{self._drives_ids[sharepoint_drive_name]}/items/{sharepoint_file_id}"
        
        response = requests.get(
            url=file_url,
            headers=self._authorization_headers,
        )

        if response.status_code == 200:
            data = response.json()
            metadata = self._download_file(data, sharepoint_drive_name, download_dir)
            
            logger.info(f"Download finished.")
            return metadata
            
        logger.error(response.json()["error"])
        raise ValueError(response.json()["error"])
