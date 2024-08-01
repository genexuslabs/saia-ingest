import re
import os
import json
import requests
import logging
from datetime import datetime
from saia_ingest.utils import do_get, get_yaml_config, get_configuration
from urllib.parse import quote


logger = logging.getLogger(__name__)

class SharepointFileItem:
    """This structure has the information of a Sharepoint item of type file.

    Args:
        token (str): Access token.
        item_information (Dict):  Dictionary with item information
    """
    
    def __init__(
        self,
        token,
        item_information
    ) :
        self.access_token = token
        self.web_url = self._get_web_url(item_information)
        self.parent_id = self._get_parent_id(item_information)
        self.name = item_information["name"]
        self.download_url = self._get_download_url(item_information)
        self.etag = self._get_eTAg(item_information)
        self.last_modified_date_time = self._get_last_modified_date_time(item_information)
        self.id = item_information["id"]
        self.metadata_endpoint = self._get_metadata_from_json(item_information)
        self.status = self._get_status(item_information)
        self.upload_attempts = 0
    
    def _get_web_url(self, information):
        return (quote(information["webUrl"], safe=':/')
                if "webUrl" in information
                else information["web_url"]
                )
    def _get_parent_id(self, information):
        return (information["parent_id"]
                if "parent_id" in information
                else information["parentReference"]["id"]
                )
    def _get_download_url(self, information):
        return (information["download_url"]
            if "download_url" in information
            else information["@microsoft.graph.downloadUrl"]
            )
    def _get_eTAg(self, information):
        return (information["eTag"][1:-1]
            if information["eTag"][-1] in ['\"','\'']
            else information["eTag"]
            )
    def _get_last_modified_date_time(self, information):
        return (information["lastModifiedDateTime"]
            if "lastModifiedDateTime" in information
            else information["last_modified_date_time"]
            )
    def _get_metadata_from_json(self, information):
        return (information["metadata_endpoint"]
            if "metadata_endpoint" in information
            else information["drive_endpoint"] + f"/items/{self.id}/listItem?expand=fields"
            )
    def _get_status(self, information):
        return (information["status"]
            if "status" in information
            else 'Unknown'
            )
        
    def get_name(self):
        return self.name
        
    def download(self, save_path):
        """
        Downloads the file from the provided URL.

        Args:
            item (Dict[str, Any]): Dictionary containing file metadata.
            download_path (str): The path where the files should be downloaded.

        Returns:
            str: The path of the downloaded file in the temporary directory.
        """
        # Get the download URL for the file.
        file_download_url = self.download_url

        response = requests.get(file_download_url)
        
        if response.status_code != 200:
            raise ValueError(response.json()["error"])
        
        file_path = os.path.join(save_path, self.name)
        
        with open(os.path.join(save_path, self.name), "wb") as f:
            f.write(response.content)
        
        return file_path

    def decode_response(self, response, map_to_decode):
        response_text = response.text
        for code, char in map_to_decode.items():
            response_text = response_text.replace(code, char)
        return json.loads(response_text)

    def _get_metadata_fields(self, response_json, fields_policy):
        exclude_fields = False if "exclude_fields" not in fields_policy else fields_policy["exclude_fields"]
        pattern = '' if "apply_regex_value" not in fields_policy else fields_policy["apply_regex_value"]
        
        fields_to_return = {}
        
        for key, value in response_json.get("fields").items():
            if (key in fields_policy['fields']) != exclude_fields:
                fields_to_return[key] = value if not pattern else re.sub(pattern, '&', str(value))
        
        return fields_to_return
       
    def _map_fields(self, metadata_fields, translate_policy):
        for field in translate_policy:
            if field["name"] in metadata_fields:
                codes = (
                    get_yaml_config(field["new_values"] 
                    if isinstance(field["new_values"], str) 
                    else field["new_values"])
                )
                metadata_fields[field["name"]] = codes.get(metadata_fields[field["name"]], field["default_value"])
     
    def _rename_fields(self, fields, rename_policy):
        for rename_info in rename_policy:
            old_name = rename_info["old_name"]
            if old_name in fields:
                new_name = rename_info["new_name"]
                fields[new_name] = fields[old_name]
                if "delete_old" in rename_info and rename_info["delete_old"]:
                    del fields[old_name]
    
    def _change_date_format(self, fields, format):
        for name in format["names"]:
            if name in fields:
                fields[name] = datetime.strptime(fields[name], format['input_format'])
                fields[name] = fields[name].strftime(format['output_format'])
    
    def _format_dates(self, fields, format_policy):
        for format in format_policy:
            self._change_date_format(fields, format)
                            
    def retrieve_metadata(self, processing_policy):
        
        response = do_get(self.access_token, self.metadata_endpoint)
        
        if not processing_policy:
            return response.json().get("fields", {})
        
        response_json = (
                    self.decode_response(response, processing_policy["map_to_decode"])
                    if "map_to_decode" in processing_policy
                    else response.json()
        )
        
        fields  = (
            self._get_metadata_fields(response_json, processing_policy["fields"]) 
            if "fields" in processing_policy
            else response_json.get("fields",{})
        )
        
        if "map_fields" in processing_policy:
            self._map_fields(fields, processing_policy["map_fields"])

        if "rename" in processing_policy:
            self._rename_fields(fields, processing_policy["rename"])
            
        if "dates_format" in processing_policy:
            self._format_dates(fields, processing_policy["dates_format"])
        
        metadata = {
                    "web_url" : self.web_url,
                    "parent_id" : self.parent_id,
                    "name" : self.name,
                    "download_url" : self.download_url,
                    "eTag" : self.etag,
                    "last_modified_date_time" : self.last_modified_date_time,
                    "id" : self.id,
                    "metadata_endpoint" : self.metadata_endpoint
        }
        
        metadata.update(fields)
               
        return metadata

    def set_status(self, new_status):
        self.status = new_status
        
    def increment_upload_attempts(self):
        self.upload_attempts = self.upload_attempts + 1