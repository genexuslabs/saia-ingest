import re
import os
import json
import requests
import logging
from sharepoint.sharepoint_item import SharepointFileItem

from saia_ingest.utils import do_get

logger = logging.getLogger(__name__)

class PathReader:
    """SharePoint Path reader.

    Reads informatiom from a particular Path into a SharePoint Drive.

    Args:
        token (str): Access token.
        dirve (Dict):  Dictionary with drive information
    """
    
    def __init__(
        self,
        token,
        path_information
    ) :
        self.access_token = token
        self.base_path = path_information["path"] if "path" in path_information else ""
        self.depth = path_information["depth"] if "depth" in path_information else 0
        self.drive_endpoint = path_information["drive_endpoint"]

    def _get_path_id(self, path):
        """
        Retrieves the path ID in the SharePoint Drive.

        Returns:
            str: The ID of the SharePoint site folder.
        """
        if path in ["root", "", "./", "/"]:
            return "root"

        folder_id_endpoint = (
            f"{self.drive_endpoint}/root:/{path}"
        )

        response_json = do_get(self.access_token, folder_id_endpoint).json()

        return response_json["id"]

    def _get_path_endpoint(self, id):
        """
        Retrieves the enpoint's URL where to get folder's items.

        Returns:
            str: enpoint URL.

        """
        folder = f"items/{id}" if (id != "root") else "root"      
        return f"{self.drive_endpoint}/{folder}/children"
       
    def _split_files_and_folders_items(self, items_data, allowed_depth):
        files = []
        folders = []
        for item in items_data["value"]:
            if allowed_depth > 0 and "folder" in item:
                folders.append({"id": item["id"], "depth": allowed_depth -1})
            else:
                item["drive_endpoint"] = self.drive_endpoint
                files.append(SharepointFileItem(self.access_token, item))
        return files, folders
        
    def _get_items_from_path(self, path_id, allowed_depth):
        """
        Downloads items from path

        Returns:
            List[SharepointFileItem]: A list containing all the items downloaded.

        Raises:
            ValueError: If there is an error in downloading the files.
        """
        if allowed_depth < 0:
            return [], []
        
        path_endpoint = self._get_path_endpoint(path_id)
        items_data = do_get(self.access_token, path_endpoint).json()
        return self._split_files_and_folders_items(items_data, allowed_depth - 1)
    
    def get_file_items_generator(self):
        files, folders = self._get_items_from_path(self._get_path_id(self.base_path), self.depth)
        while files:
            yield files.pop(0)
            if (not files) and folders:
                folder = folders.pop(0)
                files, new_folders = self._get_items_from_path(folder["id"], folder["depth"])
                folders = folders + new_folders
    