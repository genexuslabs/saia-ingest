import re
import os
import json
import requests
import logging
from sharepoint.path_reader import PathReader

logger = logging.getLogger(__name__)

class DriveReader:
    """SharePoint Drive reader.

    Reads informatiom from a SharePoint Drive inside a Site.

    Args:
        token (str): Access token.
        dirve (Dict):  Dictionary with drive information
    """
    
    def __init__(
        self,
        token,
        drive
    ) :
        self.access_token = token
        self.name = drive["name"]
        self.id = drive["id"]
        self.site_content_endpoint = drive["site_content_endpoint"]
        self.path_readers = []
        self._get_path_readers(drive["paths"] if "paths" in drive else [])
        
    def _get_path_readers(self, paths):
        endpoint = f"{self.site_content_endpoint}/{self.id}"
        if not len(paths):
            root_reader = PathReader(self.access_token, {"drive_endpoint": endpoint})
            self.path_readers.append(root_reader)
        else: 
            for path in paths:
                path['drive_endpoint'] = endpoint 
                self.path_readers.append(PathReader(self.access_token, path))
      
    def get_file_items_generator(self):
        for path_reader in self.path_readers:
            for item in path_reader.get_file_items_generator():
                yield item
                
