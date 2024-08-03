import re
import os
import json
import requests
import logging

from saia_ingest.utils import do_get
from sharepoint.drive_reader import DriveReader

from datetime import datetime

logger = logging.getLogger(__name__)

class SiteReader:
    """SharePoint reader.

    Reads folders from the SharePoint site.

    Args:
        token (str): Access token.
        site_name (str): Site name
    """
    
    def __init__(
        self,
        token: str,
        site: str,
    ) :
        self.access_token = token
        self.name = site["name"]
        self.metadata_policy = None
        self.id = ''
        self._retrive_site_id()
        self.drives = {}
        self._retrive_site_drives_ids(site["drives"])
    
    def get_name(self):
        return self.name
    
    def _retrive_site_id(self):
        """
        Retrieves the site ID of a SharePoint site using the provided site name.

        Raises:
            Exception: If the specified SharePoint site is not found.
        """
        site_information_endpoint = (
            f"https://graph.microsoft.com/v1.0/sites?search={self.name}"
        )
        
        response_json = do_get(self.access_token, site_information_endpoint).json()
        
        if (len(response_json["value"]) <= 0 and "id" not in response_json["value"][0] ):
            raise ValueError(
                    f"The specified sharepoint site {self.name} is not found."
                )
            
        self.id = response_json["value"][0]["id"]
        
    def _get_drive_id(self, drive_name, drive_list):
        for drive in drive_list:
            if drive_name == drive["name"]:
                return drive["id"]
        raise ValueError(
                f"Drive {drive_name} information not found for site {self.name}."
            )
    
    def _retrive_site_drives_ids(self, requested_drives):
        """
        Retrieves the IDs of the drives belonging to a Site

        Raises:
            Exception:
                -No drive were found.
                -Site id is not initialized
        """
        
        if not self.id:
            raise ValueError(
                f"Missing id for site {self.name} when retriving drives information"
            )
        
        site_content_endpoint = (
            f"https://graph.microsoft.com/v1.0/sites/{self.id}/drives"
        )
        

        response_json = do_get(self.access_token, site_content_endpoint).json()
        
        if len(response_json["value"]) <= 0:
            raise ValueError(
                f"No drives found for site {self.name}."
            )
            
        for drive in requested_drives :
            drive_id = self._get_drive_id(drive["name"], response_json["value"])
            drive["id"] = drive_id
            drive["site_content_endpoint"] = site_content_endpoint
            self.drives[drive["name"]] = DriveReader(self.access_token,drive)
            
    def get_site_file_items_generator(self):
        for drive in self.drives:
            for item in self.drives[drive].get_file_items_generator():
                yield item