import tempfile
import logging
import concurrent.futures
import os
import re
import time
import json

from typing import Any, Dict
from datetime import datetime

from saia_ingest.utils import get_yaml_config, get_configuration, load_json_file
from saia_ingest.rag_api import RagApi
from sharepoint.sharepoint_reader import SharePointReader
from sharepoint.sharepoint_item import SharepointFileItem


SUCCESS_STATUS = "Success"

class Sharepoint_Ingestor:
    
    def __init__(self, configuration_path: str, start_time: datetime):
        """
        Inits a Sharepoint_Ingestor from configuration file.

        Arguments: 
            configuration_path: A string with the path to the configuration file for the Ingestor.
        
        Raises:
            ValueError: If there is an error with the values provided.
        """
        logging.getLogger().info("Initiating ingestor.")
        config = get_yaml_config(configuration_path)
        self.saia_configuration = self._get_saia_configuration(config)
        self.sharepoint_configuration = self._get_sharepoint_configuration(config)
        self.rag_api = self._get_rag_api()
        self.reader = self._get_sharepoint_reader()
        download_directories, clean_status = self._get_download_configurations(config)
        self.download_directories = download_directories
        self.clean_status = clean_status
        self.reprocess_configuration = self._get_reprocess_configuration()
        self.start_time = start_time
    
    def _get_saia_configuration(self, configuration_object: Dict[str, Any]):
        return get_configuration(configuration_object, 'saia')
    
    def _get_sharepoint_configuration(self, configuration_object: Dict[str, Any]):
        return get_configuration(configuration_object, 'sharepoint')
    
    def _get_rag_api(self):
        return RagApi(
                    self.saia_configuration.get('base_url',""),
                    self.saia_configuration.get('api_token',""), 
                    self.saia_configuration.get('profile',""), 
                    self.saia_configuration.get('max_parallel_executions',5)
                    )
    
    def _get_sharepoint_reader(self):
        return SharePointReader(
                                self.sharepoint_configuration.get('connection',{}),
                                )
     
    def _get_reprocess_configuration(self):
        default_configuration = {"retry_count": 1,
                                 "reprocess_failed_files": False,
                                 "failed_status": ["Failed"]}
        return self.sharepoint_configuration.get("reprocess",default_configuration)
    
    def _get_download_configurations(self, configuration):
        general_configuration = configuration.get("general", {})
        download_directories = self._get_download_directories(general_configuration)
        clean_status = ( [SUCCESS_STATUS]
                        if "download" not in general_configuration or "clean_status" not in general_configuration["download"]
                        else general_configuration["download"].get("clean_status"))
        return download_directories, clean_status
    
    def _get_reprocced_files_configuration(self, configuration):
        general_configuration = configuration.get("general", {})
        return general_configuration.get("reprocess_failed_files", False)
    
    def _get_download_directories(self, configuration):
        if "download" not in configuration:
            temp_folder = tempfile.TemporaryDirectory().name
            return {"files": temp_folder, "metadata": temp_folder}
        
        download_directory = configuration["download"]
        
        files_path = (download_directory
                      if isinstance(download_directory, str)
                      else get_configuration(download_directory, "files")
                     )
        metadata_path = (download_directory
                         if isinstance(download_directory, str)
                         else get_configuration(download_directory, "metadata")
                        )
        return {"files": files_path, "metadata": metadata_path}

    def _get_metadata_from_item(self, item):
        metadata_processing_policy = self.sharepoint_configuration.get('metadata_processing_policy',{})
        return item.retrieve_metadata(metadata_processing_policy)
    
    def retrieve_information(self):
        max_parallel_executions = self.sharepoint_configuration.get("max_parallel_executions", 1)
        sites = self.sharepoint_configuration.get('sites', {})
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
            futures = [executor.submit(self.reader.retrieve_site_information, site) for site in sites]
            concurrent.futures.wait(futures)

    def _save_metadata(self, metadata):
        name = metadata["sharepoint"]["name"] + ".metadata"
        metadata_path = os.path.join(self.download_directories["metadata"], name )
        with open(metadata_path, "w") as f:
            f.write(json.dumps(metadata, indent=2))

    def _process_next_item(self, i):
        item = self.reader.get_next_item()
        while item:
            item.increment_upload_attempts()
            
            logging.getLogger().info(f"Thread {str(i)} start {item.upload_attempts} attempt for {item.name}")
            
            start_time = time.time()
            
            item_content_path = item.download(self.download_directories["files"])
            
            sharepoint_metadata = self._get_metadata_from_item(item)
            
            saia_metadata = self.rag_api.upload_document_with_metadata_file(item_content_path, json.dumps(sharepoint_metadata, indent=2))
            
            item.set_status(saia_metadata["indexStatus"])
            
            metadata = {"sharepoint": sharepoint_metadata, "saia": saia_metadata}
            
            self._save_metadata(metadata)
            
            if "error" not in saia_metadata and item.status in self.clean_status:
                 os.remove(item_content_path)
                
            end_time = time.time()
            
            logging.getLogger().info(f"Thread {str(i)} finished attending  {item.name} in {end_time - start_time:.2f}s with status: {item.status}")
            
            if item.status == SUCCESS_STATUS or item.upload_attempts <= self.reprocess_configuration["retry_count"]:
                item = self.reader.get_next_item()
            
    def process_file_items(self):
        profile_name = self.saia_configuration.get('profile',"")
        
        logging.getLogger().info(f"Uploading files to { profile_name }")
        
        start_time = time.time()
        
        max_parallel_executions = self.saia_configuration.get("max_parallel_executions", 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
            futures = [executor.submit(self._process_next_item, i) for i in range(max_parallel_executions)]
            concurrent.futures.wait(futures)
    
        end_time = time.time()
        
        logging.getLogger().info(f"Upload to { profile_name } finished at {end_time - start_time:.2f}s")

    def init_item_generator(self):
        reader = self.reader
        reprocess_configuration = self.reprocess_configuration
        metadata_path = self.download_directories["metadata"]
        if (self.reprocess_configuration["reprocess_failed_files"]):
            reader.init_file_system_item_generator(metadata_path, reprocess_configuration["failed_status"])
        else:
            reader.init_sharepoint_item_generator()
            
    def execute(self):
        ret = True
        try:
            logging.getLogger().info(f"Ingestor start.")

            self.retrieve_information()          
            self.init_item_generator()
            self.process_file_items()

            end_time = time.time()

            logging.getLogger().info(f"Ingestor ends at {end_time - self.start_time:.2f}s")

            if self.saia_configuration.get("upload_operation_log", False):
                end_time = time.time()
                message_response = f"bulk ingest ({end_time - self.start_time:.2f}s)"
                ret = self.rag_api.operation_log_upload("ALL", message_response, 0)
        except Exception as e:
            logging.getLogger().error(f"Error: {e}")
            ret = False
        finally:
            end_time = time.time()
            logging.getLogger().info(f"Execution Time: {end_time - self.start_time:.2f}s")
            return ret 