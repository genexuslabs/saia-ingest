import requests
import os
import time
import logging
from dotenv import load_dotenv, find_dotenv
import json
import concurrent.futures

GET_METHOD = "GET"
POST_METHOD = "POST"
PUT_METHOD = "PUT"
DELETE_METHOD = "DELETE"

class RagApi:
    """Rag Api.

    This class provide a way to programmatically communicate with the RAG's functions that are provided
    by the following api (https://wiki.genexus.com/enterprise-ai/wiki?29)

    Args:
        base_url (str):     The base URL for your GeneXus Enterprise AI installation
        api_token (str):    Token provided by the environment that allow the communication with the API.
                            Please check: https://wiki.genexus.com/enterprise-ai/wiki?20
        profile (str):      A specific RAG assistant name.
        
        max_parallel_executions (Optional[str]): The maximum parallel execution allowed. Default 5
    """
    def __init__(self, base_url, api_token, profile, max_parallel_executions = 5):
        """
        Inits a Rag_api instance.

        Raises:
            ValueError: If there is an error with the values provided.
        """

        if not base_url:
            raise ValueError('Invalid value: base_url')
        
        if not api_token:
            raise ValueError('Invalid value: api_token')

        self.api_token = api_token
        self.base_url = base_url
        self.profile = profile
        self.max_parallel_executions = max_parallel_executions
        self.base_header = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json"
        }

        if profile and not self.is_valid_profile(profile):
            raise ValueError('Invalid value: profile')


    def set_profile(self, profile_name):
        """
        Set the profile that the Rag_Api will use in some functions as default.

        Args:
            profile_name (str): The name of the RAG assistant to be set.

        Returns:
            bool: The change was done correctly.

        Raises:
            ValueError: If the RAG assistant provided is not valid.
        """
        if profile_name and not self.is_valid_profile(profile_name):
            raise ValueError('Invalid value: profile')
        self.profile = profile_name
        return True
    
    def _do_request(self, method, url='', headers=None, params=None, data=None, files=None, json=None):
        response = None
        try:
            response = requests.request(method, url, headers=headers, params=params, data=data, files=files, json=json)
        except Exception as e:
            if e.response['Error']['Code'] == '401':
                logging.getLogger().info(f"Not authorized to {url}")
            if e.response['Error']['Code'] == '404':
                logging.getLogger().info(f"The url {url} does not exist.")
            elif e.response['Error']['Code'] == '403':
                logging.getLogger().info(f"Forbidden access to '{url}'")
            else:
                raise e
            logging.getLogger().info(f"Error: {e}")
        finally:
            return response
        
    def search_profiles(self):
        '''
        Retrieve all the RAG Assistants for a Project.
        '''
        
        url = f"{self.base_url}/v1/search/profiles"
        response = self._do_request(GET_METHOD, url, headers=self.base_header)
        return response.json()

    def get_profile(self, profile_name):
        '''
        Get RAG Assistant details with the name provided.
        
        Args:
            profile_name (str): Name of the RAG assistant from which we want information
        
        '''
        url = f"{self.base_url}/v1/search/profile/{profile_name}"
        response = self._do_request(GET_METHOD, url, headers=self.base_header)
        return response.json()

    def is_valid_profile(self, profile_name):
        '''
        Returns True if the RAG assistant is valid.
        
        Args:
            profile_name (str): Name of the RAG assistant from which we want information
        
        '''
        response = self.get_profile(profile_name)
        ret = not 'errors' in response
        logging.getLogger().info(f'{profile_name} is a valid profile.' if ret else f'Profile {profile_name} not found.')
        return ret

    def get_profile_documents(self, profile_name='', skip=None, count=10):
        '''
        List the documents for a RAG Assistant. If profile_name is not provided,
        it will use the one set in the Rag_Api as default.
        
        Args:
            profile_name (Optional[str]): Name of the RAG assistant from which we want information. Default self.profile
            skip         (Optional[str]): Number of documents to skip
            count        (Optional[str]): Number of documents to return
            
        '''
        name = profile_name or self.profile
        
        url = f"{self.base_url}/v1/search/profile/{name}/documents"
        params = {}
        if skip:
            params["skip"] = skip
        if count:
            params["count"] = count
        response = self._do_request(GET_METHOD, url, headers=self.base_header, params=params)
        return response.json()

    def get_document(self, document_id, profile_name=''):
        '''
        Gets details about the document with the id provided.
        
        Args:
            document_id  (str):           Id of the document from which we want information
            profile_name (Optional[str]): Name of the RAG assistant to get information. Default self.profile
        
        '''
        name = profile_name or self.profile
        url = f"{self.base_url}/v1/search/profile/{name}/document/{document_id}"
        response = self._do_request(GET_METHOD, url, headers=self.base_header)
        return response.json()
    
    def post_profiles(self, fpath, recursive = False):
        '''
        Given a path, it will create RAG assistants accordingly in the environment.
        If fpath is a json file path, it will create the RAG assistant with that information.
        If fpath is a folder path, it will create a RAG assistant for each json file in the folder.
        
        Optionally, with the recursive flag, you can make the function go through all the sub-folders
        creating RAG assistants for every json file that it found.
        
        Args:
            fpath     (str):            Path where the folder or config file is located.
            recursive (Optional[bool]): If True, the function will go through every sub-folder.
            
        Returns:
            A list with the created RAG assistants.
        
        '''
        
        ret = []
        if os.path.isfile(fpath):
            try:
                with open(fpath, 'r') as file:
                    data = json.load(file)
                    url = f"{self.base_url}/v1/search/profile"
                    response = self._do_request(POST_METHOD, url, json=data, headers=self.base_header)
                    ret.append(response.json())
            except:
                logging.getLogger().info(f"Invalid config file at {fpath}.")
        else:
            for filename in os.listdir(fpath):
                if filename.endswith('.json') or (recursive and os.path.isdir(os.path.join(fpath, filename))):
                    ret = ret + self.post_profiles(os.path.join(fpath, filename))
        return ret
    
    def update_profile(self, update_data, profile_name = ''):
        '''
        Update a RAG Assistant.
        
        Args:
            update_data (str):             Json string that contains the new information.
            profile_name (Optional[str]):  Name of the RAG assistant we want to update. Default self.profile
        '''
        name = profile_name or self.profile
        url = f"{self.base_url}/v1/search/profile/{name}"
        response = self._do_request(PUT_METHOD, url, json=update_data, headers=self.base_header)
        return response.json()
    
    def delete_profile(self, profile_name = ''):
        '''
        Delete a RAG Assistant.
        
        Args:
            profile_name (Optional[str]):  Name of the RAG assistant we want to update. Default self.profile
        '''
        name = profile_name or self.profile
        url = f"{self.base_url}/v1/search/profile/{name}"
        response = self._do_request(DELETE_METHOD, url, headers=self.base_header)
        return response.json()
    
    def delete_profile_document(self, id, profile_name = ''):
        '''
        Delete a document associated to a RAG Assistant.
        
        Args:
            profile_name (Optional[str]):  Name of the RAG assistant we want to update. Default self.profile
        '''
        name = profile_name or self.profile
        url = f"{self.base_url}/v1/search/profile/{name}/document/{id}"
        response = self._do_request(DELETE_METHOD, url, headers=self.base_header)
        return response.json()

    def upload_document_binary(self, file_path, content_type = 'application/pdf', profile_name = ''):
        '''
        Upload a document as binary.
        
        Args:
            file_path    (str):            Path of the document in the local file system.
            content_type (Optional[str]):  Content type to be send in the headers of the request. Default application/pdf.
            profile_name (Optional[str]):  Name of the RAG assistant we want to update. Default self.profile
        '''
        name = profile_name or self.profile
        url = f"{self.base_url}/v1/search/profile/{name}/document"
        headers = {
            "filename": os.path.basename(file_path),
            "Content-Type": content_type
        }
        headers.update(self.base_header)
        with open(file_path, 'rb') as file:
            response = self._do_request(POST_METHOD, url, headers=headers, data=file)
        return response.json()    
    
    def _is_valid_json(self, my_json):
        try:
            json_object = json.loads(my_json)
        except ValueError as e:
            return None
        return json_object
    
    def upload_document_with_metadata_file(self, file_path, metadata = None, profile_name = ''):
        '''
        Upload a document as multipart.
        
        Args:
            file_path    (str):            Path of the document in the local file system.
            metadata     (Optional[str]):  Metadata of the file to be upload. If it is a path, the metadata will be
                                           upload from the json file provided.
                                           If metadata is not a path, it is supposed to be a json format string.
            profile_name (Optional[str]):  Name of the RAG assistant we want to update. Default self.profile
        '''
        profile = profile_name or self.profile
        
        url = f"{self.base_url}/v1/search/profile/{profile}/document"
        start_time = time.time()
        files = {
            'file': open(file_path, 'rb')
        }
        data = None
        if metadata and self._is_valid_json(metadata):
            data = {'metadata': metadata}
        else:
            if metadata and os.path.exists(metadata):
                files.update({'metadata': open(metadata, 'r')})
        response = self._do_request(POST_METHOD, url, headers=self.base_header, data=data, files=files)
        
        response_body = response.json()
        
        end_time = time.time()
        if response.ok:
            message_response = f"{os.path.basename(file_path)},{response_body['indexStatus']},{response_body['name']},{response_body['id']},{end_time - start_time:.2f}"
        else:
            message_response = f"{os.path.basename(file_path)},Error,{response_body.get('errors')[0].get('id')},{response_body.get('errors')[0].get('description')}"

        logging.getLogger().info(message_response)
        
        return response_body

    def delete_all_documents(self):
        '''
        Delete all documents related to the self.profile Assistant.
        '''
        if self.profile:
            docs = self.get_profile_documents(self.profile)
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallel_executions) as executor:
                    futures = [executor.submit(self.delete_profile_document, d['id'], self.profile) for d in docs['documents']]
            concurrent.futures.wait(futures)
        
    
    def ask_rag_agent(self, prompt, profile_name):
        '''
        Given a prompt, make a question to an agent.
        
        Args:
            prompt    (str):               Promt to be provided to the agent.
            profile_name (Optional[str]):  Name of the RAG assistant we want to update. Default self.profile
        '''
        url = f"{self.base_url}/v1/search/execute"
        name = profile_name or self.profile
        resp = self._do_request(POST_METHOD, url, headers=self.base_header, json={"profile": name, "question": prompt})
        return resp.json()
