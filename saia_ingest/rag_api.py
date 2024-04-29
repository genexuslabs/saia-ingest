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
    def __init__(self, base_url, api_token, profile = '', max_parallel_executions = 5):
        self.base_url = base_url
        self.api_token = api_token
        self.max_parallel_executions = max_parallel_executions
        self.base_header = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json"
        }
        
        self.profile = profile if profile and self.is_valid_profile(profile) else ''
        
    def do_request(self, method, url='', headers=None, params=None, data=None, files=None, json=None):
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
        url = f"{self.base_url}/v1/search/profiles"
        response = self.do_request(GET_METHOD, url, headers=self.base_header)
        return response.json()

    def get_profile(self, name):
        url = f"{self.base_url}/v1/search/profile/{name}"
        response = self.do_request(GET_METHOD, url, headers=self.base_header)
        return response.json()

    def is_valid_profile(self, name):
        response = self.get_profile(name)
        ret = not 'errors' in response
        logging.getLogger().info(f'{name} is a valid profile.' if ret else f'Profile {name} not found.')
        return ret

    def get_profile_documents(self, name, skip=None, count=1000):
        url = f"{self.base_url}/v1/search/profile/{name}/documents"
        params = {}
        if skip:
            params["skip"] = skip
        if count:
            params["count"] = count
        response = self.do_request(GET_METHOD, url, headers=self.base_header, params=params)
        return response.json()

    def get_document(self, name, document_id):
        url = f"{self.base_url}/v1/search/profile/{name}/document/{document_id}"
        response = self.do_request(GET_METHOD, url, headers=self.base_header)
        return response.json()
    
    def post_profile(self, fpath):
        print(fpath)
        ret = []
        if os.path.isfile(fpath):
            with open(fpath, 'r') as file:
                data = json.load(file)
                url = f"{self.base_url}/v1/search/profile"
                response = self.do_request(POST_METHOD, url, json=data, headers=self.base_header)
                ret.append(response.json())
        elif os.path.isdir(fpath):
            for filename in os.listdir(fpath):
                if filename.endswith('.json') or os.path.isdir(os.path.join(fpath, filename)):
                    ret = ret + self.post_profile(os.path.join(fpath, filename))          
        else:
            raise ValueError("Invalid file path")
        return ret
    
    def update_profile(self, name, update_data):
        url = f"{self.base_url}/v1/search/profile/{name}"
        response = self.do_request(PUT_METHOD, url, json=update_data, headers=self.base_header)
        return response.json()
    
    def delete_profile(self, name):
        url = f"{self.base_url}/v1/search/profile/{name}"
        response = self.do_request(DELETE_METHOD, url, headers=self.base_header)
        return response.json()
    
    def delete_profile_document(self, name, id):
        url = f"{self.base_url}/v1/search/profile/{name}/document/{id}"
        response = self.do_request(DELETE_METHOD, url, headers=self.base_header)
        return response.json()

    def upload_document_binary(self, name, file_path, content_type):
        url = f"{self.base_url}/v1/search/profile/{name}/document"
        headers = {
            "filename": os.path.basename(file_path),
            "Content-Type": content_type
        }
        headers.update(self.base_header)
        with open(file_path, 'rb') as file:
            response = self.do_request(POST_METHOD, url, headers=headers, data=file)
        return response.json()    
    
    def is_valid_json(self, my_json):
        try:
            json_object = json.loads(my_json)
        except ValueError as e:
            return None
        return json_object
    
    def upload_document_with_metadata_file(self, file_path, metadata = None, profile = ''):
        profile_name = profile or self.profile
        
        url = f"{self.base_url}/v1/search/profile/{profile_name}/document"
        start_time = time.time()
        files = {
            'file': open(file_path, 'rb')
        }
        data = None
        if metadata and self.is_valid_json(metadata):
            data = {'metadata': metadata}
        else:
            if metadata and os.path.exists(metadata):
                files.update({'metadata': open(metadata, 'r')})
        response = self.do_request(POST_METHOD, url, headers=self.base_header, data=data, files=files)
        
        response_body = response.json()
        
        end_time = time.time()
        message_response = f"{os.path.basename(file_path)},{response_body['indexStatus']},{response_body['name']},{response_body['id']},{end_time - start_time:.2f}"
        
        logging.getLogger().info(message_response)
        
        return response_body

    def delete_all_documents(self):
        if self.profile:
            docs = self.get_profile_documents(self.profile)
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallel_executions) as executor:
                    futures = [executor.submit(self.delete_profile_document, self.profile, d['id']) for d in docs['documents']]
            concurrent.futures.wait(futures)
