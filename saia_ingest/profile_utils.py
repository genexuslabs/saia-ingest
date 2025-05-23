import os
from pathlib import Path
import time
from datetime import datetime, timezone
import logging
from typing import List
import requests
import urllib3
import json

from saia_ingest.file_utils import calculate_file_hash
from .log import AccumulatingLogHandler
from .config import DefaultHeaders, Defaults

# Suppress the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def is_valid_profile(
        base_url: str,
        api_token: str,
        profile: str,
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}"
        response = requests.get(
            url, 
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': DefaultHeaders.JSON_CONTENT_TYPE
            })
        ret = response.ok
        if response.status_code != 200:
            logging.getLogger().info(f"{response.status_code}: {response.text}")
            ret = False
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
        ret = False
    finally:
        return ret

def get_name_from_metadata_file(
        metadata_file: str,
        label_key: str,
        default_value: str,
    ) -> str:
    label_value = default_value
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            try:
                data = json.load(f)
                label_value = data.get(label_key, default_value)
            except json.JSONDecodeError:
                logging.getLogger().error(f"Error opening: {metadata_file}")
    return label_value

def file_upload(
        base_url: str,
        api_token: str,
        profile: str,
        file_path: str,
        file_name: str = None,
        metadata_file: dict = None,
        save_answer = False,
        optional_args: dict = None,
    ) -> bool:
    ret = True
    response = None
    response_body = ""
    retry_delay = 150
    connection_error_label = "Could not connect to RAG API endpoint"
    try:
        url = f"{base_url}/v1/search/profile/{profile}/document"
        start_time = time.time()
        with open(file_path, "rb") as file:
            file_name = file.name.split("/")[-1] if file_name is None else file_name
            files = {"file": (file_name, file, "application/octet-stream")}
            data = optional_args
            if metadata_file is not None:
                metadata_json_str = json.dumps(metadata_file)
                data = {u"metadata": metadata_json_str, **optional_args} 
            response = requests.post(
                url,
                files=files,
                data=data,
                headers={
                    'Authorization': f'Bearer {api_token}',
                    'filename': file_name.encode('utf-8')
                }
            )
        response_body = response.json()

        ret = response.ok
        if response.status_code != 200:
            message_response = f"{file_name},Error,{response.status_code}: {response.text}"
            if connection_error_label in response.text:
                time.sleep(retry_delay)
            ret = False
        else:
            if save_answer:
                file_crc = calculate_file_hash(file_path)
                response_body[Defaults.FILE_HASH] = file_crc
                file_metadata_path = file_path + Defaults.PACKAGE_METADATA_POSTFIX
                with open(file_metadata_path, 'w', encoding='utf-8') as file:
                    file.write(json.dumps(response_body, indent=2))
            end_time = time.time()
            metadata_elements = response_body.get('metadata', [])
            metadata_count_items = f",{len(metadata_elements)}" if len(metadata_elements) > 0 else ""
            message_response = f"{response_body['indexStatus']}, {file_name},{response_body['name']},{response_body['id']}{metadata_count_items},{end_time - start_time:.2f} seconds"
        logging.getLogger().info(message_response)
    except Exception as e:
        if e.response['Error']['Code'] == '401':
            logging.getLogger().error(f"Not authorized to {url}")
        if e.response['Error']['Code'] == '404':
            logging.getLogger().error(f"The url {url} does not exist.")
        elif e.response['Error']['Code'] == '403':
            logging.getLogger().error(f"Forbidden access to '{url}'")
        elif connection_error_label in response.text:
            time.sleep(retry_delay)
        else:
            raise e
        ret = False
    finally:
        return ret

def operation_log_upload(
        base_url: str,
        api_token: str,
        profile: str,
        step: str,
        name: str,
        level: int = 0,
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}/log"

        accumulating_handler = None
        for handler in logging.getLogger().handlers:
            if isinstance(handler, AccumulatingLogHandler):
                accumulating_handler = handler
                break
        if not accumulating_handler:
            logging.getLogger().warning("AccumulatingLogHandler not found in the root logger's handlers.")
            return

        data = {
            "step": step,
            "level": level,
            "name": name,
            "data": accumulating_handler.get_accumulated_logs().__str__()
        }
        
        response = requests.post(
            url,
            json=data,
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': DefaultHeaders.JSON_CONTENT_TYPE,
            }
        )
        response_body = response.json()
        if response.status_code != 200:
            logging.getLogger().info(f"{response.status_code}: {response.text} {response_body}")
            ret = False
    except Exception as e:
        logging.getLogger().error(f"Could not update operation log {e}")
        ret = False
    finally:
        return ret

def file_delete(
        base_url: str,
        api_token: str,
        profile: str,
        file_id: str,
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}/document/{file_id}"
        response = requests.delete(
            url, 
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': DefaultHeaders.JSON_CONTENT_TYPE
            })
        ret = response.ok
        if response.status_code != 200:
            logging.getLogger().error(f"{response.status_code}: {response.text}")
            ret = False
    except Exception as e:
        if e.response['Error']['Code'] == '401':
            logging.getLogger().error(f"Not authorized to {url}")
        if e.response['Error']['Code'] == '404':
            logging.getLogger().error(f"The url {url} does not exist.")
        elif e.response['Error']['Code'] == '403':
            logging.getLogger().error(f"Forbidden access to '{url}'")
        else:
            raise e
        ret = False
    finally:
        return ret

def sync_failed_files(
        docs: list,
        file_list: List[Path],
        local_folder: str,
        reprocess_valid_status_list: list = [],
        reprocess_status_detail_list_contains: list = [],
        reprocess_failed_files_exclude: list = [],
        timestamp: datetime = None
    ) -> (list[str], list[str]): # type: ignore
    ret = True
    to_delete = []
    to_insert = []
    try:
        for f in docs:
            id = f.get('id', None)
            name = f.get('name', None)
            extension = f.get('extension', None)
            status = f.get('indexStatus', None)
            status_detail = f.get('indexDetail', '')
            doc_timestamp_str = f.get('timestamp', None)

            if name in reprocess_failed_files_exclude:
                continue

            if status_detail == 'Invalid content':
                continue

            doc_timestamp = datetime.fromisoformat(doc_timestamp_str).replace(tzinfo=timezone.utc)
            if doc_timestamp < timestamp:
                continue

            if status in reprocess_valid_status_list:
                if len(reprocess_status_detail_list_contains) > 0:
                    found = False
                    for item in reprocess_status_detail_list_contains:
                        if item in status_detail:
                            found = True
                            break
                    if not found:
                        continue

                to_delete.append(id)
                name_with_extension = f"{name}.{extension}"
                if file_list is None:
                    base_local_folder = local_folder
                else:
                    base_local_folder = next((p for p in file_list if normalize(p.name) == normalize(name_with_extension)), None)
                    if base_local_folder is None:
                        base_local_folder = local_folder
                        logging.getLogger().warning(f"Could not find {name_with_extension}")
                to_insert.append(os.path.join(base_local_folder, name_with_extension))

    except Exception as e:
        logging.getLogger().error(f"Error sync_failed_files: {e}")
        ret = False
    finally:
        logging.getLogger().info(f"To Delete: {len(to_delete)}: To Insert: {len(to_insert)}")
        return (to_delete, to_insert)

# Normalize by removing spaces
def normalize(name: str) -> str:
    return name.replace(" ", "")

def get_documents(
        base_url: str,
        api_token: str,
        profile: str,
        skip: int = 0,
        count: int = 999999,
    ) -> list[str]:
    ret = True
    new_list = []
    try:
        url = f"{base_url}/v1/search/profile/{profile}/documents?skip={skip}&count={count}"
        response = requests.get(
            url, 
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': DefaultHeaders.JSON_CONTENT_TYPE
            })
        ret = response.ok
        if response.status_code != 200:
            logging.getLogger().error(f"{response.status_code}: {response.text}")
            ret = False
        # clean wrong files, get a list to reprocess...
        document_result = response.json()
        new_list = list(document_result['documents'])
    except Exception as e:
        logging.getLogger().error(f"Error getting documents {e}")
        ret = False
    finally:
        return new_list


def get_bearer_token(
        loader: any
    ) -> str:
    base_url = loader.bearer_url
    client_id = loader.bearer_client_id
    client_secret = loader.bearer_client_secret
    scope = loader.bearer_scope
    grant_type = loader.bearer_grant_type

    access_token = ""
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope,
        'grant_type': grant_type
    }
    response = requests.post(base_url, data=data)
    if response.status_code == 200:
        token_response = response.json()
        access_token = token_response['access_token']
    else:
        error_msg = f"Error {response.status_code} getting token: {response.text}"
        raise Exception(error_msg)

    return access_token


def get_json_response_from_url(
        base_url: str,
        loader: any,
        h_subscription_key: str,
        h_subscription_value: str
    ) -> tuple[list[str], str]:
    new_list = []
    next_url_href = None
    try:
        url = base_url
        headers = {
            'Authorization': f'Bearer {loader.bearer_token}',
            h_subscription_key: h_subscription_value,
            'User-Agent': DefaultHeaders.AGENT,
            'Content-Type': DefaultHeaders.JSON_CONTENT_TYPE
        }
        response = requests.get(
            url,
            headers=headers,
            verify=False # Disable SSL verification
        )
        if response.status_code == 401:
            logging.getLogger().error(f"{response.status_code}: {response.text} - Getting new token and retrying...")
            loader.bearer_token = get_bearer_token(loader)
            headers['Authorization'] = f'Bearer {loader.bearer_token}'
            response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logging.getLogger().error(f"{response.status_code}: {response.text}")
        document_result = response.json()
        new_list = list(document_result['elements'])
        links = list(document_result['links'])
        for link in links:
            if link.get("rel", None) == "next":
                next_url_href = link.get("href", None)
                break

    except Exception as e:
        logging.getLogger().info(f"URL: {url}")
        logging.getLogger().error(f"Error getting elements {e}")
    finally:
        return (new_list, next_url_href)


def search_failed_to_delete(files: list[str]) -> list[str]:
    """Check if local metadata exists and return a list of Document ids to delete"""
    file_list = []
    for file in files:
        item_file_metadata = f"{file}{Defaults.PACKAGE_METADATA_POSTFIX}"
        if os.path.exists(item_file_metadata):
            with open(item_file_metadata, 'r') as f:
                try:
                    data = json.load(f)
                    id = data.get('id', None)
                    if id is not None:
                        file_list.append(id)
                except json.JSONDecodeError:
                    logging.getLogger().error(f"Error decoding JSON in file: {item_file_metadata}")
    return file_list
