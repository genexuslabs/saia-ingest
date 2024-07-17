import os
import time
from datetime import datetime, timezone
import logging
import requests
import json
from .log import AccumulatingLogHandler
from .config import DefaultHeaders

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

def file_upload(
        base_url: str,
        api_token: str,
        profile: str,
        file_path: str,
        file_name: str = None,
        metadata_file: dict = None,
        save_answer = False
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}/document"
        start_time = time.time()
        with open(file_path, "rb") as file:
            file_name = file.name.split("/")[-1] if file_name is None else file_name
            files = {"file": (file_name, file, "application/octet-stream")}
            data = None
            if metadata_file is not None:
                metadata_json_str = json.dumps(metadata_file)
                data = {u"metadata": metadata_json_str} 
            response = requests.post(
                url,
                files=files,
                data=data,
                headers={
                    'Authorization': f'Bearer {api_token}',
                    'filename': file_name
                }
            )
        response_body = response.json()
        # TODO: map the document to the ID and save it
        ret = response.ok
        if response.status_code != 200:
            message_response = f"{file_name},Error,{response.status_code}: {response.text}"
            ret = False
        else:
            if save_answer:
                with open(file_path + '.saia.metadata', 'w') as file:
                    file.write(json.dumps(response_body, indent=2))
            end_time = time.time()
            message_response = f"{response_body['indexStatus']}, {file_name},{response_body['name']},{response_body['id']},{end_time - start_time:.2f} seconds"
        logging.getLogger().info(message_response)
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
        ret = response.ok
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
        local_folder: str,
        reprocess_valid_status_list: list = [],
        reprocess_status_detail_list_contains: list = [],
        timestamp: datetime = None
    ) -> (list[str], list[str]):
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

            if status_detail == 'Invalid content':
                continue

            doc_timestamp = datetime.fromisoformat(doc_timestamp_str).replace(tzinfo=timezone.utc)
            if doc_timestamp < timestamp:
                continue

            if status in reprocess_valid_status_list:
                if reprocess_status_detail_list_contains is not None:
                    found = False
                    for item in reprocess_status_detail_list_contains:
                        if item in status_detail:
                            found = True
                    if not found:
                        continue

                to_delete.append(id)
                name_with_extension = f"{name}.{extension}"
                to_insert.append(os.path.join(local_folder, name_with_extension))

    except Exception as e:
        logging.getLogger().error(f"Error sync_failed_files: {e}")
        ret = False
    finally:
        logging.getLogger().info(f"To Delete: {len(to_delete)}: To Insert: {len(to_insert)}")
        return (to_delete, to_insert)

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

