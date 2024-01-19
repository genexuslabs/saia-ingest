import time
import logging
import requests
import json
from .log import AccumulatingLogHandler

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
                'Content-Type': 'application/json'
            })
        response_body = response.json()
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
            logging.getLogger().info(f"{response.status_code}: {response.text}")
            ret = False
        else:
            end_time = time.time()
            logging.getLogger().info(f"uploaded {file_name} as {response_body['name']} id:{response_body['id']} {end_time - start_time:.2f}s")
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
            logging.getLogger().Warning("AccumulatingLogHandler not found in the root logger's handlers.")
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
                'Content-Type': 'application/json',
            }
        )
        response_body = response.json()
        ret = response.ok
        if response.status_code != 200:
            logging.getLogger().info(f"{response.status_code}: {response.text}")
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
        file_name: str,
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}/document/{file_name}"
        response = requests.delete(
            url, 
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': 'application/json'
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