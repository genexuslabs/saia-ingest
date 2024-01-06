import time
import logging
import requests

def is_valid_profile(
        base_url: str,
        api_token: str,
        profile: str,
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}"
        logging.getLogger().info(f"using {url}")
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
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}/document"
        logging.getLogger().info(f"using {url}")
        start_time = time.time()
        with open(file_path, "rb") as file:
            file_name = file.name.split("/")[-1]
            files = {"file": (file_name, file, "application/octet-stream")}
            response = requests.post(
                url,
                files=files,
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
            logging.getLogger().info(f"uploaded {file_name}\n{response_body}\n")
        end_time = time.time()
        logging.getLogger().info(f"elapsed time: {end_time - start_time}s")
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

def file_delete(
        base_url: str,
        api_token: str,
        profile: str,
        file_name: str,
    ) -> bool:
    ret = True
    try:
        url = f"{base_url}/v1/search/profile/{profile}/document/{file_name}"
        logging.getLogger().info(f"using {url}")
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