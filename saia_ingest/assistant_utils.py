import os
import time
import logging
import requests
import json
from .log import AccumulatingLogHandler
from .config import DefaultHeaders


def get_assistants(
        base_url: str,
        api_token: str,
        detail: str = None,
    ) -> list[str]:
    new_list = []
    try:
        detail_filter = "" if detail is None else f"?detail={detail}"
        url = f"{base_url}/v1/organization/assistants{detail_filter}"
        
        response = requests.get(
            url, 
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': DefaultHeaders.JSON_CONTENT_TYPE
            })
        ret = response.ok
        if response.status_code != 200:
            logging.getLogger().error(f"{response.status_code}: {response.text}")
        result = response.json()
        new_list = list(result.get("assistants"))
    except Exception as e:
        logging.getLogger().error(f"Error getting assistants {e}")
    finally:
        return new_list


def get_assistant(
        base_url: str,
        api_token: str,
        assistant: str,
    ) -> list[str]:
    ret = {}
    try:
        url = f"{base_url}/v1/assistant/{assistant}"
        response = requests.get(
            url, 
            headers={
                'Authorization': f'Bearer {api_token}',
                'Content-Type': DefaultHeaders.JSON_CONTENT_TYPE
            })
        ret = response.json()
        if response.status_code != 200:
            logging.getLogger().error(f"{response.status_code}: {response.text}")
    except Exception as e:
        if e.response['Error']['Code'] == '401':
            logging.getLogger().error(f"Not authorized to {url}")
        if e.response['Error']['Code'] == '404':
            logging.getLogger().error(f"The url {url} does not exist.")
        elif e.response['Error']['Code'] == '403':
            logging.getLogger().error(f"Forbidden access to '{url}'")
        else:
            raise e
    finally:
        return ret
