import os
import json
from pathlib import Path
from typing import List
import magic
import logging
import yaml
import requests
import chardet
from .config import Defaults

def get_yaml_config(yaml_file):
    # Load the configuration from the YAML file
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def detect_file_extension(file_path):
    mime = magic.Magic()

    # Open the file in binary mode
    with open(file_path, 'rb') as file:
        file_signature = file.read(1024)
    # Detect the file type based on the magic number
    file_type = mime.from_buffer(file_signature)
    file_type = file_type.lower()

    if 'text' in file_type:
        return '.txt'
    elif 'microsoft word' in file_type:
        return '.docx'
    elif 'image' in file_type:
        return '.png'  # Add more image types as needed
    elif 'pdf' in file_type:
        return '.pdf'
    else:
        logging.getLogger().info(f"{file_path} unknown file type: {file_type}")
        return ""

def change_file_extension(file_path, new_extension):
    base_path, _ = os.path.splitext(file_path)
    new_file_path = base_path + new_extension
    return new_file_path

def get_subfolder_metadata(file_path, base_folder_names, father_name, sibiling_name):
    metadata = None
    path_parts = file_path.split(os.sep)
    
    # Find the index of the base_folder_name folder
    for base_folder_name in base_folder_names:
        if base_folder_name in path_parts:
            base_index = path_parts.index(base_folder_name)        
            metadata_folder1 = path_parts[base_index]
            next_folder = path_parts[base_index + 1] if base_index + 1 < len(path_parts) else None
            
            metadata = {
                father_name: metadata_folder1,
                sibiling_name: next_folder
            }
            return metadata

    return metadata

def get_metadata_file(file_path, file_name, metadata_extension='.json', metadata_mappings:dict=None) -> dict:
    # Get the metadata file content
    ret = None
    if len(metadata_mappings) == 2:
        folders, keys = list(metadata_mappings.values())[:2]
        if len(folders) > 0 and len(keys) >= 2:
            ret = get_subfolder_metadata(file_path, folders, keys[0], keys[1])
            if ret:
                return ret

    new_file_name = change_file_extension(file_name, metadata_extension)
    metadata_file = os.path.join(file_path, new_file_name)
    if os.path.isfile(metadata_file):
        ret = load_json_file(metadata_file)
    return ret

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def load_json_file(file_path) -> dict:
    ret = None
    encoding = detect_encoding(file_path)
    try:
        with open(file_path, 'r', encoding=encoding) as json_file:
            ret = json.load(json_file)
    except Exception as e:
        logging.getLogger().error(f"Error reading json: {e}")
    return ret

def search_failed_files(directory, failed_status):
    file_list = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(Defaults.PACKAGE_METADATA_POSTFIX):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    try:
                        data = json.load(f)
                        if data['indexStatus'] in failed_status:
                            data['file_path'] = file_path 
                            file_list.append(data)
                    except json.JSONDecodeError:
                        logging.getLogger().error(f"Error decoding JSON in failed file: {file_path}")
    return file_list

def find_value_by_key(metadata_list, key):
    for item in metadata_list:
        if key == item.get('key'):
            return item.get('value')
    return None

def search_fields_values(directory, fields_to_exclude = []):
    dict_of_sets = {}
    count = 0
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.metadata.raw'):
                count += 1
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    try:
                        data = json.load(f)
                        if 'fields' in data.keys():
                            for key in [key for key in data['fields'].keys() if not key in fields_to_exclude]:
                                if not key in dict_of_sets.keys():
                                    dict_of_sets[key] = []
                                if not isinstance(data['fields'][key], list):
                                    dict_of_sets[key].append(data['fields'][key])
                    except json.JSONDecodeError:
                        logging.getLogger().error(f"Error decoding JSON in file: {file_path}")
    for key  in dict_of_sets:
        dict_of_sets[key] = list(set(dict_of_sets[key]))
    return dict_of_sets

def get_configuration(configuration_object, name):
    specific_configuration = configuration_object.get(name, None)
    if not specific_configuration:
        raise ValueError(name.capitalize() + 'configuration missing. Please check your config file.')
    return specific_configuration

def do_get(token, endpoint, headers:dict={}):
        """
        Do a get to the specified endpoint.

        Raises:
            Exception: If status code is not 200 or value is not in the response json fields.
        """
        
        headers = get_authorization_header(token, headers)
        response = requests.get(
            url=endpoint,
            headers=headers,
        )
        
        if response.status_code != 200:
            raise ValueError(response.json()["error"])
        
        return response

def get_authorization_header(token, headers:dict={}):
    complete_headers = headers
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return complete_headers

def get_new_files(paths:List[Path]):

    out_paths = []
    for item in paths:

        file = os.path.normpath(item)

        item_file_metadata = f"{file}{Defaults.PACKAGE_METADATA_POSTFIX}"
        if not os.path.exists(item_file_metadata):
            out_paths.append(item)

    return out_paths

from datetime import datetime

def parse_date(date_string):
    if date_string is None or date_string == "":
        return None, None, None, None  # Handle the case where no date is provided

    date_string = date_string.split('T')[0] # Remove the time part if present
    date_string = date_string.replace("-", "/")  # Normalize separators
    
    # Define possible date formats
    date_formats = [
        "%m/%d/%Y",  # MM/DD/YYYY
        "%Y/%m/%d",  # YYYY/MM/DD
        "%d/%m/%Y",  # DD/MM/YYYY
        "%Y-%m-%d",  # ISO format (in case hyphens remain)
        "%d-%m-%Y"   # European format with hyphens
    ]

    for fmt in date_formats:
        try:
            # Change any formato to YYYYMMDD format
            date_object = datetime.strptime(date_string, fmt)

            formatted_date = date_object.strftime("%Y%m%d")
            day = date_object.strftime("%d")
            month = date_object.strftime("%m")
            year = date_object.strftime("%Y")

            return formatted_date, day, month, year
        except ValueError:
            continue  # Try the next format

    raise ValueError(f"Date format not recognized: {date_string}")
