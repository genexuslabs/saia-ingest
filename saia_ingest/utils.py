import os
import json
import magic
import logging
import yaml

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

def get_metadata_file(file_path, file_name, metadata_extension =  '.json') -> dict:
    # Get the metadata file content
    ret = None
    new_file_name = change_file_extension(file_name, metadata_extension)
    metadata_file = os.path.join(file_path, new_file_name)
    if os.path.isfile(metadata_file):
        ret = load_json_file(metadata_file)
    return ret

def load_json_file(file_path) -> dict:
    ret = None
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            ret = json.load(json_file)
    except Exception as e:
        pass
    return ret

def search_failed_files(directory, failed_status):
    file_list = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.saia.metadata'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    try:
                        data = json.load(f)
                        if data['indexStatus'] in failed_status:
                            data['file_path'] = file_path 
                            file_list.append(data)
                    except json.JSONDecodeError:
                        print(f"Error decoding JSON in file: {file_path}")
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
                        print(f"Error decoding JSON in file: {file_path}")
    for key  in dict_of_sets:
        dict_of_sets[key] = list(set(dict_of_sets[key]))
    return dict_of_sets
