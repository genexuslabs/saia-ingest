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


def get_metadata_file(file_path, file_name) -> str:
    # Get the metadata file content
    ret = None
    new_file_name = change_file_extension(file_name, '.json')
    metadata_file = os.path.join(file_path, new_file_name)
    if os.path.isfile(metadata_file):
        try:
            with open(metadata_file, 'r') as json_file:
                ret = json.load(json_file)
        except Exception as e:
            pass
    return ret

