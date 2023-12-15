import yaml

def get_yaml_config(yaml_file):
    # Load the configuration from the YAML file
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config



