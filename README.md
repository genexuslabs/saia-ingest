[![pytest](https://github.com/genexuslabs/saia-ingest/actions/workflows/pytest.yml/badge.svg)](https://github.com/genexuslabs/saia-ingest/actions/workflows/pytest.yml)

Welcome to the [GeneXus Enterprise AI](./EnterpriseAISuite.md) Ingest utilities package, codename `saia-ingest`.

It's purpose is to provide sample code to connect to different data sources and help external developers to interact with the platform to upload documents. Check the [configuration](#configuration) section to know the available data-sources.

Check the [API](API.md) section if you want to get sample code to explore the [GeneXus Enterprise AI](./EnterpriseAISuite.md) API.

You can use this repository as reference to extend it for other [data sources](#data-sources); please let us know if you create [new connectors](./CONTRIBUTION.md) and contribute back!

## Getting started

 * Clone this repository.
 * [Install](#installation) it locally.
 * Define your [data source](#data-sources) to use and configure a [yaml](#yaml) file.
 * Execute it using the samples provided.

## Installation

To install the package and its dependencies, follow these steps:

1. Create a new virtual environment and activate it, for this case we will create one called `venv`:

```bash
# Linux
sudo apt install python3-venv
python3 -m venv venv
source venv/bin/activate
# MAC
virtualenv venv
source venv/bin/activate
# Windows
python -m venv venv
.\venv\Scripts\Activate
```

2. Install the package dependencies:

```bash
# install poetry first
pip install poetry
# every time you update
poetry install
```

3. Set the `PYTHONPATH` environment variable to the path of the current directory:

```bash
export PYTHONPATH="$PYTHONPATH:$(pwd)"
```

Now the package is locally installed, continue defining a [configuration file](#yaml).

## Configuration

### Variables

Depending on the command used you may need to set some environment variables such as:

```
export OPENAI_API_KEY=<your API Key>
# set it to be used always
echo "export OPENAI_API_KEY=X" >> ~/.bashrc
```

### YAML

Make sure to set the correct `yaml` configuration file under the `config` folder. If it does not exists, create a `config` folder under the repository. All command will use configuration files from that folder by default. 

Run the associated operation using the `saia-cli` entry point, supported the `ingest` verb only.

```bash
saia-cli ingest -c ./config/s3_sandbox.yaml
# using a timestamp
saia-cli ingest -c ./config/s3_sandbox.yaml -t 2023-12-21
# using a type
saia-cli ingest -c ./config/s3_sandbox.yaml --type test
```

### Data Sources

The configuration file details all parameters needed to run the ingestion, use the `--type` to decide the target ingestion; supported data sources are:

 * `s3` [config](./amazon_s3/s3_config.md)
 * `jira` [config](./atlassian_jira/jira_config.md)
 * `confluence` [config](./atlassian_confluence/confluence_config.md)
 * `github` [config](./docs/github_config.md)
 * `gdrive` Google Drive [config](./gdrive/gdrive_config.md)
 * `sharepoint` [config](./sharepoint/sharepoint_config.md)

### Logging

Check the `debug` folder, where every execution is logged.

## Run Tests

ToDo add tests, so far a simple one just to check the mechanism is working, make sure to create a [configuration](#yaml) file.

```python
pytest tests/test_api.py
pytest tests/test_proxy.py
```

## Contribution

check [here](CONTRIBUTION.md).

## License

check [here](LICENSE).
