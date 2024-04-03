## Installation

To install the package and its dependencies, follow these steps:

1. Create a new virtual environment and activate it, for this case called `venv`:

   ```bash
   # Ubuntu
   sudo apt install python3-venv
   python3 -m venv venv
   source venv/bin/activate
   # Windows
   python -m venv venv
   .\venv\Scripts\Activate
   ```
2. Install the package dependencies:

   ```bash
   # install poetry first
   pip install poetry
   # fist time
   poetry install
   # on repository updates
   poetry update
   ```
3. Set the `PYTHONPATH` environment variable to the path of the current directory:

   ```bash
   export PYTHONPATH="$PYTHONPATH:$(pwd)"
   ```

## Configuration

### Variables

Depending on the command used you may need to set some environment variables

```
export OPENAI_API_KEY=<your API Key>
# set it to be used always
echo "export OPENAI_API_KEY=X" >> ~/.bashrc
```

### YAML

Make sure to set the correct `yaml` configuration file under the `config` folder. If it does not exists, create a `config` folder under the repository. All command will use configuration files from that folder by default. 

Run the associated operation using the `saia-cli` entry point, supported the `ingest` verb only.

```bash
saia-cli ingest -c ./config/s3_ls.yaml
# using a timestamp
saia-cli ingest -c ./config/s3_ls.yaml -t 2023-12-21
# using a type
saia-cli ingest -c ./config/s3_ls.yaml --type test
```

## Run Tests

ToDo add tests, so far a dummy one just to check the mechanism is working.

```python
pytest tests/test_api.py
```

## License

check [here](LICENSE).
