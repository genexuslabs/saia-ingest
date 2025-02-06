## File System

Create a `yaml` file under the `config` folder with the following parameters, let's assume `fs_sandbox.yaml`:

```yaml
fs:
  input_dir: !!str 'string' # Full path to a folder
  required_exts: # list of required extensions
    - !!str .md # just an example, include the "dot" !
  recursive: True|False (default)
  delete_local_folder: True|False (default)
  use_metadata_file: True|False (default)
saia:
  base_url: !!str 'string' # Globant Enterprise AI Base URL
  api_token: !!str 'string'
  profile: !!str 'string' # Must match the RAG assistant ID
  max_parallel_executions: !!int 5
  upload_operation_log: !!bool False|True (default) # Check operations LOG for detail if enabled
  ingestion: # optional key-value parameters from https://wiki.genexus.com/enterprise-ai/wiki?581,Ingestion+Provider
    # example
    provider: !!str geai
    structure: !!str table
    dpi: !!int 205
```

The process will read files from the file system `fs` section and upload them to the defined `Enterprise AI` endpoint detailed on the `saia` configuration.

### Execution

Example execution only considering files modified since `yesterday`:

```bash
saia-cli ingest -c ./config/fs_sandbox.yaml --type fs --days 1
```
