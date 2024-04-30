## Github connector

This connector uses the `Github repository reader by [llama_index](https://github.com/run-llama/llama_index/tree/main/llama-index-integrations/readers/llama-index-readers-github).

Based on the returned list of documents you can upload it to the platform.

Create a `yaml` file under the `config` folder with the following parameters, let's assume `github_sandbox.yaml`; contact your provider for some of these values if needed:

```yaml
github:
  api_token: !!str 'string' # or GITHUB_TOKEN environment variable
  base_url: !!str 'https://api.github.com'
  api_version: !!str 'string' # defaults to 2022-11-28
  verbose: !!bool true|false (default) # Whether to print verbose messages
  owner: !!str 'string' # Owner of the repository
  repo: !!str 'string' # Name of the repository
  use_parser: !!bool true|false (default) # Whether to use the parser to extract text from files
  filter_directories_filter: INCLUDE (default) | EXCLUDE
  filter_directories: # List of filters
    - !!str 'filter 1'
    - !!str 'filter 2'
  filter_file_extensions_filter: INCLUDE (default) | EXCLUDE
  filter_file_extensions:  # List of extensions
    - !!str '.extension 1'
    - !!str '.extension 2'
  concurrent_requests: !!int integer
  branch: !!str 'main' # exclusive with commit_sha
  commit_sha: !!str ''
  namespace: !!str 'namespace name' # Must match the associated RAG assistant, check the index section
vectorstore:
  api_key: !!str 'check with the provider'
  index_name: !!str 'check with the provider'
embeddings:
  openapi_key: !!str 'check with the provider' # Or use your own
  chunk_size: !!int integer # DefaultVectorStore.CHUNK_SIZE by default
  chunk_overlap: !!int integer # DefaultVectorStore.CHUNK_OVERLAP by default
  model: !!str name # defaults to text-embedding-ada-002
```

### Execution

Example execution:

```bash
saia-cli ingest -c ./config/github_sandbox.yaml --type github
```

Expected output is similar to:

```bash
INFO:root:Successfully github ingestion 'timestamp' config: <path_to_config.yaml>
```

Use the `verbose` parameter to get detail of the processing steps.

__Tip__: under the `debug` folder, the `{provider}_YYYYMMDDHHMMSS.json` is the result of the issues ingestion and can be uploaded to any RAG assistant if you use the `.custom` extension when uploading the file.
