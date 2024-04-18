## Confluence connector

Create a `yaml` file under the `config` folder with the following parameters, let's assume `confluence_sandbox.yaml`; contact your provider for some of these values:

```yaml
confluence:
  email: !!str 'string'
  api_token: !!str 'some_token' # get it from Confluence
  server_url: !!str 'somedomain.atlassian.net/wiki' # check with your provider
  spaces: # List of spaces to process
    - !!str 'space 1'
    - !!str 'space 2'
  include_attachments: !!bool true|false (default)
  include_children: !!bool true|false (default)
  cloud: !!bool true|false (default)
  namespace: !!str 'namespace name' # Must match the associated RAG assistant, check the index section
vectorstore:
  api_key: !!str 'check with the provider'
  index_name: !!str 'check with the provider'
embeddings:
  openapi_key: !!str 'check with the provider' # Or use your own
  chunk_size: !!int integer # 1000 by default
  chunk_overlap: !!int integer # 100 by default
```

### Execution

Example execution:

```bash
saia-cli ingest -c ./config/confluence_sandbox.yaml --type confluence
```

Expected output is similar to:

```bash
INFO:root:space <space_name1> documents <number>
INFO:root:space <space_name2> documents <number>
...
INFO:root:Documents <total_number> Chunks <total_chunks>
INFO:root:Successfully confluence ingestion 'timestamp' config: <path_to_config.yaml>
```

__Tip__: under the `debug` folder, the `{provider}_YYYYMMDDHHMMSS.json` is the result of the issues ingestion and can be uploaded to any RAG assistant if you use the `.custom` extension when uploading the file.
