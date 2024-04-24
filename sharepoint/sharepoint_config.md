## Sharepoint connector

Create a `yaml` file under the `config` folder with the following parameters, let's assume `sharepoint_sandbox.yaml`; contact your provider for some of these values:

```yaml
s3: # contact the provider for the following information
  client_id: !!str 'string'
  client_secret: !!str 'string'
  tenant_id: !!str 'string'
  sharepoint_site_name: !!str 'string'
  sharepoint_folder_path: !!str 'string'
  download_dir: !!str 'string' # Download folder where the files are downloaded.
  recursive: True # Set if files from subfolders are download.
saia:
  base_url: !!str 'string' # GeneXus Enterprise AI Base URL
  api_token: !!str 'string'
  profile: !!str 'string' # Must match the RAG assistant ID
  max_parallel_executions: !!int 5
  upload_operation_log: True # Check operations LOG for detail if enabled
```

### Execution

Example execution taking into account documents located in `sharepoint_folder_path`:

```bash
saia-cli ingest -c ./config/sharepoint_sandbox.yaml --type sharepoint
```
