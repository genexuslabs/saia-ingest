## Google Drive connector

Check the [Google Drive API overview](https://developers.google.com/drive/api/guides/about-sdk) to know what you need to enable from the Google console.

Download the `credentials.json` file following [these instructions](https://developers.google.com/drive/api/quickstart/python) and set the `credentials` parameter below. Notice the process works only with a `folder_id`.

Create a `yaml` file under the `config` folder with the following parameters, let's assume `gdrive_sandbox.yaml`; contact your provider for some of these values:

```yaml
gdrive: # Mandatory
  folder_id: !!str 'string' # Check the Folder Id on the Google Drive URL
  mime_types: # Add or remove values accordingly to filter files in a folder
    - !!str 'text/plain' # Text files or any plain format
    - !!str 'application/vnd.google-apps.document' # Documents
    - !!str 'application/vnd.google-apps.spreadsheet' # Sheets
    - !!str 'application/vnd.google-apps.presentation' # Slides
  credentials: !!str 'path_to_credentials.json'
  delete_local_folder: !!bool true|false (default) # To Delete folders after the upload process
saia:
  base_url: !!str 'string' # Globant Enterprise AI Base URL
  api_token: !!str 'string'
  profile: !!str 'string' # Must match the RAG assistant ID
  max_parallel_executions: !!int 5
  upload_operation_log: !!bool False|True (default) # Check operations LOG for detail if enabled
  ingestion: # optional
```

For the `ingestion` section, check [here](../geai_ingestion.md).

### Execution

Example execution:

```bash
saia-cli ingest -c ./config/gdrive_sandbox.yaml --type gdrive
```

Expected output is similar to:

```bash
INFO:googleapiclient.discovery_cache:file_cache is only supported with oauth2client<4.0.0
INFO:root:Downloaded <X> files to C:\Users\UserName\AppData\Local\Temp\<temp_folder>
...
INFO:root:<FileName>,<Status>,<Name>,<GUID>,<elapsed_time>
...
INFO:root:Successfully gdrive ingestion 'timestamp' config: ./config/gdrive_sandbox.yaml
```

### Implementation

Check the [implementation](../saia_ingest/ingestor.py#432), it [downloads the files](../gdrive/gdrive_reader.py) to a temporary folder and then uses the [Globant Enterprise AI](../EnterpriseAISuite.md) FileUpload API.
