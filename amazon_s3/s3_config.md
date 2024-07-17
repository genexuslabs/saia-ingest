## S3 connector

Create a `yaml` file under the `config` folder with the following parameters, let's assume `jira_sandbox.yaml`; contact your provider for some of these values:

```yaml
s3: # contact the provider for the following information
  bucket: !!str 'string'
  key: !!str 'string'
  region: !!str 'string'
  aws_access_key: !!str 'string'
  aws_secret_key: !!str 'string'
  collection_name: !!str 'string'
  prefix: !!str 'string'
  url: 'string' # in general https://s3.console.aws.amazon.com
  use_local_folder: !!bool True|False (default) # Skip S3 processing and use a local folder
  local_folder: !!str 'string' # Full path to a folder
  use_metadata_file: !!bool True|False (default) # Check if a local metadata file exists
  use_augment_metadata: !!bool True|False (default) # Create new metadata based on the original
  process_files: !!bool True|False (default) # Renaming files based on the document type.
  required_exts: # list of required extensions, for example: txt, pdf, docx, pptx, xlsx
  reprocess_failed_files: !!bool True|False (default) # Check if failed uploads needs to be reprocessed
  reprocess_failed_files_file: !!str 'string' # Full path to a file
  reprocess_valid_status_list: # List of Statuses to process, valid values Unknown, Starting, Failed, Pending, Success
  delete_local_folder: !!bool True|False (default) # Delete temporary folder if created
  excluded_exts: # list of excluded extensions, by default it is suggested to include the following: raw, metadata
    - !!str 'metadata'
    - !!str 'raw'
saia:
  base_url: !!str 'string' # GeneXus Enterprise AI Base URL
  api_token: !!str 'string'
  profile: !!str 'string' # Must match the RAG assistant ID
  max_parallel_executions: !!int 5
  upload_operation_log: !!bool False|True (default) # Check operations LOG for detail if enabled
```

### Execution

Example execution taking into account updated documents from 2 days ago:

```bash
saia-cli ingest -c ./config/s3_sandbox.yaml --type s3 --days 2
```

Expected output is similar to:

```bash
INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
INFO:root:Downloading files from 'bucketname' to C:\Users\UserName\AppData\Local\Temp\tmp435tqchf
INFO:root:Skipped: <X> Total: <Y>
INFO:root:time: <Z>s # seconds
INFO:root:Successfully s3 ingestion 'timestamp' config: ./config/s3_sandbox.yaml
```
