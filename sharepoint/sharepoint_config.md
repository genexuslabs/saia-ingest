## Sharepoint connector

The connector can load files from a folder in a sharepoint site; it also supports traversing recursively through the sub-folders.

Check the following prerequisites

1. You need to create an App Registration in [Microsoft Entra ID](https://learn.microsoft.com/en-us/azure/healthcare-apis/register-application)
2. API Permissions for the created app.
   1. Microsoft Graph --> Application Permissions --> Sites.ReadAll (**Grant Admin Consent**)
   2. Microsoft Graph --> Application Permissions --> Files.ReadAll (**Grant Admin Consent**)
   3. Microsoft Graph --> Application Permissions --> BrowserSiteLists.Read.All (**Grant Admin Consent**)

More info on Microsoft Graph APIs - [Refer here](https://learn.microsoft.com/en-us/graph/permissions-reference)

## Usage

Create a `yaml` file under the `config` folder with the following parameters, let's assume `sharepoint_sandbox.yaml`; contact your provider for some of these values:

```yaml
sharepoint: # contact the provider for the following information
  client_id: !!str 'string'
  client_secret: !!str 'string'
  tenant_id: !!str 'string'
  sharepoint_sites_names: # List of Site Names containing a list of subfolders
   siteItemName1: ['Folder1', 'Folder2', ...]
   siteItemName2: ['Folder1', 'Folder2', ...]
  download_dir: !!str 'string' # Download folder where the files are downloaded.
  depth: !!number # Depth to check when processing sub-folders.
  reprocess_failed_files: !!bool True|False (default) # Check if failed uploads needs to be reprocessed
  reprocess_valid_status_list: # List of Statuses to process, valid values Unknown, Starting, Failed, Pending, Success
  metadata_policy: # Optional
    include_fields: !!bool True|False (default) # Consider extra metadata fields?
    fields: [
      'MetadataName_1',
      ...
      'MetadataName_n'
    ]
    translation_needed: ['LookupIdItem_1', ..., 'LookupIdItem_n'] # Dictionary of items that needs to be mapped to other values
    translation_folder: !!str # Fullpath to a configuration yaml file
    field_rename: # Renaming of fields
      MetadataFieldName_1: 
        new_name: !!str NewMetadataFieldName_1
        delete_old: !!bool True|False (default) # Remove original metadata element?
    # Specific for Date Management
    date_fields: ['DateMetadataItem_1', ... , 'DateMetadataItem_2']
    date_input_format: !!str '%Y-%m-%dT%H:%M:%SZ' # Date Input format
    date_output_format: !!str '%Y%m%d' # Date Output format
saia:
  base_url: !!str 'string' # GeneXus Enterprise AI Base URL
  api_token: !!str 'string'
  profile: !!str 'string' # Must match the RAG assistant ID
  max_parallel_executions: !!int 5
  upload_operation_log: !!bool False|True (default) # Check operations LOG for detail if enabled
```

The parameters `client_id`, `client_secret` and `tenant_id` are those of the registered app in Microsoft Azure Portal.

### Execution

Example execution taking into account documents located in `sharepoint_folder_path`:

```bash
saia-cli ingest -c ./config/sharepoint_sandbox.yaml --type sharepoint
```

Expected output is similar to:

```bash
INFO:root:<RAG Assistant Name> is a valid profile.
INFO:root:Checking for files to sync with <RAG Assistant Name> profile.
INFO:root:Deleting files with status: ['Unknown', 'Pending'].
INFO:root:Downloading files from sharepoint.
INFO:sharepoint.sharepoint_reader:Downloading files with id 'ID' to <tmp_folder>
Downloading files with id 'FileName' to <tmp_folder>
INFO:sharepoint.sharepoint_reader:Download finished.
INFO:root:Uploading files to <RAG Assistant Name>
....
....
INFO:root:Upload finished.
INFO:root:time: Xs
INFO:root:Successfully sharepoint ingestion 'timestamp' config: ./config/sharepoint_sandbox.yaml
```
