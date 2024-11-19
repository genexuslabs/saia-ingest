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
sharepoint:
  connection: # contact the provider for the following information, they are mandatory
    client_id: !!str string
    client_secret: !!str string
    tenant_id: !!str string
  sites: # List of sites to consider for downloading
    - name: !!str string # Site name
      drives: # List of site drives, default to all drives
        - name: !!str 'string' # drive name
          paths: # (Optional) paths to consider inside the drive 
            - path: "/" # (Optional) Path
              depth: 2 # (Optional) max depth to consider
  reprocess: # (Optional)
    reprocess_failed_files: !!bool True|False (default) # Check if failed uploads needs to be reprocessed
    avoid_download: !!bool True|False (default)  # (Optional) should re-download the file?
    failed_status: # (Optional) # List of Statuses to process, valid values Unknown, Starting, Failed, Pending, Success
      - Failed # just an example
    retry_count: 1 # Defaults to 1
  metadata_processing_policy: # (Optional)
    map_to_decode: #  (Optional)  Replace text map when getting the metadata.
      _x007e_: "~"
      _x0021_: "!"
    fields:
      exclude_fields: !!bool True|False (default) # (Optional) should the "fields" element be excluded from the metadata?
      fields: # list of fields to be excluded from metadata 
        - field1
        - field2
    map_fields: # (Optional) How mapping should be done
      - name: 'SampleFieldId'
        new_values: C:/tmp/config/SampleFieldId.yaml # place here the mapping needed, useful for changing IDs to descriptions
        default_value: '' # If no mapping set use this value
    rename: # (Optional) fields to be renamed
      - old_name: field1 # current field name
        new_name: field2 # new mame
        delete_old: !!bool True|False (default) # should delete the old field?
    dates_format: # (Optional)
      - names:
        - date_field1
        - date_field2
        input_format: '%Y-%m-%dT%H:%M:%SZ' # Format to interpret the field
        output_format: '%Y%m%d' # Fromat used to store in the metadata.
general: # (Optional) Folder download
  download:
    clean_status: # (Optional) File status to consider for cleanup when finished
      - Success
    files: !!str string # (Optional) Folder where documents are downloaded, otherwise using a temporal one
    metadata: !!str string # (Optional) Folder where metadata is constructed, by default uses the files property
saia:
  base_url: !!str string # Globant Enterprise AI Base URL
  api_token: !!str string
  profile: !!str string # Must match the RAG assistant ID
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
