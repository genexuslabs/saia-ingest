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
saia:
  base_url: !!str 'string' # GeneXus Enterprise AI Base URL
  api_token: !!str 'string'
  profile: !!str 'string' # Must match the RAG assistant ID
  max_parallel_executions: !!int 5
  upload_operation_log: True # Check operations LOG for detail if enabled
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
INFO:root:Skipped: 5 Total: 5
INFO:root:time: 3.24s
INFO:root:Successfully s3 ingestion 'timestamp' config: ./config/s3_sandbox.yaml
```
