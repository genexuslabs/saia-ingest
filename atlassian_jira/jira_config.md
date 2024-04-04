## JIRA connector

Create a `yaml` file under the `config` folder with the following parameters, let's assume `jira_sandbox.yaml`; contact your provider for some of these values:

```yaml
jira:
  email: !!str 'string'
  api_token: !!str 'some_token' # get it from JIRA
  server_url: !!str 'somedomain.atlassian.net' # check with your provider
  project: !!str 'some project'
  query: !!str 'filter=10002' # Create a filter for the issues and check the associated ID
  namespace: !!str 'namespace name' # Must match the associated RAG assistant, check the index section
vectorstore:
  api_key: !!str 'check with the provider'
  environment: !!str 'check with the provider'
  index_name: !!str 'check with the provider'
embeddings:
  openapi_key: !!str 'check with the provider'
  chunk_size: !!int integer # 1000 by default
  chunk_overlap: !!int integer # 100 by default
```

### Execution

Example execution:

```bash
saia-cli ingest -c ./config/jira_sandbox.yaml --type jira
```

Expected output is similar to:

```bash
INFO:root:processed 10 from 10
INFO:root:Successfully jira ingestion 'no timestamp' config: ./config/jira_sandbox.yaml
```

__Tip__: under the `debug` folder, the `jira_YYYYMMDDHHMMSS.json` is the result of the issues ingestion and can be uploaded to any RAG assistant if you use the `.custom` extension when uploading the file.

