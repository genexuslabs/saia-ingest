"""Init file"""

from .ingestor import ingest_s3, ingest_jira, ingest_confluence, ingest_github

def dummy():
    print("Dummy function")
    return
