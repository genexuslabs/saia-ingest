import os
from datetime import datetime
import json

from llama_index import QueryBundle
from llama_index.retrievers import BaseRetriever
from typing import Any, List

from langchain.embeddings import OpenAIEmbeddings
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import Pinecone
from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter

from .utils import get_yaml_config
from .vectorstore import initialize_vectorstore_connection, get_vectorstore_index
# tweaked the implementation locally
from atlassian_jira.jirareader import JiraReader
from atlassian_confluence.confluencereader import ConfluenceReader
from amazon_s3.s3reader import S3Reader

from llama_hub.github_repo import GithubClient, GithubRepositoryReader

verbose = False

def split_documents(documents, chunk_size=1000, chunk_overlap=100):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    lc_documents = text_splitter.split_documents(documents)
    return lc_documents

def load_documents(loader, space_key='', include_attachments=False, include_children=False):
    documents = loader.load_langchain_documents(space_key=space_key, include_attachments=include_attachments, include_children=include_children)
    return documents

def ingest(lc_documents, index_name, namespace):
    # https://python.langchain.com/docs/integrations/vectorstores/pinecone

    embeddings = OpenAIEmbeddings()
    vectorstore = Pinecone.from_documents(documents=lc_documents, embedding=embeddings, index_name=index_name, namespace=namespace)

def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def save_to_file(lc_documents, prefix='module'):
    try:
        debug_folder = os.path.join(os.getcwd(), 'debug')
        create_folder(debug_folder)

        serialized_docs = []
        for x in lc_documents:
            doc = {
                'pageContent': x.page_content,
                'metadata': x.metadata
            }
            serialized_docs.append(doc)

        now = datetime.now()
        formatted_timestamp = now.strftime("%Y%m%d%H%M%S") # Format the datetime object as YYYYMMDDHHMMSS
        filename = '%s_%s.json' % (prefix, formatted_timestamp)
        file_path = os.path.join(debug_folder, filename)
        with open(file_path, 'w', encoding='utf8') as json_file:
            json.dump(serialized_docs, json_file, ensure_ascii=False, indent=4)
    except Exception as e:
        print('save_to_file exception:', e)


def ingest_jira(configuration: str) -> bool:
    ret = True
    try:
        # Configuration
        config = get_yaml_config(configuration)
        email = config['jira']['email']
        api_token = config['jira']['api_token']
        jira_server_url = config['jira']['server_url']
        query = config['jira']['query']

        # Reader
        reader = JiraReader(email=email, api_token=api_token, server_url=jira_server_url)

        # Load documents
        maxResults = 100
        startAt = 0
        keep_processing = True
        all_documents = []

        while keep_processing: 
            documents, total = reader.load_langchain_documents(query=query, startAt=startAt, maxResults=maxResults)
            maxResults = documents.__len__()
            all_documents.extend(documents)
            current_length = all_documents.__len__()
            startAt += maxResults
            if current_length >= total:
                keep_processing = False

        print(f"processed {current_length} from {total}")

        lc_documents = all_documents

        save_to_file(lc_documents, prefix='jira')

        # Vectorstore
        api_key = config['vectorstore']['api_key']
        environment = config['vectorstore']['environment']
        index_name = config['vectorstore']['index_name']
        jira_namespace = config['jira']['namespace']
        initialize_vectorstore_connection(api_key=api_key, environment=environment)

        ingest(lc_documents, index_name, jira_namespace)        

    except Exception as e:
        print(f"Error: {e}")
        ret = False
    finally:
        return ret
    
def ingest_confluence(configuration: str) -> bool:
    ret = True
    try:
        config = get_yaml_config(configuration)
        user_name = config['confluence']['email']
        conf_token = config['confluence']['api_token']
        confluence_server_url = config['confluence']['server_url']
        space_keys = config['confluence']['spaces']
        include_attachments = config['confluence']['include_attachments']
        include_children = config['confluence']['include_children']
        cloud = config['confluence']['cloud']
        confluence_namespace = config['confluence']['namespace']
        openapi_key = config['embeddings']['openapi_key']
        chunk_size = config['embeddings']['chunk_size']
        chunk_overlap = config['embeddings']['chunk_overlap']
        vectorstore_api_key = config['vectorstore']['api_key']

        os.environ['OPENAI_API_KEY'] = openapi_key
        os.environ['CONFLUENCE_USERNAME'] = user_name
        os.environ['CONFLUENCE_PASSWORD'] = conf_token
        os.environ['PINECONE_API_KEY'] = vectorstore_api_key

        loader = ConfluenceReader(base_url=confluence_server_url, cloud=cloud)

        documents = []
        for key in space_keys:
            try:
                space_documents = load_documents(loader, space_key=key, include_attachments=include_attachments, include_children=include_children)
                for item in space_documents:
                    documents.append(item)
                print(f"space {key} documents {space_documents.__len__()}")
            except Exception as e:
                print(f"Error processing {key}: {e}")
                continue

        lc_documents = split_documents(documents, chunk_size=chunk_size, chunk_overlap=chunk_overlap)

        save_to_file(lc_documents, prefix='confluence')

        # Vectorstore
        environment = config['vectorstore']['environment']
        index_name = config['vectorstore']['index_name']
        initialize_vectorstore_connection(api_key=vectorstore_api_key, environment=environment)

        print(f"Documents {documents.__len__()} Chunks {lc_documents.__len__()}")

        ingest(lc_documents, index_name, confluence_namespace)

    except Exception as e:
        print(f"Error: {e}")
        ret = False
    finally:
        return ret


def ingest_github(configuration: str) -> bool:
    ret = True
    try:
        config = get_yaml_config(configuration)

        github_token = config['github']['api_token']
        base_url = config['github']['base_url']
        api_version = config['github']['api_version']
        verbose = config['github']['verbose']
        owner = config['github']['owner']
        repo = config['github']['repo']
        filter_directories = config['github']['filter_directories']
        filter_file_extensions = config['github']['filter_file_extensions']
        concurrent_requests = config['github']['concurrent_requests']
        branch = config['github']['branch']
        commit_sha = config['github']['commit_sha'] or None
        namespace = config['github']['namespace']

        chunk_size = config['embeddings']['chunk_size']
        chunk_overlap = config['embeddings']['chunk_overlap']

        vectorstore_api_key = config['vectorstore']['api_key']
        environment = config['vectorstore']['environment']
        index_name = config['vectorstore']['index_name']

        github_client = GithubClient(github_token, base_url, api_version, verbose)
        loader = GithubRepositoryReader(
            github_client,
            owner = owner,
            repo = repo,
            filter_directories = (filter_directories, GithubRepositoryReader.FilterType.INCLUDE),
            filter_file_extensions = (filter_file_extensions, GithubRepositoryReader.FilterType.INCLUDE),
            verbose = verbose,
            concurrent_requests = concurrent_requests
        )

        documents = loader.load_langchain_documents(commit_sha=commit_sha, branch=branch)

        if documents.__len__() <= 0:
            print('No documents found')
            return False
        
        lc_documents = split_documents(documents, chunk_size=chunk_size, chunk_overlap=chunk_overlap)

        save_to_file(lc_documents, prefix='github')

        initialize_vectorstore_connection(api_key=vectorstore_api_key, environment=environment)

        ingest(lc_documents, index_name, namespace)

    except Exception as e:
        print(f"Error: {e}")
        ret = False
    finally:
        return ret


def ingest_s3(
        configuration: str,
        timestamp: datetime = None,
    ) -> bool:
    ret = True
    try:
        config = get_yaml_config(configuration)
        url = config['s3']['url']
        region = config['s3']['region']
        bucket = config['s3']['bucket']
        key = config['s3']['key']
        aws_access_key = config['s3']['aws_access_key']
        aws_secret_key = config['s3']['aws_secret_key']
        prefix = config['s3']['prefix'] or None

        loader = S3Reader(
            s3_endpoint_url=url,
            region_name=region,
            bucket=bucket,
            key=key,
            prefix=prefix,
            aws_access_id=aws_access_key,
            aws_access_secret=aws_secret_key,
            timestamp=timestamp
            )
    
        documents = loader.load_langchain_documents()

        save_to_file(documents, prefix='s3')

        if verbose:
            for document in documents:
                print(document.lc_id, document.metadata)

        # Vectorstore
        api_key = config['vectorstore']['api_key']
        environment = config['vectorstore']['environment']
        index_name = config['vectorstore']['index_name']
        namespace = config['vectorstore']['namespace']

        doc_count = documents.__len__()
        if doc_count <= 0:
            return ret

        print(f"Vectorizing {doc_count} items to {index_name}/{namespace}")

        initialize_vectorstore_connection(api_key=api_key, environment=environment)

        ingest(documents, index_name, namespace)

    except Exception as e:
        print(f"Error: {e}")
        ret = False
    finally:
        return ret
