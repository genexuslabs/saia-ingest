import os
import time
from datetime import datetime, timezone
import json
import concurrent.futures

#from llama_index import QueryBundle
#from llama_index.retrievers import BaseRetriever
#from typing import Any, List

from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Pinecone
from langchain.text_splitter import RecursiveCharacterTextSplitter

from saia_ingest.profile_utils import is_valid_profile, file_upload, file_delete, operation_log_upload, sync_failed_files, search_failed_to_delete
from saia_ingest.rag_api import RagApi
from saia_ingest.utils import get_yaml_config, get_metadata_file, load_json_file, search_failed_files, find_value_by_key

# tweaked the implementation locally
from atlassian_jira.jirareader import JiraReader
from atlassian_confluence.confluencereader import ConfluenceReader
from amazon_s3.s3reader import S3Reader
from gdrive.gdrive_reader import GoogleDriveReader

from llama_hub.github_repo import GithubClient, GithubRepositoryReader

from saia_ingest.config import DefaultVectorStore

from typing import List, Dict
import logging
import shutil


verbose = False

def split_documents(documents, chunk_size=DefaultVectorStore.CHUNK_SIZE, chunk_overlap=DefaultVectorStore.CHUNK_OVERLAP):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    lc_documents = text_splitter.split_documents(documents)
    return lc_documents

def load_documents(loader, space_key='', page_ids=None, include_attachments=False, include_children=False):
    documents = loader.load_langchain_documents(space_key=space_key, page_ids=page_ids, include_attachments=include_attachments, include_children=include_children)
    return documents

def ingest(lc_documents, api_key, index_name, namespace, model="text-embedding-ada-002"):
    # https://python.langchain.com/docs/integrations/vectorstores/pinecone

    embeddings = OpenAIEmbeddings(api_key=api_key, model=model)
    vectorstore = Pinecone.from_documents(documents=lc_documents, embedding=embeddings, index_name=index_name, namespace=namespace)

def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def check_valid_profile(rag_api, profile_name):
    ret = rag_api.is_valid_profile(profile_name)
    if not ret:
        logging.getLogger().error(f"Invalid profile {profile_name}")
    return ret

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
        return file_path
    except Exception as e:
        logging.getLogger().error('save_to_file exception:', e)

def ingest_jira(configuration: str) -> bool:
    ret = True
    start_time = time.time()
    try:
        # Configuration
        config = get_yaml_config(configuration)
        jira_level = config.get('jira', {})
        email = jira_level.get('email', None)
        api_token = jira_level.get('api_token', None)
        jira_server_url = jira_level.get('server_url', None)
        query = jira_level.get('query', None)

        embeddings_level = config.get('embeddings', {})
        openapi_key = embeddings_level.get('openapi_key', None)
        embeddings_model = embeddings_level.get('model', 'text-embedding-ada-002')

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

        logging.getLogger().info(f"processed {current_length} from {total}")

        lc_documents = all_documents

        if len(lc_documents) <= 0:
            logging.getLogger().warn('No documents found')
            return ret

        docs_file = save_to_file(lc_documents, prefix='jira')

        # Saia
        saia_level = config.get('saia', {})
        saia_base_url = saia_level.get('base_url', None)
        saia_api_token = saia_level.get('api_token', None)
        saia_profile = saia_level.get('profile', None)
        upload_operation_log = saia_level.get('upload_operation_log', False)
    
        if saia_base_url is not None:

            ragApi = RagApi(saia_base_url, saia_api_token, saia_profile)

            target_file = f"{docs_file}.custom"
            shutil.copyfile(docs_file, target_file)

            response_body = ragApi.upload_document_with_metadata_file(target_file) # ToDo check .metadata
            if response_body is None:
                logging.getLogger().error("Error uploading document")
                return False
            
            if upload_operation_log:
                end_time = time.time()
                message_response = f"bulk ingest ({end_time - start_time:.2f}s)"
                ret = operation_log_upload(saia_base_url, saia_api_token, saia_profile, "ALL", message_response, 0)

        else:
            ## Fallback to directly ingest to vectorstore
            vectorstore_level = config.get('vectorstore', {})
            vectorstore_api_key = vectorstore_level.get('api_key', None)
            index_name = vectorstore_level.get('index_name', None)

            jira_namespace = jira_level.get('namespace', None)

            os.environ['OPENAI_API_KEY'] = openapi_key
            os.environ['PINECONE_API_KEY'] = vectorstore_api_key

            ingest(lc_documents, openapi_key, index_name, jira_namespace, embeddings_model)

    except Exception as e:
        logging.getLogger().error(f"Error: {e}")
        ret = False
    finally:
        return ret
    
def ingest_confluence(
        configuration: str,
        timestamp: datetime = None,
    ) -> bool:

    ret = True
    start_time = time.time()
    try:
        config = get_yaml_config(configuration)
        confluence_level = config.get('confluence', {})
        user_name = confluence_level.get('email', None)
        conf_token = confluence_level.get('api_token', None)
        confluence_server_url = confluence_level.get('server_url', None)
        space_keys = confluence_level.get('spaces', None)
        page_ids = confluence_level.get('page_ids', None)
        include_attachments = confluence_level.get('include_attachments', None)
        include_children = confluence_level.get('include_children', None)
        cloud = confluence_level.get('cloud', None)
        confluence_namespace = confluence_level.get('namespace', None)

        embeddings_level = config.get('embeddings', {})
        openapi_key = embeddings_level.get('openapi_key', None)
        chunk_size = embeddings_level.get('chunk_size', None)
        chunk_overlap = embeddings_level.get('chunk_overlap', None)
        embeddings_model = embeddings_level.get('model', 'text-embedding-ada-002')

        vectorstore_level = config.get('vectorstore', {})
        vectorstore_api_key = vectorstore_level.get('api_key', None)

        os.environ['OPENAI_API_KEY'] = openapi_key
        os.environ['CONFLUENCE_USERNAME'] = user_name
        os.environ['CONFLUENCE_PASSWORD'] = conf_token
        os.environ['PINECONE_API_KEY'] = vectorstore_api_key

        loader = ConfluenceReader(base_url=confluence_server_url, cloud=cloud, timestamp=timestamp)

        documents = []

        if page_ids is not None:
                try:
                    list_documents = load_documents(loader, page_ids=page_ids, include_attachments=include_attachments, include_children=include_children)
                    for item in list_documents:
                        documents.append(item)
                except Exception as e:
                    logging.getLogger().error(f"Error processing {page_ids}: {e}")
        elif space_keys is not None:
            for key in space_keys:
                try:
                    space_documents = load_documents(loader, space_key=key, include_attachments=include_attachments, include_children=include_children)
                    for item in space_documents:
                        documents.append(item)
                    logging.getLogger().info(f"space {key} documents {space_documents.__len__()}")
                except Exception as e:
                    logging.getLogger().error(f"Error processing {key}: {e}")
                    continue

        lc_documents = split_documents(documents, chunk_size=chunk_size, chunk_overlap=chunk_overlap)

        docs_file = save_to_file(lc_documents, prefix='confluence')

        # Saia
        saia_level = config.get('saia', {})
        saia_base_url = saia_level.get('base_url', None)
        saia_api_token = saia_level.get('api_token', None)
        saia_profile = saia_level.get('profile', None)
        upload_operation_log = saia_level.get('upload_operation_log', False)
    
        if saia_base_url is not None:

            ragApi = RagApi(saia_base_url, saia_api_token, saia_profile)

            target_file = f"{docs_file}.custom"
            shutil.copyfile(docs_file, target_file)

            response_body = ragApi.upload_document_with_metadata_file(target_file) # ToDo check .metadata
            if response_body is None:
                logging.getLogger().error("Error uploading document")
                return False
            
            if upload_operation_log:
                end_time = time.time()
                message_response = f"bulk ingest ({end_time - start_time:.2f}s)"
                ret = operation_log_upload(saia_base_url, saia_api_token, saia_profile, "ALL", message_response, 0)

        else:
            ## Fallback to directly ingest to vectorstore
            index_name = vectorstore_level.get('index_name', None)

            logging.getLogger().info(f"Documents {documents.__len__()} Chunks {lc_documents.__len__()}")

            ingest(lc_documents, openapi_key, index_name, confluence_namespace, embeddings_model)

    except Exception as e:
        logging.getLogger().error(f"Error: {e}")
        ret = False
    finally:
        return ret

def ingest_github(configuration: str) -> bool:
    ret = True
    start_time = time.time()
    try:
        config = get_yaml_config(configuration)

        gh_level = config.get('github', {})
        github_token = gh_level.get('api_token', None)
        base_url = gh_level.get('base_url', None)
        api_version = gh_level.get('api_version', None)
        verbose = gh_level.get('verbose', None)
        owner = gh_level.get('owner', None)
        repo = gh_level.get('repo', None)
        filter_directories = gh_level.get('filter_directories', None)
        filter_directories_filter = gh_level.get('filter_directories_filter', 'INCLUDE')
        filter_file_extensions = gh_level.get('filter_file_extensions', None)
        filter_file_extensions_filter = gh_level.get('filter_file_extensions_filter', 'INCLUDE')
        concurrent_requests = gh_level.get('concurrent_requests', None)
        branch = gh_level.get('branch', None)
        commit_sha = gh_level.get('commit_sha', None)
        namespace = gh_level.get('namespace', None)
        use_parser = gh_level.get('use_parser', False)

        # Saia
        saia_level = config.get('saia', {})
        saia_base_url = saia_level.get('base_url', None)
        saia_api_token = saia_level.get('api_token', None)
        saia_profile = saia_level.get('profile', None)
        upload_operation_log = saia_level.get('upload_operation_log', False)

        embeddings_level = config.get('embeddings', {})
        openapi_key = embeddings_level.get('openapi_key', None)
        chunk_size = embeddings_level.get('chunk_size', None)
        chunk_overlap = embeddings_level.get('chunk_overlap', None)
        embeddings_model = embeddings_level.get('model', 'text-embedding-ada-002')

        vectorstore_level = config.get('vectorstore', {})
        vectorstore_api_key = vectorstore_level.get('api_key', None)
        index_name = vectorstore_level.get('index_name', None)

        os.environ['OPENAI_API_KEY'] = openapi_key
        os.environ['PINECONE_API_KEY'] = vectorstore_api_key

        github_client = GithubClient(github_token, base_url, api_version, verbose)

        loader_args = {
            'github_client': github_client,
            'owner': owner,
            'repo': repo,
            'use_parser': use_parser,
            'verbose': verbose,
            'concurrent_requests': concurrent_requests
        }        
        if filter_directories is not None:
            filter_directories_filter = GithubRepositoryReader.FilterType.INCLUDE if filter_directories_filter == 'INCLUDE' else GithubRepositoryReader.FilterType.EXCLUDE
            loader_args['filter_directories'] = (filter_directories, filter_directories_filter)

        if filter_file_extensions is not None:
            filter_file_extensions_filter = GithubRepositoryReader.FilterType.INCLUDE if filter_file_extensions_filter == 'INCLUDE' else GithubRepositoryReader.FilterType.EXCLUDE
            loader_args['filter_file_extensions'] = (filter_file_extensions, filter_file_extensions_filter)

        loader = GithubRepositoryReader(**loader_args)

        if branch and commit_sha:
            logging.getLogger().error('branch and commit_sha are exclusive, use one or the other')
            ret = False
            return ret

        branch = branch if branch else None
        commit_sha = commit_sha if commit_sha else None

        documents = loader.load_langchain_documents(commit_sha=commit_sha, branch=branch)

        if documents.__len__() <= 0:
            logging.getLogger().error('No documents found')
            return ret
        
        lc_documents = split_documents(documents, chunk_size=chunk_size, chunk_overlap=chunk_overlap)

        docs_file = save_to_file(lc_documents, prefix='github')

        if saia_base_url is not None:

            ragApi = RagApi(saia_base_url, saia_api_token, saia_profile)

            target_file = f"{docs_file}.custom"
            shutil.copyfile(docs_file, target_file)

            response_body = ragApi.upload_document_with_metadata_file(target_file) # ToDo check .metadata
            if response_body is None:
                logging.getLogger().error("Error uploading document")
                return False
            
            if upload_operation_log:
                end_time = time.time()
                message_response = f"bulk ingest ({end_time - start_time:.2f}s)"
                ret = operation_log_upload(saia_base_url, saia_api_token, saia_profile, "ALL", message_response, 0)

        else:
            ## Fallback to directly ingest to vectorstore
            ingest(lc_documents, openapi_key, index_name, namespace, embeddings_model)

    except Exception as e:
        logging.getLogger().error(f"Error: {type(e)} {e}")
        ret = False
    finally:
        return ret

def saia_file_upload(
        saia_base_url: str,
        saia_api_token: str,
        saia_profile: str,
        file_item: str,
        use_metadata_file: bool = False,
        metadata_extension: str = '.json'
    ) -> bool:
    ret = True

    file = os.path.normpath(file_item)
    file_path = os.path.dirname(file)
    file_name = os.path.basename(file)

    metadata_file = get_metadata_file(file_path, file_name, metadata_extension) if use_metadata_file else None
    ret = file_upload(saia_base_url, saia_api_token, saia_profile, file, file_name, metadata_file, True)
    return ret

def ingest_s3(
        configuration: str,
        start_time: datetime,
        timestamp: datetime = None,
    ) -> bool:
    ret = True
    success_count = 0
    failed_count = 0
    try:
        config = get_yaml_config(configuration)
        s3_level = config.get('s3', {})
        embeddings_level = config.get('embeddings', {})
        url = s3_level.get('url', None)
        region = s3_level.get('region', None)
        bucket = s3_level.get('bucket', None)
        key = s3_level.get('key', None)
        keys_from_file = s3_level.get('keys_from_file', None)
        aws_access_key = s3_level.get('aws_access_key', None)
        aws_secret_key = s3_level.get('aws_secret_key', None)
        prefix = s3_level.get('prefix', None)
        embeddings_model = embeddings_level.get('model', 'text-embedding-ada-002')
        required_exts = s3_level.get('required_exts', None)
        excluded_exts = s3_level.get('excluded_exts', None)
        use_local_folder = s3_level.get('use_local_folder', False)
        local_folder = s3_level.get('local_folder', None)
        use_metadata_file = s3_level.get('use_metadata_file', False)
        use_augment_metadata = s3_level.get('use_augment_metadata', False)
        delete_local_folder = s3_level.get('delete_local_folder', False)
        process_files = s3_level.get('process_files', False)
        reprocess_failed_files = s3_level.get('reprocess_failed_files', False)
        reprocess_valid_status_list = s3_level.get('reprocess_valid_status_list', [])
        reprocess_status_detail_list_contains = s3_level.get('reprocess_status_detail_list_contains', [])
        alternative_document_service = s3_level.get('alternative_document_service', None)
        source_base_url = s3_level.get('source_base_url', None)
        source_doc_id = s3_level.get('source_doc_id', None)
        download_dir = s3_level.get('download_dir', None)
        verbose = s3_level.get('verbose', False)

        # Saia
        saia_level = config.get('saia', {})
        saia_base_url = saia_level.get('base_url', None)
        saia_api_token = saia_level.get('api_token', None)
        saia_profile = saia_level.get('profile', None)
        max_parallel_executions = saia_level.get('max_parallel_executions', 5)

        if saia_base_url is not None:
            ret = is_valid_profile(saia_base_url, saia_api_token, saia_profile)
            if ret is False:
                logging.getLogger().error(f"Invalid profile {saia_profile}")
                return ret

        # Default to ingest directly to index
        loader = S3Reader(
            s3_endpoint_url=url,
            region_name=region,
            bucket=bucket,
            key=key,
            keys_from_file=keys_from_file,
            prefix=prefix,
            aws_access_id=aws_access_key,
            aws_access_secret=aws_secret_key,
            timestamp=timestamp,
            required_exts=required_exts,
            excluded_exts=excluded_exts,
            use_local_folder=use_local_folder,
            local_folder=local_folder,
            use_metadata_file=use_metadata_file,
            use_augment_metadata=use_augment_metadata,
            process_files=process_files,
            max_parallel_executions=max_parallel_executions,
            source_base_url=source_base_url,
            source_doc_id=source_doc_id,
            alternative_document_service=alternative_document_service,
            download_dir=download_dir,
            verbose=verbose
            )
        loader.init_s3()
    
        if saia_base_url is not None:
            # Use Saia API to ingest

            ragApi = RagApi(saia_base_url, saia_api_token, saia_profile)

            if reprocess_failed_files:
                # Clean files with failed state, re upload
                file_reference = s3_level.get('reprocess_failed_files_reference', None)
                if file_reference is not None and os.path.exists(file_reference):
                    local_file = file_reference
                    docs = load_json_file(local_file)
                else:
                    docs = ragApi.get_profile_documents(saia_profile, skip=0, count=999999)

                reprocess_failed_files_exclude = s3_level.get('reprocess_failed_files_exclude', [])
                t_timestamp = timestamp
                min_filter_date = alternative_document_service.get('min_filter_date', None)
                if min_filter_date is not None:
                    t_timestamp = datetime.fromisoformat(min_filter_date).replace(tzinfo=timezone.utc)
                to_delete, file_paths = sync_failed_files(docs['documents'], local_folder, reprocess_valid_status_list, reprocess_status_detail_list_contains, reprocess_failed_files_exclude, t_timestamp)
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
                    futures = [executor.submit(file_delete, saia_base_url, saia_api_token, saia_profile, d) for d in to_delete]
                    concurrent.futures.wait(futures)
            else:
                file_paths = loader.get_files() if loader.alternative_document_service is None else loader.get_files_from_url()

                saia_file_ids_to_delete = search_failed_to_delete(file_paths)
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
                    futures = [executor.submit(ragApi.delete_profile_document, id, saia_profile) for id in saia_file_ids_to_delete]
                concurrent.futures.wait(futures)

            file_path = None

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
                futures = [executor.submit(saia_file_upload, saia_base_url, saia_api_token, saia_profile, file_item, use_metadata_file) for file_item in file_paths]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result is True:
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception as exc:
                        logging.getLogger().error(f"General exception: {exc}")
            
            if delete_local_folder and len(file_paths) > 0:
                file_path = os.path.dirname(file_paths[0])
                shutil.rmtree(file_path)

            logging.getLogger().info(f"Success: {success_count} Skip: {loader.skip_count}")
            logging.getLogger().info(f"Upload Failed: {failed_count} Download Failed: {loader.error_count}")
            logging.getLogger().info(f"Total: {loader.total_count}")

            upload_operation_log = saia_level.get('upload_operation_log', False)
            if upload_operation_log:
                end_time = time.time()
                message_response = f"bulk ingest ({end_time - start_time:.2f}s)"
                ret = operation_log_upload(saia_base_url, saia_api_token, saia_profile, "ALL", message_response, 0)

        else:

            ## Fallback to directly ingest to vectorstore
            documents = loader.load_langchain_documents()

            save_to_file(documents, prefix='s3')

            if verbose:
                for document in documents:
                    logging.getLogger().info(document.lc_id, document.metadata)

            embeddings_level = config.get('embeddings', {})
            openapi_key = embeddings_level.get('openapi_key', None)

            # Vectorstore
            vectorstore_level = config.get('vectorstore', {})
            api_key = vectorstore_level.get('api_key', None)
            index_name = vectorstore_level.get('index_name', None)
            namespace = vectorstore_level.get('namespace', None)

            doc_count = documents.__len__()
            if doc_count <= 0:
                return ret

            logging.getLogger().info(f"Vectorizing {doc_count} items to {index_name}/{namespace}")

            ret = ingest(documents, openapi_key, index_name, namespace, embeddings_model)

    except Exception as e:
        logging.getLogger().error(f"Error: {e}")
        ret = False
    finally:
        end_time = time.time()
        logging.getLogger().info(f"time: {end_time - start_time:.2f}s")
        return ret

def ingest_gdrive(
        configuration: str,
        timestamp: datetime = None,
    ) -> bool:
    ret = True
    try:
        start_time = time.time()

        config = get_yaml_config(configuration)
        gdrive_level = config.get('googledrive', {})
        folder_id = gdrive_level.get('folder_id', None)
        mime_types = gdrive_level.get('mime_types', None)
        cred = gdrive_level.get('credentials', None)
        delete_local_folder = gdrive_level.get('delete_local_folder', False)

        loader = GoogleDriveReader(credentials_path=cred)
        file_paths = loader.get_files(folder_id=folder_id, mime_types=mime_types)

        doc_count = len(file_paths)
        if doc_count <= 0:
            logging.getLogger().warn('No documents found')
            return ret

        path = os.path.dirname(file_paths[0])
        logging.getLogger().info(f"Downloaded {doc_count} files to {path}")

        # Saia
        saia_level = config.get('saia', {})
        saia_base_url = saia_level.get('base_url', None)
        saia_api_token = saia_level.get('api_token', None)
        saia_profile = saia_level.get('profile', None)
        max_parallel_executions = saia_level.get('max_parallel_executions', 5)
        upload_operation_log = saia_level.get('upload_operation_log', False)

        if saia_base_url is None:
            logging.getLogger().error(f"Missing '{Defaults.PACKAGE_DESCRIPTION}' configuration")
            logging.getLogger().error(f"Review configuration {Defaults.PACKAGE_URL}")
            ret = False
            return ret

        ret = is_valid_profile(saia_base_url, saia_api_token, saia_profile)
        use_metadata_file = False
        if ret is False:
            logging.getLogger().error(f"Invalid profile {saia_profile}")
            ret = False
            return ret

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_executions) as executor:
            futures = [executor.submit(saia_file_upload, saia_base_url, saia_api_token, saia_profile, file_item, use_metadata_file) for file_item in file_paths]
            concurrent.futures.wait(futures)
        
        if delete_local_folder and len(file_paths) > 0:
            file_path = os.path.dirname(file_paths[0])
            shutil.rmtree(file_path)

        if upload_operation_log:
            end_time = time.time()
            message_response = f"bulk ingest ({end_time - start_time:.2f}s)"
            ret = operation_log_upload(saia_base_url, saia_api_token, saia_profile, "ALL", message_response, 0)

        return ret

    except Exception as e:
        logging.getLogger().error(f"Error: {type(e)} {e}")
        ret = False
    finally:
        return ret

def ingest_sharepoint(
        configuration: str,
        start_time: datetime,
    ) -> bool:
    ingestor = Sharepoint_Ingestor(configuration, start_time)
    return ingestor.execute()
