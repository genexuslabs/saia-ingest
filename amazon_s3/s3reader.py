# Initial from https://github.com/run-llama/llama-hub/blob/main/llama_hub/s3/base.py
"""S3Reader class for reading from S3 buckets."""

import os
import logging
import tempfile
import boto3
import json
import concurrent.futures
from collections import ChainMap, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import quote, unquote
from saia_ingest.config import Defaults
from saia_ingest.utils import detect_file_extension, do_get, parse_date
from saia_ingest.profile_utils import get_bearer_token, get_json_response_from_url
from saia_ingest.file_utils import calculate_file_hash

from llama_index import download_loader
from llama_index.readers.base import BaseReader
from llama_index.readers.schema.base import Document

from saia_ingest.vault.keyvault_client import KeyVaultClient

logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)

class S3Reader(BaseReader):
    """General reader for any S3 file or directory."""

    def __init__(
        self,
        *args: Any,
        region_name: Optional[str] = None,
        bucket: str,
        key: Optional[str] = None,
        keys_from_file: Optional[str] = None,
        prefix: Optional[str] = "",
        file_extractor: Optional[Dict[str, Union[str, BaseReader]]] = None,
        required_exts: Optional[List[str]] = None,
        excluded_exts: Optional[List[str]] = None,
        filename_as_id: bool = False,
        num_files_limit: Optional[int] = None,
        file_metadata: Optional[Callable[[str], Dict]] = None,
        aws_access_id: Optional[str] = None,
        aws_access_secret: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        s3_endpoint_url: Optional[str] = "https://s3.amazonaws.com",
        custom_reader_path: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        use_local_folder: Optional[bool] = False,
        local_folder: Optional[str] = None,
        use_metadata_file: Optional[bool] = False,
        use_augment_metadata: Optional[bool] = False,
        process_files: Optional[bool] = False,
        max_parallel_executions: Optional[int] = 10,
        verbose: Optional[bool] = False,
        download_dir: Optional[str] = None,
        source_base_url: Optional[str] = None,
        source_doc_id: Optional[str] = None,
        alternative_document_service: Optional[Dict[str, str]] = None,
        detect_file_duplication: Optional[bool] = False,
        skip_storage_download: Optional[bool] = False,
        download_using_prefix: Optional[bool] = False,
        reprocess_valid_status_list: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize S3 bucket and key, along with credentials if needed.

        If key is not set, the entire bucket (filtered by prefix) is parsed.

        Args:
        bucket (str): the name of your S3 bucket
        key (Optional[str]): the name of the specific file. If none is provided,
            this loader will iterate through the entire bucket.
        prefix (Optional[str]): the prefix to filter by in the case that the loader
            iterates through the entire bucket. Defaults to empty string.
        file_extractor (Optional[Dict[str, BaseReader]]): A mapping of file
            extension to a BaseReader class that specifies how to convert that file
            to text. See `SimpleDirectoryReader` for more details.
        required_exts (Optional[List[str]]): List of required extensions.
            Default is None.
        excluded_exts (Optional[List[str]]): List of excluded extensions.
            Default is None.
        num_files_limit (Optional[int]): Maximum number of files to read.
            Default is None.
        file_metadata (Optional[Callable[str, Dict]]): A function that takes
            in a filename and returns a Dict of metadata for the Document.
            Default is None.
        aws_access_id (Optional[str]): provide AWS access key directly.
        aws_access_secret (Optional[str]): provide AWS access key directly.
        s3_endpoint_url (Optional[str]): provide S3 endpoint URL directly.
        download_dir (Optional[str]): The directory where the files should be downloaded.
        """
        super().__init__(*args, **kwargs)

        self.bucket = bucket
        self.key = key
        self.keys_from_file = keys_from_file
        self.prefix = prefix

        self.file_extractor = file_extractor
        self.required_exts = required_exts
        self.excluded_exts = excluded_exts
        self.filename_as_id = filename_as_id
        self.num_files_limit = num_files_limit
        self.file_metadata = file_metadata
        self.custom_reader_path = custom_reader_path

        self.region_name = region_name
        self.aws_access_id = aws_access_id
        self.aws_access_secret = aws_access_secret
        self.aws_session_token = aws_session_token
        self.s3_endpoint_url = s3_endpoint_url

        self.timestamp = timestamp

        self.use_local_folder = use_local_folder
        self.process_files = process_files
        self.local_folder = local_folder
        self.use_metadata_file = use_metadata_file
        self.use_augment_metadata = use_augment_metadata
        self.max_parallel_executions = max_parallel_executions
        self.alternative_document_service = alternative_document_service
        self.source_base_url = source_base_url
        self.source_doc_id = source_doc_id
        self.detect_file_duplication = detect_file_duplication
        self.skip_storage_download = skip_storage_download
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.metadata_key_element = None
        self.description_template = None
        self.timestamp_tag = 'publishdate'
        self.extension_tag = 'fileextension'

        self.s3 = None
        self.s3_client = None

        self.verbose = verbose
        self.download_dir = download_dir if download_dir else tempfile.mkdtemp()
        self.download_using_prefix = download_using_prefix
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
        self.reprocess_valid_status_list = reprocess_valid_status_list
        
        self.json_extension = '.json'
        self.metadata_extension = '.metadata'
        self.bearer_token = None
        self.element_ids = set()
        self.element_dict = {}
        self.skip_dict = {}
        self.skip_count = 0
        self.error_count = 0
        self.error_dict = {}
        self.total_count = 0

        # Validations
        self.keys = None
        if self.keys_from_file is not None:
            with open(keys_from_file, 'r') as file:
                self.keys = json.load(file)


    def get_versions(self, key) -> Any:
        response = self.s3_client.list_object_versions(Bucket=self.bucket, Prefix=key)
        versions = response.get('Versions', [])
        if versions:
            for version in versions:
                version_id = version['VersionId']
                is_current = version['IsLatest']
                last_modified = version['LastModified']
                logging.getLogger().info(f"Version ID: {version_id}, Is Current: {is_current}, Last Modified: {last_modified}")
        return versions


    def get_metadata(self, key) -> Any:
        """Get a File Metadata"""
        user_metadata = {}
        try:
            head_object_response = self.s3.meta.client.head_object(Bucket=self.bucket, Key=key)
            user_metadata = head_object_response.get('Metadata', user_metadata)
            if self.metadata_early_merge is False:
                self.augment_metadata(key, user_metadata)
        except Exception as e:
            logging.getLogger().error(f"Error getting metadata for {key}: {e}")
        return user_metadata


    def init_s3(self, force=False) -> None:
        """Initialize S3 client"""
        if self.s3 is not None and not force:
            return
        self.s3 = boto3.resource("s3")
        self.s3_client = boto3.client("s3")
        if self.aws_access_id:
            self.session = boto3.Session(
                region_name=self.region_name,                
                aws_access_key_id=self.aws_access_id,
                aws_secret_access_key=self.aws_access_secret,
                aws_session_token=self.aws_session_token,
            )
            self.s3 = self.session.resource("s3", region_name=self.region_name)
            self.s3_client = self.session.client("s3", region_name=self.region_name, endpoint_url=self.s3_endpoint_url)


    def write_object_to_file(self, data, file_path):
        try:
            with open(file_path, 'w', encoding='utf8') as file:
                json.dump(data, file, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.getLogger().error(f"Error writing to {file_path}: {e}")

    def get_files_from_url(self) -> list[str]:
        """Return a list of documents from an alternative URL"""
        file_paths = []
        downloaded_files = []
        doc_nums = []
        prefix = ""

        bearer_params = self.alternative_document_service.get('bearer_token', None)
        if bearer_params is None:
            raise Exception("Missing 'bearer_token' in 'alternative_document_service' parameters")

        self.bearer_url = bearer_params.get('url', None)
        self.bearer_client_id = bearer_params.get('client_id', None)
        self.bearer_client_secret = bearer_params.get('client_secret', None)
        self.bearer_scope = bearer_params.get('scope', None)
        self.bearer_grant_type = bearer_params.get('grant_type', None)

        metadata_whitelist_items = self.alternative_document_service.get('metadata_whitelist_items', [])
        self.timestamp_tag = self.alternative_document_service.get('timestamp_tag', 'publishdate')
        self.extension_tag = self.alternative_document_service.get('extension_tag', 'fileextension')
        self.metadata_key_element = self.alternative_document_service.get('metadata_key_element', 'documentid')
        self.description_template = self.alternative_document_service.get('description_template', None)
        self.skip_storage_download = self.alternative_document_service.get('skip_storage_download', False)
        self.metadata_early_merge = self.alternative_document_service.get('metadata_early_merge', False)
        self.metadata_force_use_url = self.alternative_document_service.get('metadata_force_use_url', False)

        key_vault_params = self.alternative_document_service.get('key_vault', None)
        if key_vault_params is not None:
            key_vault_name = key_vault_params.get('name', None)
            key_vault_access_key = key_vault_params.get('access_key', None)
            key_vault_secret_key = key_vault_params.get('secret_key', None)
            key_vault_client_id = key_vault_params.get('client_id', None)
            key_vault_client_secret = key_vault_params.get('client_secret', None)
            tenant_id = key_vault_params.get('tenant_id', None)

            if key_vault_name is None:
                raise Exception("Missing 'name' in 'alternative_document_service/key_vault' parameters")
            if key_vault_access_key is None:
                raise Exception("Missing 'access_key' in 'alternative_document_service/key_vault' parameters")
            if key_vault_secret_key is None:
                raise Exception("Missing 'secret_key' in 'alternative_document_service/key_vault' parameters")
            if key_vault_client_id is None:
                raise Exception("Missing 'client_id' in 'alternative_document_service/key_vault' parameters")
            if key_vault_client_secret is None:
                raise Exception("Missing 'client_secret' in 'alternative_document_service/key_vault' parameters")
            if tenant_id is None:
                raise Exception("Missing 'tenant_id' in 'alternative_document_service/key_vault' parameters")
            
            key_vault_client = KeyVaultClient(
                vault_name=key_vault_name,
                client_id=key_vault_client_id,
                client_secret=key_vault_client_secret,
                tenant_id=tenant_id
            )
            self.aws_access_id = key_vault_client.get_secret(key_vault_access_key)
            self.aws_access_secret = key_vault_client.get_secret(key_vault_secret_key)
            self.init_s3(True)

        self.skip_existing_file = self.alternative_document_service.get('skip_existing_file', False)

        base_url = self.alternative_document_service.get('base_url', None)
        s_key = self.alternative_document_service.get('select_key', None)
        s_value = self.alternative_document_service.get('select_value', None)
        f_key = self.alternative_document_service.get('filter_key', None)
        o_key = self.alternative_document_service.get('order_key', None)
        o_value = self.alternative_document_service.get('order_value', None)
        i_key = self.alternative_document_service.get('start_index_key', None)
        i_value = self.alternative_document_service.get('start_index_value', None)
        c_key = self.alternative_document_service.get('count_key', None)
        c_value = self.alternative_document_service.get('count_value', None)

        f_values = self.alternative_document_service.get('filter_values', None)

        min_filter_date = self.alternative_document_service.get('min_filter_date', None)
        if min_filter_date is None:
            min_filter_date = self.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

        h_subscription_key = self.alternative_document_service.get('subscription_key', None)
        h_subscription_value = self.alternative_document_service.get('subscription_value', None)

        if h_subscription_key is None:
            raise Exception("Missing 'subscription_key' in 'alternative_document_service' parameters")
        if h_subscription_value is None:
            raise Exception("Missing 'subscription_value' in 'alternative_document_service' parameters")

        if f_values is None or f_values == []:
            raise Exception("Missing 'filter_values' items in 'alternative_document_service' parameters")

        if self.bearer_token is None:
            self.bearer_token = get_bearer_token(self)

        temp_dir = self.download_dir
        skip_count = 0
        logging.getLogger().info(f"Downloading files from '{self.bucket}' to {temp_dir} since {min_filter_date}")

        for f_item in f_values:
            f_item_name = f_item.get('name', None)
            f_item_where = f_item.get('where', None)
            if f_item_name is None or f_item_where is None:
                logging.getLogger().error(f"Missing 'name' or 'where' in 'alternative_document_service' parameters")
                continue
            # Update datetime filter
            f_value = f_item_where.replace('min_filter_date', min_filter_date)
            s_key_value = f"{s_key}={s_value}&" if s_value is not None else ""
            f_key_value = f"{f_key}={f_value}&" if f_value is not None else ""
            o_key_value = f"{o_key}={o_value}&" if o_value is not None else ""
            i_key_value = f"{i_key}={i_value}&" if i_value is not None else ""
            c_key_value = f"{c_key}={c_value}&" if c_value is not None else ""
            complete_url = f"{base_url}?{s_key_value}{f_key_value}{o_key_value}{i_key_value}{c_key_value}"

            while complete_url is not None:
                elements, next_url_href = get_json_response_from_url(complete_url, self, h_subscription_key, h_subscription_value)
                if elements is None or len(elements) == 0:
                    complete_url = None
                    continue

                if self.verbose:
                    logging.getLogger().info(f"{f_item_name} {len(elements)}")

                for item in elements:

                    if self.metadata_force_use_url is True:
                        if "url" not in item:
                            logging.getLogger().error(f"{item['document_id']} without url")
                            continue
                        elif item["url"] == None:
                            logging.getLogger().error(f"{item['document_id']} invalid url")
                            continue

                    self.total_count += 1

                    doc_num = item.get('docnum', None)
                    doc_name = item.get('docname', '')
                    file_type = item.get('filetype', 'None')
                    if doc_num is None:
                        doc_num = item.get('document_id', None)
                        doc_name =doc_num
                        _, extension = os.path.splitext(item.get('url', ''))
                        file_type = extension.lstrip('.')
                    if file_type is None:
                        file_type = self.get_file_extension(doc_name)

                    if not self.skip_storage_download and file_type is not None and not self.is_supported_extension(file_type.lower()):
                        skip_count += 1
                        self.skip_dict[doc_num] = item
                        logging.getLogger().warning(f"{doc_num} '{doc_name}' with '{file_type}' extension discarded")
                        continue
                    
                    original_key = f"{self.prefix}/{doc_num}" if self.prefix else doc_num

                    if self.skip_existing_file:
                        doc_num = item.get('document_id', None)
                        url = item.get('url', None)
                        if url is None:
                            logging.getLogger().error(f"{doc_num} invalid url")
                            continue
                        filename_with_extension = os.path.basename(url)
                        doc_name, file_extension = os.path.splitext(filename_with_extension)
                        file_extension = file_extension.lstrip(".")
                        file_type = self.get_file_extension(filename_with_extension)
                        extension = file_type if file_type is not None else self.get_file_extension(doc_name)
                        complete_file_path = f"{temp_dir}/{doc_num}.{extension}"
                        if os.path.exists(complete_file_path):
                            self.element_ids.add(doc_num)
                            doc_nums.append(doc_num)
                            self.element_dict[doc_num] = item
                            continue
                        if not self.is_supported_extension(file_type.lower()):
                            skip_count += 1
                            self.skip_dict[doc_num] = item
                            logging.getLogger().warning(f"{doc_num} '{doc_name}' with '{file_type}' extension discarded")
                            continue

                    if self.skip_storage_download:
                        doc_num = item.get('document_id', None)
                        url = item.get('url', None)
                        filename_with_extension = os.path.basename(url)
                        doc_name, file_extension = os.path.splitext(filename_with_extension)
                        file_extension = file_extension.lstrip(".")
                        file_type = self.get_file_extension(filename_with_extension)
                        original_key = f"{self.prefix}/{doc_num}" if self.prefix else doc_num
                        try:
                            response = do_get(None, url, self.headers)
                            if response.status_code == 200:
                                filename = f"{doc_num}.{file_type}"
                                complete_file_path = f"{temp_dir}/{filename}"
                                content = response.content
                                with open(complete_file_path, 'wb') as file:
                                    file.write(content)
                                downloaded_files.append(complete_file_path)
                                doc_nums.append(doc_num)
                                complete_metadata_file_path = f"{temp_dir}/{doc_num}{self.json_extension}"
                                filtered_metadata_item = self.get_metadata_whitelist_items(item, metadata_whitelist_items)
                                filtered_metadata_item.update({self.extension_tag: file_type})
                                self.write_object_to_file(filtered_metadata_item, complete_metadata_file_path)
                                if self.use_augment_metadata:
                                    self.element_ids.add(doc_num)
                                    self.element_dict[doc_num] = filtered_metadata_item
                                    user_metadata = self.augment_metadata(temp_dir, filename, filtered_metadata_item, self.timestamp_tag)
                                    if user_metadata:
                                        self.write_object_to_file(user_metadata, complete_metadata_file_path)
                            else:
                                logging.getLogger().error(f"Failed to download file. Status code: {response.status_code}")
                                continue
                        except Exception as e:
                            logging.getLogger().error(f"Error downloading {url} doc:'{doc_num}' name:'{doc_name}' error: {e}")
                            continue
                    else:
                        try:
                            prefix = f"{self.prefix}/{doc_num}" if self.prefix else doc_num
                            self.download_s3_file(prefix, temp_dir, downloaded_files)
                            if self.metadata_early_merge:
                                filename = f"{doc_num}.{file_type}"
                                filtered_metadata_item = self.get_metadata_whitelist_items(item, metadata_whitelist_items)
                                filtered_metadata_item.update({self.extension_tag: file_type})

                                s3_initial_metadata = self.get_metadata(prefix)
                                all_metadata = dict(ChainMap(s3_initial_metadata, filtered_metadata_item))
                                final_metadata = {k: v for k, v in all_metadata.items() if v not in [None, 'null', '']}

                                complete_metadata_file_path = f"{temp_dir}/{doc_num}{self.json_extension}"
                                self.write_object_to_file(final_metadata, complete_metadata_file_path)
                                if self.use_augment_metadata:
                                    user_metadata = self.augment_metadata(temp_dir, doc_num, final_metadata, self.timestamp_tag)
                                    if user_metadata:
                                        self.write_object_to_file(user_metadata, complete_metadata_file_path)
                            doc_nums.append(doc_num)
                        except Exception as e:
                            self.error_count += 1
                            self.error_dict[doc_num] = item
                            logging.getLogger().error(f"Error downloading {original_key} '{doc_name}' {e}")
                            continue

                    if not self.skip_storage_download:
                        # add item to be processed later
                        self.element_ids.add(doc_num)
                        self.element_dict[doc_num] = item

                    logging.getLogger().debug(f" {original_key} to {doc_num}")

                complete_url = f"{base_url}{next_url_href}" if next_url_href is not None else None

        self.skip_count = skip_count
        if len(self.element_ids) <= 0:
            return []
        if self.verbose:
            _ = self.save_debug(self.element_dict, prefix='denodo')

        if self.process_files:
            _ = self.rename_files(downloaded_files, temp_dir, self.excluded_exts, None, self.json_extension, prefix)

        if self.element_ids is not None and len(self.element_ids) > 0:
            file_paths = []
            for f in os.listdir(temp_dir):
                full_path = os.path.join(temp_dir, f)
                if os.path.isfile(full_path):
                    file_name_without_ext = os.path.splitext(f)[0]
                    if (not f.endswith((self.json_extension, self.metadata_extension)) and 
                            file_name_without_ext in doc_nums):
                        file_paths.append(full_path)
        else:
            file_paths = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if os.path.isfile(os.path.join(temp_dir, f)) and not f.endswith((self.json_extension, self.metadata_extension))]
        return file_paths

    def get_metadata_whitelist_items(self, initial: dict, metadata_whitelist: list[str]) -> dict:
        """
        Filters the given dictionary, keeping only keys that exist in metadata_whitelist.
        
        :param initial: Dictionary to be filtered.
        :return: A new dictionary containing only the whitelisted keys.
        """
        return {k: v for k, v in initial.items() if k in metadata_whitelist}

    def save_debug(self, serialized_docs: any, prefix:str) -> str:
        try:
            debug_folder = os.path.join(os.getcwd(), 'debug')
            now = datetime.now()
            formatted_timestamp = now.strftime("%Y%m%d%H%M%S")
            filename = '%s_%s.json' % (prefix, formatted_timestamp)
            file_path = os.path.join(debug_folder, filename)
            with open(file_path, 'w', encoding='utf8') as json_file:
                json.dump(serialized_docs, json_file, ensure_ascii=False, indent=4)
            return file_path
        except Exception as e:
            logging.getLogger().error(f"Error saving debug file: {e}")

    def get_file_extension(self, name) -> str:
        '''get extension without the leading dot'''
        return os.path.splitext(name)[1][1:]

    def get_files(self) -> list[str]:
        """Return a list of documents"""
        skip_count = 0
        count = 0
        file_paths = []

        if self.use_local_folder:

            if self.process_files:
                _ = self.rename_files(None, self.local_folder, self.excluded_exts, None, self.json_extension, self.prefix + '/')

            if self.keys is not None:
                for key in self.keys:
                    file_name = os.path.basename(key)
                    file_path = os.path.join(self.local_folder, file_name)
                    if os.path.isfile(file_path):
                        metadata_file = f"{file_path}{Defaults.PACKAGE_METADATA_POSTFIX}"
                        process_file = True
                        if os.path.isfile(metadata_file):
                            process_file = False
                            with open(metadata_file, 'r', encoding='utf-8') as file:
                                metadata_file = json.load(file)
                            upsert_status = metadata_file.get('indexStatus', None)
                            if upsert_status is not None and upsert_status in self.reprocess_valid_status_list:
                                process_file = True
                        if process_file:
                            file_paths.append(file_path)
            else:
                for f in os.listdir(self.local_folder):
                    f_extension = self.get_file_extension(f)
                    if self.excluded_exts is not None and f_extension in self.excluded_exts:
                        continue
                    if not os.path.isfile(os.path.join(self.local_folder, f)):
                        continue
                    suffix = Path(f).suffix.lower().replace('.', '')
                    if not self.is_supported_extension(suffix):
                        continue
                    file_paths.append(os.path.join(self.local_folder, f))

            self.total_count = len(file_paths)
            return file_paths

        temp_dir = self.download_dir

        logging.getLogger().info(f"Downloading files from '{self.bucket}' to {temp_dir}")

        if self.key:
            logging.getLogger().info(f"key: '{self.key}'")
            original_key = f"{self.prefix}/{self.key}" if self.prefix else self.key
            self.download_s3_file(original_key, temp_dir, file_paths)
            count = 1
        elif self.keys:
            logging.getLogger().info(f"keys: '{len(self.keys)}'")
            for key in self.keys:
                original_key = f"{self.prefix}/{key}" if self.prefix else key
                self.download_s3_file(original_key, temp_dir, file_paths)
            count = len(self.keys)
        else:
            bucket = self.s3.Bucket(self.bucket)
            for i, obj in enumerate(bucket.objects.filter(Prefix=self.prefix)):
                if self.num_files_limit is not None and i > self.num_files_limit:
                    break

                suffix = Path(obj.key).suffix

                is_dir = obj.key.endswith("/")  # skip folders
                is_bad_ext = not self.is_supported_extension(suffix)

                if is_dir or is_bad_ext:
                    continue

                temp_name = next(tempfile._get_candidate_names())
                temp_name = obj.key.split("/")[-1]

                if not os.path.exists(temp_dir):
                    os.makedirs(temp_dir)

                original_key = obj.key

                skip_file = False
                if self.timestamp is not None and self.timestamp > obj.last_modified:
                    skip_file = True
                    skip_count += 1
                if skip_file:
                    continue

                count += 1
                try:
                    self.download_s3_file(original_key, temp_dir, file_paths)
                except Exception as e:
                    self.error_count += 1
                    self.error_dict[temp_name] = obj
                    if e.response['Error']['Code'] == '404':
                        logging.getLogger().error(f"The object '{obj.key}' does not exist.")
                    elif e.response['Error']['Code'] == '403':
                        logging.getLogger().error(f"Forbidden access to '{obj.key}'")
                    else:
                        raise e
        
        self.total_count = count
        self.skip_count = skip_count

        if self.process_files:
            renamed_files = self.rename_files(file_paths, temp_dir, self.excluded_exts, None, self.json_extension, self.prefix + '/')

        if file_paths is None:
            file_paths = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if os.path.isfile(os.path.join(temp_dir, f)) and not f.endswith((self.json_extension, self.metadata_extension))]
        else:
            file_paths = renamed_files
        return file_paths


    def download_s3_file(self, prefix_key: str, temp_dir: str, file_paths: list):
        """Download a single file"""
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html#S3.Client.download_file
        key = prefix_key
        original_key = key
        bucket = self.bucket
        if self.prefix and self.download_using_prefix is False:
            key = key.replace(f"{self.prefix}/", "")
        filepath = f"{temp_dir}/{key}"
        folder_path = os.path.dirname(filepath)
        os.makedirs(folder_path, exist_ok=True)

        try:
            self.s3.meta.client.download_file(bucket, original_key, filepath)
            file_paths.append(filepath)
            logging.getLogger().debug(f" {original_key} to {key}")
        except Exception as e:
            self.error_count += 1
            self.error_dict[key] = key
            logging.getLogger().error(f"Error downloading {original_key} {e}")

    def is_supported_extension(self, suffix: str) -> bool:
        """Discard extension not listed in required_exts"""
        if self.required_exts is None or suffix == '':
            return True
        return suffix in self.required_exts


    def load_data(self) -> List[Document]:
        """Load file(s) from S3."""
        
        file_paths = self.get_files() if self.alternative_document_service is None else self.get_files_from_url()
        temp_dir = os.path.dirname(file_paths[0]) if len(file_paths) > 0 else None
        try:
            from llama_index import SimpleDirectoryReader
        except ImportError:
            custom_reader_path = self.custom_reader_path

            if custom_reader_path is not None:
                SimpleDirectoryReader = download_loader(
                    "SimpleDirectoryReader", custom_path=custom_reader_path
                )
            else:
                SimpleDirectoryReader = download_loader("SimpleDirectoryReader")

        loader = SimpleDirectoryReader(
            temp_dir,
            file_extractor=self.file_extractor,
            required_exts=self.required_exts,
            filename_as_id=self.filename_as_id,
            num_files_limit=self.num_files_limit,
            file_metadata=self.file_metadata,
        )

        documents = loader.load_data()
        return documents

    def rename_files(
            self,
            file_list: List[str],
            folder_path: str,
            excluded_exts: str,
            main_extension: str,
            metadata_extension: str,
            key_prefix: str,
        ) -> list[str]:
        '''Process all files in a folder, renaming them and adding metadata files'''
        if not os.path.exists(folder_path):
            logging.getLogger().warning(f"The folder '{folder_path}' does not exist.")
            return

        if file_list is None or len(file_list) == 0:
            # Get a list of all files in the folder
            files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        else:
            files = []
            for file_item in file_list:
                if os.path.isfile(file_item):
                    if not file_item.endswith((self.json_extension, self.metadata_extension)):
                        file_name = os.path.splitext(os.path.basename(file_item))[0]
                        files.append(file_name)

        renamed_files = []
        # Process each file
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallel_executions) as executor:
            futures = [executor.submit(self.rename_file, folder_path, excluded_exts, main_extension, metadata_extension, key_prefix, file_item, self.timestamp_tag, self.extension_tag) for file_item in files]
            for future in concurrent.futures.as_completed(futures):
                try:
                    file_path = future.result()
                    renamed_files.append(file_path)
                except Exception as exc:
                    logging.getLogger().error(f"General exception: {exc}")
        return renamed_files


    def rename_file(
            self,
            folder_path: str,
            excluded_extensions: List[str],
            main_extension: str,
            metadata_extension: str,
            key_prefix: str,
            file_name_with_extension: str,
            timestamp_tag: str = 'publishdate',
            extension_tag: str = 'fileextension'
        ) -> str:

        complete_path = ""
        f_extension = self.get_file_extension(file_name_with_extension)
        if self.excluded_exts is not None and f_extension in self.excluded_exts:
            return

        if main_extension is not None:
            if not file_name_with_extension.lower().endswith(main_extension):
                return

        file_name, file_extension = os.path.splitext(file_name_with_extension)

        metadata_file_name = file_name + metadata_extension

        extension_from_metadata = None

        get_metadata = False
        metadata_file_path = os.path.join(folder_path, metadata_file_name)
        if self.use_metadata_file and not os.path.isfile(metadata_file_path):
            # Get Metadata
            get_metadata = True

        rename_file = False
        if self.use_metadata_file and file_extension == '':
            if self.metadata_early_merge is False:
                get_metadata = True
            rename_file = True

        if get_metadata:
            # Get metadata and rename it
            if self.download_using_prefix is False:
                s3_file = key_prefix
                if "/" in key_prefix:
                    # Substitute the last part of the key_prefix with the file name
                    file_from_pattern = key_prefix.rsplit('/', 1)[1]
                    s3_file = key_prefix.replace(file_from_pattern, file_name)
            else:
                s3_file = key_prefix + file_name
            initial_metadata = self.get_metadata(s3_file)
            if self.use_augment_metadata:
                user_metadata = self.augment_metadata(folder_path, file_name, initial_metadata, timestamp_tag)
            extension_from_metadata = user_metadata.get(extension_tag, None)
            if user_metadata:
                self.write_object_to_file(user_metadata, metadata_file_path)

        if rename_file:

            if file_extension is None or file_extension == '':
                try:
                    file_path = os.path.join(folder_path, file_name_with_extension)
                    if extension_from_metadata is None:
                        extension_from_metadata = detect_file_extension(file_path)
                        logging.getLogger().warning(f"File '{file_name_with_extension}' without extension, detected {extension_from_metadata}")
                        new_file_name = file_name + extension_from_metadata
                    else:
                        str_extension = str(extension_from_metadata)
                        new_file_name = file_name + '.' + str_extension

                    new_path = os.path.join(folder_path, new_file_name)
                    # Rename the file
                    if os.path.exists(new_path):
                        os.remove(new_path)
                    os.rename(file_path, new_path)
                    complete_path = new_path
                except Exception as e:
                    logging.getLogger().error(f"Error renaming file '{file_name}' using extension '{extension_from_metadata}' {e}")
                    return ""
        return complete_path


    def update_with_mapping(self, original_dict, mapping):
        for old_key, new_value in mapping.items():
            if old_key in original_dict:
                original_dict[new_value] = original_dict.pop(old_key)  # Replace old key with new key
        return original_dict


    def augment_metadata(
            self,
            folder_path: str,
            document_name: str,
            input_metadata: dict,
            timestamp_tag: str = 'publishdate',
        ) -> dict:
        '''Preprocess and add metadata'''

        # Remove entries where the value is not desired
        initial_metadata = {k: v for k, v in input_metadata.items() if v not in [None, 'null', '']}
        try:
            id = initial_metadata.get(self.metadata_key_element, '')
            date_string = initial_metadata.get(timestamp_tag, '')
            date_string_description = date_string
            doc_url = None

            merge_metadata_elements = self.alternative_document_service.get('merge_metadata', False)
            item_metadata_from_service = self.element_dict.get(id, None)

            if item_metadata_from_service is not None and merge_metadata_elements:
                external_metadata = {k: v for k, v in item_metadata_from_service.items() if v not in [None, 'null', '']}
                # Combine with preference to the first dictionary
                initial_metadata = dict(ChainMap(initial_metadata, external_metadata))

                mapping = self.alternative_document_service.get('metadata_mappings', {})
                if mapping:
                    initial_metadata = self.update_with_mapping(initial_metadata, mapping)
                date_string = initial_metadata.get(timestamp_tag, date_string)
                doc_url = initial_metadata.get('docurl', None)

            if date_string is not None:
                try:
                    formatted_date, day, month, year = parse_date(date_string)
                    if year is not None:
                        formatted_date = date_string
                        date_string_description = f"{month}/{day}/{year}"
                        initial_metadata['date_description'] = date_string_description
                    if formatted_date is not None:
                        initial_metadata[timestamp_tag] = formatted_date
                        initial_metadata['year'] = year
                except ValueError as e:
                    logging.getLogger().error(f"{e}")

            if self.detect_file_duplication:
                file_path = f"{folder_path}/{document_name}"
                file_hash = calculate_file_hash(file_path)
                initial_metadata[Defaults.FILE_HASH] = file_hash

            if self.source_base_url is not None and self.source_doc_id is not None:
                if doc_url is not None:
                    initial_metadata['url'] = doc_url
                else:
                    # Update URL
                    source_url = f"{self.source_base_url}?{self.source_doc_id}={id}&CONTDISP=INLINE"
                    initial_metadata['url'] = source_url

            url = initial_metadata.get('url', None)
            if url:
                replacements = self.alternative_document_service.get('url_replacements', {})
                for replacement in replacements:
                    old_str = replacement['search']
                    new_str = replacement['replace']
                    if old_str in url:
                        url = url.replace(old_str, new_str)
                        initial_metadata['url'] = url
            self.generate_description(initial_metadata, date_string_description, id)
        except Exception as e:
            logging.getLogger().error(f"Error augmenting metadata for '{document_name}' from {initial_metadata} Error: {e}")

        return initial_metadata

    def generate_description(self, initial_metadata, date_string_description, id:any):
        try:
            default_values = defaultdict(lambda: "", initial_metadata)
            if not self.description_template is None:
                description = self.description_template.format(**default_values)
            else:
                name = initial_metadata.get('filename', id)
                activity = initial_metadata.get('disclosureactivity', '')
                description = f"{name} | {date_string_description} | {activity}"
            initial_metadata['description'] = description
        except Exception as e:
            logging.getLogger().error(f"Error generating description from {initial_metadata} Error: {e}")
