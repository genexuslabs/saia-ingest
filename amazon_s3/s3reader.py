# Initial from https://github.com/run-llama/llama-hub/blob/main/llama_hub/s3/base.py
"""S3Reader class for reading from S3 buckets."""

import os
import logging
import tempfile
import boto3
import json
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import quote, unquote
from saia_ingest.utils import detect_file_extension

from llama_index import download_loader
from llama_index.readers.base import BaseReader
from llama_index.readers.schema.base import Document


class S3Reader(BaseReader):
    """General reader for any S3 file or directory."""

    def __init__(
        self,
        *args: Any,
        region_name: Optional[str] = None,
        bucket: str,
        key: Optional[str] = None,
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
        """
        super().__init__(*args, **kwargs)

        self.bucket = bucket
        self.key = key
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

        self.s3 = None
        self.s3_client = None


    def get_versions(self, key) -> Any:
        response = self.s3_client.list_object_versions(Bucket=self.bucket, Prefix=key)
        versions = response.get('Versions', [])
        if versions:
            for version in versions:
                version_id = version['VersionId']
                is_current = version['IsLatest']
                last_modified = version['LastModified']
                print(f"Version ID: {version_id}, Is Current: {is_current}, Last Modified: {last_modified}")
        return versions


    def get_metadata(self, key) -> Any:
        """Get a File Metadata"""
        user_metadata = {}
        try:
            head_object_response = self.s3.meta.client.head_object(Bucket=self.bucket, Key=key)
            user_metadata = head_object_response.get('Metadata', user_metadata)
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
            with open(file_path, 'w') as file: # encoding='utf-8-sig'
                json.dump(data, file, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.getLogger().error(f"Error writing to {file_path}: {e}")


    def get_files(self) -> [str]:
        """Return a list of documents"""
        skip_count = 0
        count = 0
        file_paths = []

        if self.use_local_folder:

            if self.process_files:
                self.rename_files(self.local_folder, self.excluded_exts, None, '.json', self.prefix + '/', 'fileextension')

            for f in os.listdir(self.local_folder):
                f_extension = os.path.splitext(f)[1][1:]  # get extension without the leading dot
                if self.excluded_exts is not None and f_extension in self.excluded_exts:
                    continue
                if not os.path.isfile(os.path.join(self.local_folder, f)):
                    continue
                suffix = Path(f).suffix.lower().replace('.', '')
                if self.required_exts is not None and suffix not in self.required_exts:
                    continue
                file_paths.append(os.path.join(self.local_folder, f))

            return file_paths

        s3 = boto3.resource("s3")
        s3_client = boto3.client("s3")
        if self.aws_access_id:
            session = boto3.Session(
                region_name=self.region_name,                
                aws_access_key_id=self.aws_access_id,
                aws_secret_access_key=self.aws_access_secret,
                aws_session_token=self.aws_session_token,
            )
            s3 = session.resource("s3", region_name=self.region_name)
            s3_client = session.client("s3", region_name=self.region_name, endpoint_url=self.s3_endpoint_url)

        temp_dir = tempfile.mkdtemp()

        logging.getLogger().info(f"Downloading files from '{self.bucket}' to {temp_dir}")


        if self.key:
            suffix = Path(self.key).suffix
            filepath = f"{temp_dir}/{self.key}"
            original_key = f"{self.prefix}/{self.key}" if self.prefix else self.key
            s3.meta.client.download_file(self.bucket, original_key, filepath)
            file_paths.append(filepath)
            logging.getLogger().info(f" {original_key} to {self.key}")
        else:
            bucket = s3.Bucket(self.bucket)
            for i, obj in enumerate(bucket.objects.filter(Prefix=self.prefix)):
                if self.num_files_limit is not None and i > self.num_files_limit:
                    break

                suffix = Path(obj.key).suffix

                is_dir = obj.key.endswith("/")  # skip folders
                is_bad_ext = (
                    self.required_exts is not None
                    and suffix not in self.required_exts  # skip other extentions
                )

                if is_dir or is_bad_ext:
                    continue

                count += 1
                temp_name = next(tempfile._get_candidate_names())
                temp_name = obj.key.split("/")[-1]

                filepath = (
                    f"{temp_dir}/{temp_name}"
                )

                if not os.path.exists(temp_dir):
                    os.makedirs(temp_dir)

                original_key = obj.key

                skip_file = False
                if self.timestamp is not None and self.timestamp > obj.last_modified:
                    skip_file = True
                    skip_count += 1
                if skip_file:
                    continue

                try:
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html#S3.Client.download_file
                    s3.meta.client.download_file(self.bucket, original_key, filepath)
                    file_paths.append(filepath)
                    logging.getLogger().info(f" {obj.key} to {temp_name}")
                except Exception as e:
                    if e.response['Error']['Code'] == '404':
                        logging.getLogger().info(f"The object '{obj.key}' does not exist.")
                    elif e.response['Error']['Code'] == '403':
                        logging.getLogger().info(f"Forbidden access to '{obj.key}'")
                    else:
                        raise e
        
        logging.getLogger().info(f"Skipped: {skip_count} Total: {count}")

        if self.process_files:
            self.rename_files(temp_dir, self.excluded_exts, None, '.json', self.prefix + '/', 'fileextension')

        file_paths = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if os.path.isfile(os.path.join(temp_dir, f)) and not f.endswith('.json')]
        return file_paths


    def load_data(self) -> List[Document]:
        """Load file(s) from S3."""
        
        file_paths = self.get_files()            
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
            folder_path: str,
            excluded_exts: str,
            main_extension: str,
            metadata_extension: str,
            key_prefix: str,
            extension_tag: str = 'fileextension'
        ):
        '''Process all files in a folder, renaming them and adding metadata files'''
        if not os.path.exists(folder_path):
            logging.getLogger().warning(f"The folder '{folder_path}' does not exist.")
            return

        # Get a list of all files in the folder
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

        timestamp_tag = 'publishdate'
        # Process each file
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallel_executions) as executor:
            futures = [executor.submit(self.rename_file, folder_path, excluded_exts, main_extension, metadata_extension, key_prefix, file_item, timestamp_tag, extension_tag) for file_item in files]
            concurrent.futures.wait(futures)


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
        ):

        f_extension = os.path.splitext(file_name_with_extension)[1][1:]  # get extension without the leading dot
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
        if not os.path.isfile(metadata_file_path):
            # Get Metadata
            get_metadata = True

        rename_file = False
        if file_extension == '':
            get_metadata = True
            rename_file = True

        if get_metadata:
            # Get metadata and rename it
            s3_file = key_prefix + file_name
            initial_metadata = self.get_metadata(s3_file)
            if self.use_augment_metadata:
                user_metadata = self.augment_metadata(initial_metadata, timestamp_tag)
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
                    os.rename(file_path, new_path)
                except Exception as e:
                    logging.getLogger().error(f"Error renaming file '{file_name}' using extension '{str_extension}'")
                    return


    def augment_metadata(
            self,
            initial_metadata: dict,
            timestamp_tag: str = 'publishdate',
        ) -> dict:
        '''Preprocess and add metadata'''

        id = initial_metadata.get('documentid', '')
        name = initial_metadata.get('filename', id)
        activity = initial_metadata.get('disclosureactivity', '')
        date_string = initial_metadata.get(timestamp_tag, '')

        if date_string is not None:
            # Change from MM/DD/YYYY to YYYYMMDD format
            date_object = datetime.strptime(date_string, "%m/%d/%Y")
            formatted_date = date_object.strftime("%Y%m%d")
            year = date_object.strftime("%Y")
            # Add year
            initial_metadata[timestamp_tag] = formatted_date
            initial_metadata['year'] = year

        description = f"{name} | {date_string} | {activity}"

        initial_metadata['description'] = description

        return initial_metadata
