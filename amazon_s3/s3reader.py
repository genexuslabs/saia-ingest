# Initial from https://github.com/run-llama/llama-hub/blob/main/llama_hub/s3/base.py
"""S3Reader class for reading from S3 buckets."""

import os
import logging
import tempfile
import boto3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import quote

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
        filename_as_id: bool = False,
        num_files_limit: Optional[int] = None,
        file_metadata: Optional[Callable[[str], Dict]] = None,
        aws_access_id: Optional[str] = None,
        aws_access_secret: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        s3_endpoint_url: Optional[str] = "https://s3.amazonaws.com",
        custom_reader_path: Optional[str] = None,
        timestamp: Optional[datetime] = None,
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

    def get_files(self) -> [str]:
        """Return a list of documents"""
        skip_count = 0
        count = 0

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

        file_paths = []

        if self.key:
            suffix = Path(self.key).suffix
            filepath = f"{temp_dir}/{next(tempfile._get_candidate_names())}{suffix}"
            s3_client.download_file(self.bucket, self.key, filepath)
            file_paths.append(filepath)
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
        return file_paths


    def load_data(self) -> List[Document]:
        """Load file(s) from S3."""
        
        file_paths = self.get_files()
        """
        skip_count = 0
        count = 0

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

        with tempfile.TemporaryDirectory() as temp_dir:
            logging.getLogger().info(f"Downloading files from '{self.bucket}' to {temp_dir}")

            file_paths = []

            if self.key:
                suffix = Path(self.key).suffix
                filepath = f"{temp_dir}/{next(tempfile._get_candidate_names())}{suffix}"
                s3_client.download_file(self.bucket, self.key, filepath)
                file_paths.append(filepath)
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

                    filepath = (
                        f"{temp_dir}/{temp_name}{suffix}"
                    )

                    if not os.path.exists(temp_dir):
                        os.makedirs(temp_dir)

                    original_key = obj.key
                    encoded_key = quote(original_key)

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
        """
            
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
