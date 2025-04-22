"""
Google Drive files reader.
Initial https://github.com/run-llama/llama_index/tree/main/llama-index-integrations/readers/llama-index-readers-google/llama_index/readers/google/drive
"""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from pydantic import (
    PrivateAttr,
)

logger = logging.getLogger(__name__)

# Scope for reading and downloading google drive files
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class GoogleDriveReader():
    """Google Drive Reader.

    Reads files from Google Drive. Credentials passed directly to the constructor
    will take precedence over those passed as file paths.

    Args:
        drive_id (Optional[str]): Drive id of the shared drive in google drive.
        folder_id (Optional[str]): Folder id of the folder in google drive.
        file_ids (Optional[str]): File ids of the files in google drive.
        query_string: A more generic query string to filter the documents, e.g. "name contains 'test'".
            It gives more flexibility to filter the documents. More info: https://developers.google.com/drive/api/v3/search-files
        is_cloud (Optional[bool]): Whether the reader is being used in
            a cloud environment. Will not save credentials to disk if so.
            Defaults to False.
        credentials_path (Optional[str]): Path to client config file.
            Defaults to None.
        token_path (Optional[str]): Path to authorized user info file. Defaults
            to None.
        service_account_key_path (Optional[str]): Path to service account key
            file. Defaults to None.
        client_config (Optional[dict]): Dictionary containing client config.
            Defaults to None.
        authorized_user_info (Optional[dict]): Dictionary containing authorized
            user info. Defaults to None.
        service_account_key (Optional[dict]): Dictionary containing service
            account key. Defaults to None.
        download_dir (Optional[str]): Directory to download files to.
    """

    drive_id: Optional[str] = None
    folder_id: Optional[str] = None
    file_ids: Optional[List[str]] = None
    query_string: Optional[str] = None
    client_config: Optional[dict] = None
    authorized_user_info: Optional[dict] = None
    service_account_key: Optional[dict] = None
    token_path: Optional[str] = None
    download_dir: Optional[str]

    _is_cloud: bool = PrivateAttr(default=False)
    _creds: Credentials = PrivateAttr()
    _mimetypes: dict = PrivateAttr()

    def __init__(
        self,
        drive_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        query_string: Optional[str] = None,
        is_cloud: Optional[bool] = False,
        credentials_path: str = "credentials.json",
        token_path: str = "token.json",
        service_account_key_path: str = "service_account_key.json",
        client_config: Optional[dict] = None,
        authorized_user_info: Optional[dict] = None,
        service_account_key: Optional[dict] = None,
        download_dir: Optional[str] = None,
        use_metadata_file: Optional[bool] = False,
        timestamp: Optional[datetime] = None,
        exclude_ids: Optional[List] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize with parameters."""
        # Read the file contents so they can be serialized and stored.
        if client_config is None and credentials_path and os.path.isfile(credentials_path):
            with open(credentials_path, encoding="utf-8") as json_file:
                client_config = json.load(json_file)

        if authorized_user_info is None and token_path and os.path.isfile(token_path):
            with open(token_path, encoding="utf-8") as json_file:
                authorized_user_info = json.load(json_file)

        if service_account_key is None and service_account_key_path and os.path.isfile(service_account_key_path):
            with open(service_account_key_path, encoding="utf-8") as json_file:
                service_account_key = json.load(json_file)

        if (
            client_config is None
            and service_account_key is None
            and authorized_user_info is None
        ):
            raise ValueError(
                "Must specify `client_config` or `service_account_key` or `authorized_user_info`."
            )

        self.verbose = kwargs.get("verbose", False)
        self.drive_id=drive_id
        self.folder_id=folder_id
        self.file_ids=file_ids
        self.query_string=query_string
        self.client_config=client_config
        self.authorized_user_info=authorized_user_info
        self.service_account_key=service_account_key
        self.token_path=token_path or "token.json"
        self.download_dir = download_dir if download_dir else tempfile.TemporaryDirectory()
        self.use_metadata_file = use_metadata_file
        self.timestamp = timestamp
        self.exclude_ids = exclude_ids

        self.json_extension = '.json'
        self.metadata_extension = '.metadata'

        self._creds = None
        self._is_cloud = is_cloud
        # Download Google Docs/Slides/Sheets as actual files
        # See https://developers.google.com/drive/v3/web/mime-types
        self._mimetypes = {
            "application/vnd.google-apps.document": {
                "mimetype": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "extension": ".docx",
            },
            "application/vnd.google-apps.spreadsheet": {
                "mimetype": (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
                "extension": ".xlsx",
            },
            "application/vnd.google-apps.presentation": {
                "mimetype": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                "extension": ".pptx",
            },
            #"application/pdf": {
            #    "mimetype": "application/pdf",
            #    "extension": ".pdf",
            #},
        }

        try:
            if timestamp is not None:
                self.timestamp = timestamp
        except Exception as e:
            logger.error(f"Invalid timestamp format: {e}")

    @classmethod
    def class_name(cls) -> str:
        return "GoogleDriveReader"

    def _get_credentials(self) -> Tuple[Credentials]:
        """Authenticate with Google and save credentials.
        Download the service_account_key.json file with these instructions: https://cloud.google.com/iam/docs/keys-create-delete.

        IMPORTANT: Make sure to share the folders / files with the service account. Otherwise it will fail to read the docs

        Returns:
            credentials
        """
        # First, we need the Google API credentials for the app
        creds = None

        if self.authorized_user_info is not None:
            creds = Credentials.from_authorized_user_info(
                self.authorized_user_info, SCOPES
            )
        elif self.service_account_key is not None:
            return service_account.Credentials.from_service_account_info(
                self.service_account_key, scopes=SCOPES
            )

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_config(self.client_config, SCOPES)
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            if not self._is_cloud:
                with open(self.token_path, "w", encoding="utf-8") as token:
                    token.write(creds.to_json())

        return creds

    def _get_drive_link(self, file_id: str) -> str:
        return f"https://drive.google.com/file/d/{file_id}/view"

    def _get_relative_path(
        self, service, file_id: str, root_folder_id: Optional[str] = None
    ) -> str:
        """Get the relative path from root_folder_id to file_id."""
        try:
            # Get file details including parents
            file = (
                service.files()
                .get(fileId=file_id, supportsAllDrives=True, fields="name, parents")
                .execute()
            )

            path_parts = [file["name"]]

            if not root_folder_id:
                return file["name"]

            # Traverse up through parents until we reach root_folder_id or can't access anymore
            try:
                current_parent = file.get("parents", [None])[0]
                while current_parent:
                    # If we reach the root folder, stop
                    if current_parent == root_folder_id:
                        break

                    try:
                        parent = (
                            service.files()
                            .get(
                                fileId=current_parent,
                                supportsAllDrives=True,
                                fields="name, parents",
                            )
                            .execute()
                        )
                        path_parts.insert(0, parent["name"])
                        current_parent = parent.get("parents", [None])[0]
                    except Exception as e:
                        logger.error(f"Stopped at parent {current_parent}: {e!s}")
                        break

            except Exception as e:
                logger.error(f"Could not access parents for {file_id}: {e!s}")

            return "/".join(path_parts)

        except Exception as e:
            logger.warning(f"Could not get path for file {file_id}: {e}")
            return file["name"]

    def _get_fileids_meta(
        self,
        drive_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        file_id: Optional[str] = None,
        mime_types: Optional[List[str]] = None,
        query_string: Optional[str] = None,
        current_path: Optional[str] = None,
    ) -> List[List[str]]:
        """Get file ids present in folder/ file id
        Args:
            drive_id: Drive id of the shared drive in google drive.
            folder_id: folder id of the folder in google drive.
            file_id: file id of the file in google drive
            mime_types: The mimeTypes you want to allow e.g.: "application/vnd.google-apps.document"
            query_string: A more generic query string to filter the documents, e.g. "name contains 'test'".

        Returns:
            metadata: List of metadata of file ids.
        """
        from googleapiclient.discovery import build

        try:
            service = build("drive", "v3", credentials=self._creds, cache_discovery=False)
            fileids_meta = []

            if folder_id and not file_id:
                try:
                    folder = (
                        service.files()
                        .get(fileId=folder_id, supportsAllDrives=True, fields="name")
                        .execute()
                    )
                    current_path = (
                        f"{current_path}/{folder['name']}"
                        if current_path
                        else folder["name"]
                    )
                except Exception as e:
                    logger.warning(f"Could not get folder name: {e}")

                folder_mime_type = "application/vnd.google-apps.folder"
                query = "('" + folder_id + "' in parents)"

                # Add mimeType filter to query
                if mime_types:
                    if folder_mime_type not in mime_types:
                        mime_types.append(folder_mime_type)  # keep the recursiveness
                    mime_query = " or ".join(
                        [f"mimeType='{mime_type}'" for mime_type in mime_types]
                    )
                    query += f" and ({mime_query})"

                # Add query string filter
                if query_string:
                    query += (
                        f" and ({query_string})"
                    )

                items = []
                page_token = None
                # get files taking into account that the results are paginated
                while True:
                    if drive_id:
                        results = (
                            service.files()
                            .list(
                                q=query,
                                driveId=drive_id,
                                corpora="drive",
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                fields="*",
                                pageToken=page_token,
                            )
                            .execute()
                        )
                    else:
                        results = (
                            service.files()
                            .list(
                                q=query,
                                includeItemsFromAllDrives=True,
                                supportsAllDrives=True,
                                fields="*",
                                pageToken=page_token,
                            )
                            .execute()
                        )
                    items.extend(results.get("files", []))
                    page_token = results.get("nextPageToken", None)
                    if page_token is None:
                        break

                for item in items:
                    item_path = (
                        f"{current_path}/{item['name']}"
                        if current_path
                        else item["name"]
                    )
                    item_id = item["id"]

                    if item["mimeType"] == folder_mime_type:
                        if item_id in self.exclude_ids: # Exclude the folder if it is in the exclude_ids
                            continue
                        if drive_id:
                            fileids_meta.extend(
                                self._get_fileids_meta(
                                    drive_id=drive_id,
                                    folder_id=item_id,
                                    mime_types=mime_types,
                                    query_string=query_string,
                                    current_path=current_path,
                                )
                            )
                        else:
                            fileids_meta.extend(
                                self._get_fileids_meta(
                                    folder_id=item_id,
                                    mime_types=mime_types,
                                    query_string=query_string,
                                    current_path=current_path,
                                )
                            )
                    else:
                        # Check if file doesn't belong to a Shared Drive. "owners" doesn't exist in a Shared Drive
                        is_shared_drive = "driveId" in item
                        author = (
                            item["owners"][0]["displayName"]
                            if not is_shared_drive
                            else "Shared Drive"
                        )
                        if not self.is_greater_than_timestamp(item["modifiedTime"]):
                            continue
                        if self.exclude_ids and item_id in self.exclude_ids:
                            continue
                        fileids_meta.append(
                            (
                                item_id,
                                author,
                                item_path,
                                item["mimeType"],
                                item["createdTime"],
                                item["modifiedTime"],
                                self._get_drive_link(item_id),
                            )
                        )
                        self.save_metadata(item, author, item_path)
            else:
                # Get the file details
                file = (
                    service.files()
                    .get(fileId=file_id, supportsAllDrives=True, fields="*")
                    .execute()
                )
                # Get metadata of the file
                is_shared_drive = "driveId" in file
                author = (
                    file["owners"][0]["displayName"]
                    if not is_shared_drive
                    else "Shared Drive"
                )

                # Get the full file path
                file_path = self._get_relative_path(
                    service, file_id, folder_id or self.folder_id
                )
                file_id = file["id"]
                if self.is_greater_than_timestamp(file["modifiedTime"])\
                    and file_id not in self.exclude_ids:

                    fileids_meta.append(
                        (
                            file_id,
                            author,
                            file_path,
                            file["mimeType"],
                            file["createdTime"],
                            file["modifiedTime"],
                            self._get_drive_link(file_id),
                        )
                    )
                    self.save_metadata(file, author, file_path)
            return fileids_meta

        except Exception as e:
            logger.error(
                f"An error occurred while getting fileids metadata: {e}", exc_info=True
            )

    def _download_file(self, fileid: str, filename: str) -> str:
        """Download the file with fileid and filename
        Args:
            fileid: file id of the file in google drive
            filename: filename with which it will be downloaded
        Returns:
            The downloaded filename, which which may have a new extension.
        """
        from io import BytesIO

        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload

        try:
            # Get file details
            service = build("drive", "v3", credentials=self._creds, cache_discovery=False)
            file = service.files().get(fileId=fileid, supportsAllDrives=True).execute()

            if file["mimeType"] in self._mimetypes:
                download_mimetype = self._mimetypes[file["mimeType"]]["mimetype"]
                download_extension = self._mimetypes[file["mimeType"]]["extension"]
                new_file_name = filename + download_extension

                if file["mimeType"] == "application/vnd.google-apps.presentation":
                    # Google Slides files are exported as PDF, some files get the error
                    # This file is too large to be exported.
                    # 'reason': 'exportSizeLimitExceeded
                    download_mimetype = 'application/pdf'
                    download_extension = '.pdf'
                    new_file_name = filename + download_extension

                # Download and convert file
                request = service.files().export_media(
                    fileId=fileid, mimeType=download_mimetype
                )
            else:
                # we should have a file extension to allow the readers to work
                _, download_extension = os.path.splitext(file.get("name", ""))
                new_file_name = filename + download_extension
                # Download file without conversion
                request = service.files().get_media(fileId=fileid)

            # Download file data
            file_data = BytesIO()
            downloader = MediaIoBaseDownload(file_data, request)
            done = False

            while not done:
                status, done = downloader.next_chunk()

            # Save the downloaded file
            with open(new_file_name, "wb") as f:
                f.write(file_data.getvalue())

            return new_file_name
        except Exception as e:
            logger.error(
                f"An error occurred while downloading {fileid} file: {e}", exc_info=True
            )

    def _load_data_fileids_meta(self, fileids_meta: List[List[str]]) -> any:
        """Load data from fileids metadata
        Args:
            fileids_meta: metadata of fileids in google drive.

        Returns:
            Lis[Document]: List of Document of data present in fileids.
        """
        file_paths = []
        try:
            if self.download_dir:
                temp_dir = self.download_dir
                def get_metadata(filename):
                    return metadata[filename]

                temp_dir = Path(temp_dir)
                metadata = {}

                for fileid_meta in fileids_meta:
                    # Download files and name them with their fileid
                    fileid = fileid_meta[0]
                    filepath = os.path.join(temp_dir, fileid)
                    final_filepath = self._download_file(fileid, filepath)

                    # Add metadata of the file to metadata dictionary
                    metadata[final_filepath] = {
                        "file id": fileid_meta[0],
                        "author": fileid_meta[1],
                        "file path": fileid_meta[2],
                        "mime type": fileid_meta[3],
                        "created at": fileid_meta[4],
                        "modified at": fileid_meta[5],
                    }
                    file_paths.append(final_filepath)

            return file_paths
        except Exception as e:
            logger.error(
                f"An error occurred while loading data from fileids meta: {e}",
                exc_info=True,
            )

    def _load_from_file_ids(
        self,
        drive_id: Optional[str],
        file_ids: List[str],
        mime_types: Optional[List[str]],
        query_string: Optional[str],
    ) -> any:
        """Load data from file ids
        Args:
            file_ids: File ids of the files in google drive.
            mime_types: The mimeTypes you want to allow e.g.: "application/vnd.google-apps.document"
            query_string: List of query strings to filter the documents, e.g. "name contains 'test'".

        Returns:
            Document: List of Documents of text.
        """
        try:
            fileids_meta = []
            for file_id in file_ids:
                fileids_meta.extend(
                    self._get_fileids_meta(
                        drive_id=drive_id,
                        file_id=file_id,
                        mime_types=mime_types,
                        query_string=query_string,
                    )
                )
            return self._load_data_fileids_meta(fileids_meta)
        except Exception as e:
            logger.error(
                f"An error occurred while loading with fileid: {e}", exc_info=True
            )

    def _load_from_folder(
        self,
        drive_id: Optional[str],
        folder_id: str,
        mime_types: Optional[List[str]],
        query_string: Optional[str],
    ) -> any:
        """Load data from folder_id.

        Args:
            drive_id: Drive id of the shared drive in google drive.
            folder_id: folder id of the folder in google drive.
            mime_types: The mimeTypes you want to allow e.g.: "application/vnd.google-apps.document"
            query_string: A more generic query string to filter the documents, e.g. "name contains 'test'".

        Returns:
            Document: List of Documents of text.
        """
        try:
            fileids_meta = self._get_fileids_meta(
                drive_id=drive_id,
                folder_id=folder_id,
                mime_types=mime_types,
                query_string=query_string,
            )
            return self._load_data_fileids_meta(fileids_meta)
        except Exception as e:
            logger.error(
                f"An error occurred while loading from folder: {e}", exc_info=True
            )

    def load_data(
        self,
        drive_id: Optional[str] = None,
        folder_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        mime_types: Optional[List[str]] = None,  # Deprecated
        query_string: Optional[str] = None,
    ) -> any:
        """Load data from the folder id or file ids.

        Args:
            drive_id: Drive id of the shared drive in google drive.
            folder_id: Folder id of the folder in google drive.
            file_ids: File ids of the files in google drive.
            mime_types: The mimeTypes you want to allow e.g.: "application/vnd.google-apps.document"
            query_string: A more generic query string to filter the documents, e.g. "name contains 'test'".
                It gives more flexibility to filter the documents. More info: https://developers.google.com/drive/api/v3/search-files

        Returns:
            List[Document]: A list of documents.
        """
        self._creds = self._get_credentials()

        # If no arguments are provided to load_data, default to the object attributes
        if drive_id is None:
            drive_id = self.drive_id
        if folder_id is None:
            folder_id = self.folder_id
        if file_ids is None:
            file_ids = self.file_ids
        if query_string is None:
            query_string = self.query_string

        if folder_id:
            return self._load_from_folder(drive_id, folder_id, mime_types, query_string)
        elif file_ids:
            return self._load_from_file_ids(
                drive_id, file_ids, mime_types, query_string
            )
        else:
            logger.warning("Either 'folder_id' or 'file_ids' must be provided.")
            return []

    def list_resources(self, **kwargs) -> List[str]:
        """List resources in the specified Google Drive folder or files."""
        self._creds = self._get_credentials()

        drive_id = kwargs.get("drive_id", self.drive_id)
        folder_id = kwargs.get("folder_id", self.folder_id)
        file_ids = kwargs.get("file_ids", self.file_ids)
        query_string = kwargs.get("query_string", self.query_string)

        if folder_id:
            fileids_meta = self._get_fileids_meta(
                drive_id, folder_id, query_string=query_string
            )
        elif file_ids:
            fileids_meta = []
            for file_id in file_ids:
                fileids_meta.extend(
                    self._get_fileids_meta(
                        drive_id, file_id=file_id, query_string=query_string
                    )
                )
        else:
            raise ValueError("Either 'folder_id' or 'file_ids' must be provided.")

        return [meta[0] for meta in fileids_meta]  # Return list of file IDs

    def get_resource_info(self, resource_id: str, **kwargs) -> Dict:
        """Get information about a specific Google Drive resource."""
        self._creds = self._get_credentials()

        fileids_meta = self._get_fileids_meta(file_id=resource_id)
        if not fileids_meta:
            raise ValueError(f"Resource with ID {resource_id} not found.")

        meta = fileids_meta[0]
        return {
            "file_path": meta[2],
            "file_size": None,
            "last_modified_date": meta[5],
            "content_hash": None,
            "content_type": meta[3],
            "author": meta[1],
            "created_date": meta[4],
            "drive_link": meta[6],
        }

    def load_resource(self, resource_id: str, **kwargs) -> any:
        """Load a specific resource from Google Drive."""
        return self._load_from_file_ids(
            self.drive_id, [resource_id], None, self.query_string
        )

    def read_file_content(self, file_path: Union[str, Path], **kwargs) -> bytes:
        """Read the content of a specific file from Google Drive."""
        self._creds = self._get_credentials()

        if self.download_dir:
            temp_dir = self.download_dir
            temp_file = os.path.join(temp_dir, "temp_file")
            downloaded_file = self._download_file(file_path, temp_file)
            with open(downloaded_file, "rb") as file:
                return file.read()


    def save_metadata(self, file_metadata: any, author: str, path: str, **kwargs):
        if self.use_metadata_file:
            id = file_metadata["id"]
            name = file_metadata["name"]
            name_with_extension = name

            try:
                download_extension = self._mimetypes.get(file_metadata["mimeType"], {}).get("extension", "")
                name_with_extension = name + download_extension
            except Exception as e:
                download_extension = file_metadata["fileExtension"]
                pass

            '''
            if file_metadata["mimeType"] == "application/vnd.google-apps.presentation":
                download_extension = '.pdf'
                name_with_extension = name + download_extension
            '''

            metadata = {
                "file_id": id,
                "author": author,
                "path": path,
                "name": name_with_extension,
                "description": name,
                "created": file_metadata["createdTime"],
                "modified": file_metadata["modifiedTime"],
                "url": self._get_drive_link(id),
            }
            metadata_path = os.path.join(self.download_dir, f"{id}.json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f)
            if self.verbose:
                logger.info(f"{name}")

    def is_greater_than_timestamp(self, file_modified_time: datetime) -> bool:
        """Check if the file modified time is greater than the timestamp."""
        is_greater = True
        if self.timestamp is not None:
            is_greater = datetime.fromisoformat(file_modified_time) >= self.timestamp
        return is_greater