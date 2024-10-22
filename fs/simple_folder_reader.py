from datetime import datetime, timezone
from functools import reduce
from timeit import repeat
import warnings
import fsspec
from fsspec.implementations.local import LocalFileSystem
import multiprocessing
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Type, Union

from llama_index import Document

from saia_ingest.config import Defaults


class SimpleDirectoryReader():
    """
    Simple directory reader.

    Load files from file directory.

    Args:
        input_dir (Union[Path, str]): Path to the directory.
        exclude (List): glob of python file paths to exclude (Optional)
        exclude_hidden (bool): Whether to exclude hidden files (dotfiles).
        encoding (str): Encoding of the files, defaults toutf-8.
        recursive (bool): Whether to recursively search in subdirectories, False by default.
        required_exts (Optional[List[str]]): List of required extensions, defaults to None.
        num_files_limit (Optional[int]): Maximum number of files to read, defaults to None.
        fs (Optional[fsspec.AbstractFileSystem]): File system to use. Defaults
    """

    def __init__(
        self,
        input_dir: Optional[Union[Path, str]] = None,
        exclude: Optional[List] = None,
        exclude_hidden: bool = True,
        recursive: bool = False,
        encoding: str = "utf-8",
        required_exts: Optional[List[str]] = None,
        num_files_limit: Optional[int] = None,
        fs: Optional[fsspec.AbstractFileSystem] = None,
        timestamp: Optional[datetime] = None
    ) -> None:
        """Initialize with parameters."""

        if not input_dir:
            raise ValueError("Must provide `input_dir`.")

        self.fs = fs or get_default_fs()
        self.encoding = encoding
        self.saia_metadata_exclusion = [Defaults.PACKAGE_METADATA_POSTFIX]

        self.exclude = exclude if exclude is not None else self.saia_metadata_exclusion
        self.recursive = recursive
        self.exclude_hidden = exclude_hidden
        self.required_exts = required_exts
        self.num_files_limit = num_files_limit
        self.timestamp = timestamp

        if input_dir:
            if not self.fs.isdir(input_dir):
                raise ValueError(f"Directory {input_dir} does not exist.")
            self.input_dir = Path(input_dir)
            self.exclude = exclude
            self.input_files = self._add_files(self.input_dir)

    def is_hidden(self, path: Path) -> bool:
        return any(
            part.startswith(".") and part not in [".", ".."] for part in path.parts
        )

    def _add_files(self, input_dir: Path) -> List[Path]:
        """Add files."""
        all_files: Set[Path] = set()
        rejected_files: Set[Path] = set()
        rejected_dirs: Set[Path] = set()
        # Default to POSIX paths for non-default file systems (e.g. S3)
        _Path = Path if is_default_fs(self.fs) else PurePosixPath

        if self.exclude is not None:
            for excluded_pattern in self.exclude:
                if self.recursive:
                    # Recursive glob
                    excluded_glob = _Path(input_dir) / _Path("**") / excluded_pattern
                else:
                    # Non-recursive glob
                    excluded_glob = _Path(input_dir) / excluded_pattern
                for file in self.fs.glob(str(excluded_glob)):
                    if self.fs.isdir(file):
                        rejected_dirs.add(_Path(file))
                    else:
                        rejected_files.add(_Path(file))

        file_refs: List[str] = []
        if self.recursive:
            file_refs = self.fs.glob(str(input_dir) + "/**/*")
        else:
            file_refs = self.fs.glob(str(input_dir) + "/*")

        for _ref in file_refs:
            ref = _Path(_ref)
            is_dir = self.fs.isdir(ref)
            skip_because_hidden = self.exclude_hidden and self.is_hidden(ref)
            skip_because_bad_ext = (
                self.required_exts is not None and ref.suffix not in self.required_exts
            )
            skip_because_excluded = ref in rejected_files
            if not skip_because_excluded:
                if is_dir:
                    ref_parent_dir = ref
                else:
                    ref_parent_dir = self.fs._parent(ref)
                for rejected_dir in rejected_dirs:
                    if str(ref_parent_dir).startswith(str(rejected_dir)):
                        skip_because_excluded = True
                        break

            if (
                is_dir
                or skip_because_hidden
                or skip_because_bad_ext
                or skip_because_excluded
            ):
                continue

            if self.timestamp is not None:
                file_modified_time = datetime.fromtimestamp(ref.stat().st_mtime).replace(tzinfo=timezone.utc)
                if file_modified_time < self.timestamp:
                    continue
            
            all_files.add(ref)

        new_input_files = sorted(all_files)

        if self.num_files_limit is not None and self.num_files_limit > 0:
            new_input_files = new_input_files[0 : self.num_files_limit]

        return new_input_files

    def _exclude_metadata(self, documents: List[Document]) -> List[Document]:
        """
        Exclude metadata from documents.

        Args:
            documents (List[Document]): List of documents.
        """
        for doc in documents:
            # Keep only metadata['file_path'] in both embedding and llm content
            # str, which contain extreme important context that about the chunks.
            # Dates is provided for convenience of postprocessor such as
            # TimeWeightedPostprocessor, but excluded for embedding and LLMprompts
            doc.excluded_embed_metadata_keys.extend(
                [
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ]
            )
            doc.excluded_llm_metadata_keys.extend(
                [
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ]
            )

        return documents

    def list_resources(self, *args: Any, **kwargs: Any) -> List[str]:
        """List files in the given filesystem."""
        return [str(x) for x in self.input_files]

    def load_resource(
        self, resource_id: str, *args: Any, **kwargs: Any
    ) -> List[Document]:
        encoding = kwargs.get("encoding", self.encoding)
        fs = kwargs.get("fs", self.fs)

        path_func = Path

        return SimpleDirectoryReader.load_file(
            input_file=path_func(resource_id),
            encoding=encoding,
            fs=fs,
            **kwargs,
        )

    def read_file_content(self, input_file: Path, **kwargs: Any) -> bytes:
        """Read file content."""
        with self.fs.open(input_file, errors=self.errors, encoding=self.encoding) as f:
            return f.read()

    @staticmethod
    def load_file(
        input_file: Path,
        encoding: str = "utf-8",
        errors: str = "ignore",
        fs: Optional[fsspec.AbstractFileSystem] = None,
    ) -> List[Document]:
        """
        Static method for loading file.

                Returns:
            List[Document]: loaded documents
        """

        metadata: Optional[dict] = None
        documents: List[Document] = []

        fs = fs or get_default_fs()
        with fs.open(input_file, errors=errors, encoding=encoding) as f:
            data = f.read().decode(encoding, errors=errors)

        doc = Document(text=data, metadata=metadata or {})

        documents.append(doc)

        return documents

    def load_data(
        self,
        num_workers: Optional[int] = None,
        fs: Optional[fsspec.AbstractFileSystem] = None,
    ) -> List[Document]:
        """
        Load data from the input directory.

        Args:
            num_workers  (Optional[int]): Number of workers to parallelize data-loading over.
            fs (Optional[fsspec.AbstractFileSystem]): File system to use. If fs was specified
                in the constructor, it will override the fs parameter here.

        Returns:
            List[Document]: A list of documents.
        """
        documents = []

        files_to_process = self.input_files
        fs = fs or self.fs

        if num_workers and num_workers > 1:
            if num_workers > multiprocessing.cpu_count():
                warnings.warn(
                    "Specified num_workers exceed number of CPUs in the system. "
                    "Setting `num_workers` down to the maximum CPU count."
                )
            with multiprocessing.get_context("spawn").Pool(num_workers) as p:
                results = p.starmap(
                    SimpleDirectoryReader.load_file,
                    zip(
                        files_to_process,
                        repeat(self.encoding),
                        repeat(fs),
                    ),
                )
                documents = reduce(lambda x, y: x + y, results)

        else:
            for input_file in files_to_process:
                documents.extend(
                    SimpleDirectoryReader.load_file(
                        input_file=input_file,
                        encoding=self.encoding,
                        fs=fs,
                    )
                )

        return self._exclude_metadata(documents)

    def iter_data(
        self
    ) -> Generator[List[Document], Any, Any]:
        """
        Load data iteratively from the input directory.

        Returns:
            Generator[List[Document]]: A list of documents.
        """
        files_to_process = self.input_files

        for input_file in files_to_process:
            documents = SimpleDirectoryReader.load_file(
                input_file=input_file,
                encoding=self.encoding,
                fs=self.fs,
            )

            documents = self._exclude_metadata(documents)

            if len(documents) > 0:
                yield documents


def get_default_fs() -> fsspec.AbstractFileSystem:
    return LocalFileSystem()

def is_default_fs(fs: fsspec.AbstractFileSystem) -> bool:
    return isinstance(fs, LocalFileSystem) and not fs.auto_mkdir
