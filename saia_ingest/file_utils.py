import json
from pathlib import Path
from typing import Dict, Any
import hashlib
import logging

from saia_ingest.config import Defaults


def calculate_file_hash(filepath: Path) -> str:
    """Calculate the SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(4096):
                sha256.update(chunk)
    except FileNotFoundError:
        logging.getLogger().error(f"File not found: {filepath}")
        raise
    except PermissionError:
        logging.getLogger().error(f"Permission denied: {filepath}")
        raise
    return sha256.hexdigest()


def are_files_identical(file1: Path, file2: Path) -> bool:
    """Compare two files to see if they are identical by comparing their SHA-256 hashes."""
    try:
        return calculate_file_hash(file1) == calculate_file_hash(file2)
    except Exception as e:
        logging.getLogger().error(f"Error comparing files {file1} and {file2}: {e}")
        return False


def load_hashes_from_json(folder: Path, id:str='documentid') -> Dict[str, Any]:
    """Load all existing hashes from JSON files in the folder."""
    hash_index = {}
    duplicate_count = 0
    for json_file in folder.glob("*.json"):
        try:
            with json_file.open('r') as f:
                data = json.load(f)
                if Defaults.FILE_HASH in data:
                    file_hash = data[Defaults.FILE_HASH]
                    document_id = data[id]
                    if file_hash in hash_index:
                        duplicate_count += 1
                        logging.getLogger().warning(f"{document_id} duplicate detected: using {hash_index[file_hash]}")
                    else:
                        hash_index[file_hash] = document_id
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error reading {json_file}: {e}")
    if duplicate_count > 0:
        logging.getLogger().warning(f"{duplicate_count} duplicates found")
    return hash_index


def check_for_duplicates(new_file: Path, folder: Path) -> bool:
    """Check if the hash of a new file collides with existing hashes."""
    # Load all existing hashes
    hash_index = load_hashes_from_json(Path(folder))
    
    # Calculate the hash of the new file
    new_file_hash = calculate_file_hash(new_file)
    
    if new_file_hash in hash_index:
        print(f"Duplicate found! {new_file} matches {hash_index[new_file_hash]}")
        return True
    else:
        return False

