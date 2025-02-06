import os
import argparse
import time
from typing import Any, Optional
from datetime import datetime, timezone, timedelta
import logging
import sys
from logging.handlers import RotatingFileHandler

from ..ingestor import ingest_s3, ingest_jira, ingest_confluence, ingest_github, ingest_gdrive, ingest_sharepoint, ingest_file_system
from ..log import AccumulatingLogHandler

logging.basicConfig(level=logging.INFO)
sys.stdout.reconfigure(encoding='utf-8')
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_name = f"debug/run_{timestamp}.txt"
log_dir = os.path.dirname(log_file_name)
if not os.path.isdir(log_dir):
    os.mkdir(log_dir)
file_handler = RotatingFileHandler(log_file_name, maxBytes=1024*1024*1024, backupCount=50)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
logging.getLogger().addHandler(AccumulatingLogHandler())

def handle_ingest(
    config: Optional[str] = None,
    timestamp: Optional[str] = None,
    days: Optional[int] = None,
    type: str = "test",
    **kwargs: Any,
) -> None:
    """
    Handles the ingest command.
    """

    assert config is not None
    assert type is not None

    start_time = time.time()
    config_file = config

    if days is not None:
        timestamp = datetime.now(timezone.utc) - timedelta(days=days)
    elif timestamp is not None:
        timestamp = datetime.fromisoformat(timestamp).replace(tzinfo=timezone.utc)

    if type == "s3":
        ret = ingest_s3(config_file, start_time, timestamp=timestamp)
    elif type == "sharepoint":
        ret = ingest_sharepoint(config_file, start_time)
    elif type == "jira":
        ret = ingest_jira(config_file, timestamp=timestamp)
    elif type == "confluence":
        ret = ingest_confluence(config_file, timestamp=timestamp)
    elif type == "github":
        ret = ingest_github(config_file)
    elif type == "gdrive":
        ret = ingest_gdrive(config_file)
    elif type == "fs":
        ret = ingest_file_system(config_file, timestamp=timestamp)
    else:
        logging.getLogger().error(f"Unknown {type} type")
        return False

    if ret:
        formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M") if timestamp is not None else "no timestamp"
        logging.getLogger().info(f"{type} ingestion '{formatted_timestamp}' config: {config_file}")
    
    file_handler.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Globant Enterprise AI CLI")

    # Subparsers for the main commands
    subparsers = parser.add_subparsers(title="commands", dest="command", required=True)

    saia_parser = subparsers.add_parser(
        "ingest", help="Ingest from a known connector"
    )
    saia_parser.add_argument(
        "-c",
        "--config",
        type=str,
        help=(
            "configuration file"
        ),
    )
    saia_parser.add_argument(
        "-t",
        "--timestamp",
        type=str,
        default=None,
        help="Custom timestamp.",
    )
    saia_parser.add_argument(
        "-d",
        "--days",
        type=int,
        default=0,
        help="Last X days to consider as timestamp (examples: 7=last week, 1=yesterday).",
    )
    saia_parser.add_argument(
        "--type",
        type=str,
        default="s3",
        help="type of ingestion",
    )
    saia_parser.set_defaults(
        func=lambda args: handle_ingest(**vars(args))
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    # Call the appropriate function based on the command
    args.func(args)


if __name__ == "__main__":
    main()
