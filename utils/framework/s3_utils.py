import logging
from pathlib import Path
from typing import Any, List
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from utils.framework.path_util import get_project_root_path

LOGGER = logging.getLogger(__name__)


def download_csv_from_s3(s3_client: Any, bucket: str, path: str, table_name: str) -> Path:
    """
    Download CSV files from the given S3 bucket and path into the team's specific downloads folder.

    Args:
        s3_client (Any): The S3 client object.
        bucket (str): The name of the S3 bucket.
        path (str): The S3 path where the CSV files are located.
        table_name (str): The name of the table to create a subfolder for the downloaded files.

    Returns:
        List[Path]: A list of paths to the downloaded CSV files.
    """
    try:
        project_root = get_project_root_path()
        downloads_path = project_root / 'downloads'
        table_download_path = downloads_path / table_name

        # Ensure the download directory and subdirectory exist
        if not downloads_path.exists():
            downloads_path.mkdir(parents=True, exist_ok=True)
            LOGGER.debug(f"Created downloads directory at {downloads_path}")

        if not table_download_path.exists():
            table_download_path.mkdir(parents=True, exist_ok=True)
            LOGGER.debug(f"Created table download directory at {table_download_path}")

        downloaded_files = []

        # List and download CSV files
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=path):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.csv'):
                    file_name = obj['Key'].split('/')[-1]
                    download_path = table_download_path / file_name
                    s3_client.download_file(bucket, obj['Key'], str(download_path))
                    LOGGER.info(f"Downloaded {file_name} to {download_path}")
                    downloaded_files.append(download_path)

        return table_download_path

    except (NoCredentialsError, PartialCredentialsError) as e:
        LOGGER.error("Error with AWS credentials: %s", e)
        raise
    except Exception as e:
        LOGGER.error("Error downloading files from S3: %s", e)
        raise
