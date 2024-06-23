import polars as pl
import logging
from boto3 import client
from io import BytesIO
import pyarrow.csv as pv
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)


def read_s3_path_to_polars(s3_client: client, bucket_name: str, s3_path: str) -> pl.DataFrame:
    """
    Read all files from an S3 path, convert them to Parquet, and combine them into a single Polars DataFrame.

    Args:
        s3_client (boto3.client): S3 client object.
        bucket_name (str): Name of the S3 bucket.
        s3_path (str): Path in the S3 bucket where the files are located.

    Returns:
        pl.DataFrame: Combined Polars DataFrame containing the data from all files.
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_path)

        dataframes = []

        for page in pages:
            for obj in page.get('Contents', []):
                s3_key = obj['Key']
                LOGGER.debug(f"Reading file from S3: {s3_key}")
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                file_content = s3_object['Body'].read()

                # Decode file content with ANSI encoding, handling errors by replacing problematic characters
                decoded_content = file_content.decode('windows-1252', errors='replace')

                # Replace pipe delimiter with comma
                modified_content = decoded_content.replace('|', ',')

                # Create a file-like object from the modified content
                file_like_object = BytesIO(modified_content.encode('utf-8'))

                # Use PyArrow to read CSV and write to Parquet
                table = pv.read_csv(file_like_object)
                parquet_buffer = BytesIO()
                pq.write_table(table, parquet_buffer)
                parquet_buffer.seek(0)

                # Read the Parquet file into Polars DataFrame
                df = pl.read_parquet(parquet_buffer)
                dataframes.append(df)
                LOGGER.debug(f"File {s3_key} converted to Parquet and read into Polars DataFrame.")

        if dataframes:
            combined_df = pl.concat(dataframes)
            LOGGER.debug(f"Combined {len(dataframes)} DataFrames into a single DataFrame.")
            return combined_df
        else:
            LOGGER.warning(f"No files found in the path: {s3_path}")
            return pl.DataFrame()
    except Exception as e:
        LOGGER.error(f"Error reading files from S3 path {s3_path}: {e}")
        raise
