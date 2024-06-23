import boto3
import logging
from typing import Dict, Any
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

LOGGER = logging.getLogger(__name__)


def get_s3_client(config: Dict[str, Any]):
    """
    Establish a connection to AWS S3 using the provided configuration.

    Args:
        config (Dict[str, Any]): Configuration dictionary with S3 connection details.

    Returns:
        boto3.client: S3 client object.
    """
    try:

        s3_client = boto3.client(
            's3',
            aws_access_key_id=config['s3_aws_key'],
            aws_secret_access_key=config['s3_aws_secret'],
            aws_session_token=config['s3_aws_token'],
            region_name='eu-west-1'
           )
        LOGGER.debug("S3 client created successfully.")
        return s3_client
    except (NoCredentialsError, PartialCredentialsError) as e:
        LOGGER.error("Error with AWS credentials: %s", e)
        raise
    except Exception as e:
        LOGGER.error("Error creating S3 client: %s", e)
        raise
