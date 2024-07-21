import logging
from pathlib import Path

from utils.commons.file_convert_util import convert_to_parquet
from utils.commons.polars_comp_util import compare_dataframes, generate_html_report
from utils.commons.polars_sql_util import read_sql_query_as_df
from utils.commons.polars_util import polars_df_parquet, convert_df_to_string
from utils.framework.path_util import get_project_root_path
from utils.framework.s3_utils import download_csv_from_s3

LOGGER = logging.getLogger(__name__)


def test_automate_data_loading_flow(config_fixture, stg_client_fixture, etl_db_engine_fixture, table_name):

    # Use during development or troubleshooting to see all configurations loaded by custom_conf
    LOGGER.debug(config_fixture.settings.items())

    # Example using S3 storage to read data into Polars DataFrame it will be df2
    s3_client = stg_client_fixture
    s3_bucket = config_fixture.settings[table_name]['stage']['aws_s3']['stg_s3_bucket']
    s3_path = config_fixture.settings[table_name]['stage']['aws_s3']['stg_s3_path']

    LOGGER.info(f"Using S3 Bucket: {s3_bucket} and Path: {s3_path}")

    # Download CSV files from S3
    table_download_path = download_csv_from_s3(s3_client, s3_bucket, s3_path, table_name)

    # Convert downloaded CSV files to Parquet with pipe delimiter
    convert_to_parquet(table_download_path, 'csv', delimiter='|')

    # Load Parquet files into a single Polars DataFrame
    parquet_files = list(table_download_path.glob('*.parquet'))
    df1 = polars_df_parquet(parquet_files)
    LOGGER.info(f"Combined DataFrame: {df1}")

    # Apply column mappings from the config to df1, if available
    if 'column_map' in config_fixture.settings[table_name]['stage']['aws_s3']:
        column_map = config_fixture.settings[table_name]['stage']['aws_s3']['column_map']
        df1.columns = column_map
        LOGGER.info(f"Renamed columns of df1: {df1.columns}")
    else:
        LOGGER.info("No column_map found in the configuration; using default column names")

        # Convert df1 to string type
    df1 = convert_df_to_string(df1)

    # Query EDWP table in Redshift
    query = f"SELECT * FROM ts_eu_pgm_edwp.{table_name};"
    df3 = read_sql_query_as_df(etl_db_engine_fixture, query)

    LOGGER.info("EDWP Data loading completed")

    # Check that the last 3 columns of df3 are not null
    last_three_columns = df3.columns[-3:]
    for col in last_three_columns:
        assert df3[col].drop_nulls().shape[0] == df3.shape[0], f"Column {col} contains null values"

    # Drop the last 3 columns from df3 before comparison
    df3_trimmed = df3.drop(last_three_columns)

    LOGGER.info(df3_trimmed)

    # Convert df3_trimmed to string type
    df3_trimmed = convert_df_to_string(df3_trimmed)

    LOGGER.info(df3_trimmed)

    # Log the number of columns and list them
    LOGGER.info(f"Number of columns in df3_trimmed: {len(df3_trimmed.columns)}")
    LOGGER.info(f"Columns in df3_trimmed: {df3_trimmed.columns}")

    # Compare the DataFrames
    are_identical, comparison_message, mismatched_df = compare_dataframes(df1, df3_trimmed)

    LOGGER.info(f"DataFrames are identical : {comparison_message}")

    if not are_identical:
        html_report_path = Path(get_project_root_path()) / "mismatch_report.html"
        generate_html_report(mismatched_df, str(html_report_path))
        assert are_identical, f"DataFrames are not identical: {comparison_message}\nSee {html_report_path} for details."

    assert are_identical, "DataFrames are not identical"
