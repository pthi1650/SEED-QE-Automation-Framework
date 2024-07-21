import logging
from pathlib import Path

from utils.commons.file_convert_util import convert_to_parquet
from utils.commons.polars_comp_util import compare_dataframes, generate_html_report
from utils.commons.polars_sql_util import read_sql_query_as_df, read_sql_query, run_sql_query
from utils.commons.polars_util import polars_df_parquet, convert_df_to_string
from utils.framework.path_util import get_project_root_path
from utils.framework.s3_utils import download_csv_from_s3

LOGGER = logging.getLogger(__name__)


def test_automate_data_loading_flow(config_fixture, stg_client_fixture, etl_db_engine_fixture, table_name):
    """
    Automate data loading flow for testing purposes.

    Args:
        config_fixture: Configuration fixture for settings.
        stg_client_fixture: Stage client fixture for AWS interactions.
        etl_db_engine_fixture: Database engine fixture for Redshift.
        table_name (str): Table name for the data loading process.
    """
    # Debug configuration settings
    LOGGER.debug(config_fixture.settings.items())

    # Retrieve S3 bucket and path from the configuration
    s3_bucket = config_fixture.settings[table_name]['stage']['aws_s3']['stg_s3_bucket']
    s3_path = config_fixture.settings[table_name]['stage']['aws_s3']['stg_s3_path']

    LOGGER.info(f"Using S3 Bucket: {s3_bucket} and Path: {s3_path}")

    role = config_fixture.settings.get('aws_iam_role')
    s3_uri = f"s3://{s3_bucket}/{s3_path}"

    LOGGER.info(f"S3 URI: {s3_uri}")
    LOGGER.debug(f"Using IAM Role: {role}")

    # Step 1: Check existing external schemas
    query1 = "SELECT * FROM svv_external_schemas WHERE esoptions LIKE '%IAM_ROLE%';"
    try:
        query_results1 = read_sql_query(etl_db_engine_fixture, query1)
        LOGGER.info("External Schemas with IAM Role:")
        LOGGER.info(query_results1)
    except Exception as e:
        LOGGER.error(f"Failed to execute query: {query1}. Error: {e}")
        raise

    # Enable autocommit for the connection
    etl_db_engine_fixture.raw_connection().autocommit = True

    # schema name in data catalog
    schema_name = "qe_auto_spectrum"

    # Step 2: Create external schema using data catalog
    query2 = f"""
    CREATE EXTERNAL SCHEMA qe_auto_spectrum
    FROM DATA CATALOG
    DATABASE 'qe_auto_glue_test_db'
    IAM_ROLE '{role}';
    """
    LOGGER.debug(f"Executing query: {query2}")

    try:
        query_results2 = run_sql_query(etl_db_engine_fixture, query2)
        LOGGER.info(f"External schema {schema_name} created successfully.")
        LOGGER.info(query_results2)
    except Exception as e:
        LOGGER.error(f"Failed to execute query: {query2}. Error: {e}")
        raise

    # Step 3: Create a table in the external schema
    query3 = f"""
    CREATE EXTERNAL TABLE {schema_name}.test_table (
      catgy_hier1_nm VARCHAR(60),
      catgy_hier2_nm VARCHAR(60),
      catgy_hier3_nm VARCHAR(40),
      sku_cd VARCHAR(100),
      sku_nm VARCHAR(150),
      lot_nm VARCHAR(150),
      pck_sz_cd VARCHAR(60),
      wgt_qty NUMERIC(30, 20), 
      wgt_unit_cd VARCHAR(30),
      list_prc_ext_amt NUMERIC(20, 15), 
      list_prc_or_wgt_amt NUMERIC(25, 21), 
      liv_trff_lght_desc VARCHAR(60)
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE
    LOCATION '{s3_uri}'
    TABLE PROPERTIES ('skip.header.line.count'='1', 'serialization.encoding'='UTF-8');
    """
    LOGGER.debug(f"Executing query: {query3}")

    try:
        query_results3 = run_sql_query(etl_db_engine_fixture, query3)
        LOGGER.info("External table 'test_table' created successfully.")
        LOGGER.info(query_results3)
    except Exception as e:
        LOGGER.error(f"Failed to execute query: {query3}. Error: {e}")
        raise

    # Step 4: run minus query
    query4 = f"""
    SELECT 
        catgy_hier1_nm,
        catgy_hier2_nm,
        catgy_hier3_nm,
        sku_cd,
        sku_nm,
        lot_nm,
        pck_sz_cd,
        wgt_qty,
        wgt_unit_cd,
        list_prc_ext_amt,
        list_prc_or_wgt_amt,
        liv_trff_lght_desc
    FROM 
        {schema_name}.test_table
    EXCEPT
    SELECT 
        catgy_hier1_nm,
        catgy_hier2_nm,
        catgy_hier3_nm,
        sku_cd,
        sku_nm,
        lot_nm,
        pck_sz_cd,
        wgt_qty,
        wgt_unit_cd,
        list_prc_ext_amt,
        list_prc_or_wgt_amt,
        liv_trff_lght_desc
    FROM 
        ts_eu_pgm_edwp.out_trff_lght_mstr_rdx_gb_fact;
    """
    LOGGER.debug(f"Executing query: {query4}")

    try:
        query_results4 = run_sql_query(etl_db_engine_fixture, query4)
        LOGGER.info("Comparing External table and Internal table.")
        LOGGER.info(query_results4)
    except Exception as e:
        LOGGER.info(f"Failed to execute query: {query4}. Error: {e}")
        raise
