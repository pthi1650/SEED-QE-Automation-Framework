import logging

from utils.commons.polars_comp_util import compare_dataframes, get_union_dataset, get_inner_join_dataset, \
    get_left_join_dataset
from utils.commons.polars_sql_util import read_sql_query
from utils.commons.cloud_connection import get_s3_client
from utils.commons.polars_cloud_util import read_s3_path_to_polars
LOGGER = logging.getLogger(__name__)


def test_manual_data_loading_flow(config_fixture, redshift_engine):
    # Use the redshift_engine fixture to perform database operations
    LOGGER.debug(config_fixture.settings.items())

    # Example using S3 storage to read data into Polars DataFrame it will be df2
    s3_client = get_s3_client(config_fixture.settings)
    s3_bucket = config_fixture.settings['s3_bucket']
    s3_path = config_fixture.settings.get('s3_path', '')
    df1 = read_s3_path_to_polars(s3_client, s3_bucket, s3_path)

    # New names for the first 3 columns
    new_names = ['lgl_full_nm', 'sysco_ntwk_id', 'pstn_desc']

    # Rename the first 3 columns
    df1 = df1.rename({old: new for old, new in zip(df1.columns[:3], new_names)})
    LOGGER.info(df1)
    LOGGER.info("S3 data loading completed")

    # Query LNDP table in Redshift
    query = "SELECT * FROM ts_eu_pgm_lndp.org_emple_pstn_fra"
    df2 = read_sql_query(redshift_engine, query)
    # drop last 3 columns
    df2_src_compare = df2[:, :-3]
    LOGGER.info(df2)
    LOGGER.info("LNDP Data loading completed")

    # Query EDWP table in Redshift
    query = "SELECT * FROM ts_eu_pgm_edwp.org_emple_pstn_fra_dim"
    df3 = read_sql_query(redshift_engine, query)
    LOGGER.info(df3)
    LOGGER.info("EDWP Data loading completed")

    # Compare DataFrames
    src_lndp_match, _ = compare_dataframes(df1, df2_src_compare)
    assert src_lndp_match

    lndp_edwp_match, _ = compare_dataframes(df2, df3)
    assert lndp_edwp_match

    # # Get Union of DataFrames
    # df_union, _ = get_union_dataset(df3, df2)
    # LOGGER.info(df_union)
    #
    # # Get Inner Join of DataFrames
    # df_inner_join, _ = get_inner_join_dataset(df3, df2, 'sysco_ntwk_id')
    # LOGGER.info(df_inner_join)
    #
    # # Get Left Join of DataFrames
    # df_left_join, _ = get_left_join_dataset(df3, df2, 'sysco_ntwk_id')
    # LOGGER.info(df_left_join)

