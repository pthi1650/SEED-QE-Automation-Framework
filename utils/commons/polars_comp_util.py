import logging
import pandas
import polars as pl

LOGGER = logging.getLogger(__name__)


def sort_dataframe_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Sort the DataFrame columns alphabetically.

    Args:
        df (pl.DataFrame): The DataFrame to sort.

    Returns:
        pl.DataFrame: The DataFrame with columns sorted alphabetically.
    """
    sorted_columns = sorted(df.columns)
    return df.select(sorted_columns)


def compare_dataframes(df1: pl.DataFrame, df2: pl.DataFrame, df1_name: str = "df1", df2_name: str = "df2") -> (bool, str, pl.DataFrame):
    """
    Compare two dataframes and return if they are identical, a message describing the comparison result,
    and a DataFrame containing the mismatched rows.

    Args:
        df1 (pl.DataFrame): First dataframe to compare.
        df2 (pl.DataFrame): Second dataframe to compare.
        df1_name (str): Name of the first dataframe for reporting.
        df2_name (str): Name of the second dataframe for reporting.

    Returns:
        (bool, str, pl.DataFrame): A tuple containing a boolean indicating if the dataframes are identical,
                                   a message, and a DataFrame with mismatched rows.
    """
    try:

        if df1.equals(df2):
            return True, "Datasets are identical.", pl.DataFrame()

        if df1.columns != df2.columns:
            cols1, cols2 = set(df1.columns), set(df2.columns)
            missing_in_df1 = cols2 - cols1
            missing_in_df2 = cols1 - cols2
            return False, f"Column mismatch: Missing in {df1_name}: {missing_in_df1}, Missing in {df2_name}: {missing_in_df2}", pl.DataFrame()

        if df1.shape != df2.shape:
            return False, f"Shape mismatch: {df1_name} shape: {df1.shape}, {df2_name} shape: {df2.shape}", pl.DataFrame()

        mismatched_rows = []
        for col in df1.columns:
            mismatch_indices = (df1[col] != df2[col]).to_numpy().nonzero()[0]
            for index in mismatch_indices[:2]:  # Get up to 2 mismatched rows
                mismatched_rows.append((df1.row(index), df2.row(index)))

        if mismatched_rows:
            mismatched_df = pl.DataFrame(mismatched_rows, schema=[f"{df1_name}_row", f"{df2_name}_row"])
            return False, "Data is not identical. See mismatched rows.", mismatched_df

        return False, "DataFrames have the same schema and shape but differ in content.", pl.DataFrame()

    except Exception as error:
        return False, f"Error while comparing dataframes: {error}", pl.DataFrame()


def generate_html_report(mismatched_df: pl.DataFrame, file_name: str) -> None:
    """
    Generate an HTML report for the mismatched rows.

    Args:
        mismatched_df (pl.DataFrame): DataFrame containing the mismatched rows.
        file_name (str): Name of the HTML file to save the report.
    """
    try:
        # Highlight mismatches
        html_content = mismatched_df.to_pandas().to_html().replace('<td>', '<td style="background-color: #FFDDDD;">')

        with open(file_name, "w") as file:
            file.write(html_content)
        LOGGER.info(f"Generated HTML report: {file_name}")
    except Exception as e:
        LOGGER.error(f"Error generating HTML report: {e}")
        raise


def get_union_dataset(df1: pl.DataFrame, df2: pl.DataFrame) -> (pl.DataFrame, list):
    """
    Get the union result set for two polars dataframes.

    Args:
        df1 (pl.DataFrame): First dataframe.
        df2 (pl.DataFrame): Second dataframe.

    Returns:
        (pl.DataFrame, list): The union dataframe and its JSON representation.
    """
    try:
        df_union_results = pl.concat([df1, df2]).unique()
        df_union_json_data = df_union_results.to_dicts()
        return df_union_results, df_union_json_data

    except Exception as error:
        return pl.DataFrame(), f"Error while performing union on dataframes: {error}"


def get_inner_join_dataset(df1: pl.DataFrame, df2: pl.DataFrame, col: str) -> (pl.DataFrame, list):
    """
    Get the inner join result set for two polars dataframes.

    Args:
        df1 (pl.DataFrame): First dataframe.
        df2 (pl.DataFrame): Second dataframe.
        col (str): Column name to join on.

    Returns:
        (pl.DataFrame, list): The inner join dataframe and its JSON representation.
    """
    try:
        if col in df1.columns and col in df2.columns:
            df_inner_join_results = df1.join(df2, on=col, how="inner")
            df_inner_join_json_data = df_inner_join_results.to_dicts()
            return df_inner_join_results, df_inner_join_json_data
        else:
            return pl.DataFrame(), f"The key column '{col}' is not available in both datasets."

    except Exception as error:
        return pl.DataFrame(), f"Error while performing inner join on dataframes: {error}"


def get_left_join_dataset(df1: pl.DataFrame, df2: pl.DataFrame, col: str) -> (pl.DataFrame, list):
    """
    Get the left join result set for two polars dataframes.

    Args:
        df1 (pl.DataFrame): First dataframe.
        df2 (pl.DataFrame): Second dataframe.
        col (str): Column name to join on.

    Returns:
        (pl.DataFrame, list): The left join dataframe and its JSON representation.
    """
    try:
        if col in df1.columns and col in df2.columns:
            df_left_join_results = df1.join(df2, on=col, how="left")
            df_left_join_json_data = df_left_join_results.to_dicts()
            return df_left_join_results, df_left_join_json_data
        else:
            return pl.DataFrame(), f"The key column '{col}' is not available in both datasets."

    except Exception as error:
        return pl.DataFrame(), f"Error while performing left join on dataframes: {error}"
