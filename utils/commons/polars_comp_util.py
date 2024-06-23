import polars as pl


def compare_dataframes(df1: pl.DataFrame, df2: pl.DataFrame) -> (bool, str):
    """
    Compare two dataframes and return if they are identical and a message describing the comparison result.

    Args:
        df1 (pl.DataFrame): First dataframe to compare.
        df2 (pl.DataFrame): Second dataframe to compare.

    Returns:
        (bool, str): A tuple containing a boolean indicating if the dataframes are identical and a message.
    """
    try:
        if df1.frame_equal(df2):
            return True, "Datasets are identical."

        if df1.columns != df2.columns:
            cols1, cols2 = set(df1.columns), set(df2.columns)
            missing_in_df1 = cols2 - cols1
            missing_in_df2 = cols1 - cols2
            return False, f"Column mismatch: Missing in df1: {missing_in_df1}, Missing in df2: {missing_in_df2}"

        if df1.shape != df2.shape:
            return False, f"Shape mismatch: df1 shape: {df1.shape}, df2 shape: {df2.shape}"

        mismatched_rows = df1 != df2
        if mismatched_rows.sum().sum() > 0:
            return False, "Data is not identical in matching rows."

        return False, "DataFrames have the same schema and shape but differ in content."

    except Exception as error:
        return False, f"Error while comparing dataframes: {error}"


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
