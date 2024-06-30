import logging
from pathlib import Path
from typing import List
import polars as pl

LOGGER = logging.getLogger(__name__)


def polars_df_parquet(parquet_files: List[Path]) -> pl.DataFrame:
    """
    Read multiple Parquet files and combine them into a single Polars DataFrame.

    Args:
        parquet_files (List[Path]): A list of paths to Parquet files.

    Returns:
        pl.DataFrame: A single Polars DataFrame containing data from all the Parquet files.
    """
    try:
        dataframes = [pl.read_parquet(file) for file in parquet_files]
        combined_df = pl.concat(dataframes)
        LOGGER.info(f"Combined {len(parquet_files)} Parquet files into a single DataFrame")
        return combined_df
    except Exception as e:
        LOGGER.error(f"Error combining Parquet files into DataFrame: {e}")
        raise


def convert_df_to_string(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert all columns in the DataFrame to string type, order columns alphabetically,
    and reset the index.

    Args:
        df (pl.DataFrame): The DataFrame to convert.

    Returns:
        pl.DataFrame: The DataFrame with all columns converted to string type, ordered alphabetically
    """
    try:

        # Function to strip trailing zeros
        def strip_trailing_zeros_from_string(val):
            if isinstance(val, str):
                try:
                    # Convert string to float and then back to string to strip trailing zeros
                    return str(float(val)).rstrip('0').rstrip('.')
                except ValueError:
                    # If conversion fails, return the original string
                    return val
            return val

        def remove_quotes(text):
            if isinstance(text, str):
                return text.replace('"', '').replace("'", "")
            return text

        def to_lowercase(text):
            if isinstance(text, str):
                return text.lower()
            return text

        # Identify float and Decimal columns
        float_or_decimal_columns = [col for col in df.columns if df[col].dtype in [pl.Float64, pl.Decimal]]

        str_columns = [col for col in df.columns if df[col].dtype in [pl.String]]

        # Convert all columns to string type
        for column in df.columns:
            df = df.with_columns([pl.col(column).cast(pl.Utf8)])

        # Apply the function to each float column
        for col in float_or_decimal_columns:
            df = df.with_columns(df[col].apply(strip_trailing_zeros_from_string).alias(col))

        # Apply the function to each str column
        for col in str_columns:
            df = df.with_columns(df[col].apply(remove_quotes).alias(col))
            df = df.with_columns(df[col].apply(to_lowercase).alias(col))

        # Get the list of columns and sort them alphabetically
        sorted_columns = sorted(df.columns)
        LOGGER.info(sorted_columns)

        for col in sorted_columns:
            df = df.sort([col], maintain_order=True)

        # Convert all columns to string type
        for column in df.columns:
            df = df.with_columns([pl.col(column).cast(pl.Utf8)])

        LOGGER.info("Converted all columns to string type, ordered alphabetically")
        return df
    except Exception as e:
        LOGGER.error(f"Error processing DataFrame: {e}")
        raise