import polars as pl
from sqlalchemy.engine import Engine
from typing import List, Dict


def read_sql_query_as_df(engine: Engine, query: str) -> pl.DataFrame:
    """
    Execute a SQL query using the provided SQLAlchemy engine and return the result as a Polars DataFrame.

    Args:
        engine (Engine): SQLAlchemy engine to use for the database connection.
        query (str): SQL query to execute.

    Returns:
        pl.DataFrame: Polars DataFrame containing the query results.
    """
    try:
        # Read the data from the database into a Polars DataFrame
        df = pl.read_database(query, connection=engine)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to execute query: {query}") from e


def read_sql_query(engine: Engine, query: str) -> List[Dict[str, any]]:
    """
    Execute a SQL query using the provided SQLAlchemy engine and return the result as a list of dictionaries.

    Args:
        engine (Engine): SQLAlchemy engine to use for the database connection.
        query (str): SQL query to execute.

    Returns:
        List[Dict[str, any]]: List of dictionaries containing the query results.
    """
    try:
        # Connect to the database
        with engine.connect() as connection:
            # Execute the query
            result = connection.execute(query)
            # Fetch all results
            rows = result.fetchall()
            # Get column names from the result
            columns = result.keys()
            # Convert the results into a list of dictionaries
            data = [dict(zip(columns, row)) for row in rows]
            return data
    except Exception as e:
        raise RuntimeError(f"Failed to execute query: {query}") from e


def run_sql_query(engine: Engine, query: str) -> None:
    """
    Execute a SQL query using the provided SQLAlchemy engine and return the result as a list of dictionaries.

    Args:
        engine (Engine): SQLAlchemy engine to use for the database connection.
        query (str): SQL query to execute.

    Returns:
        List[Dict[str, any]]: List of dictionaries containing the query results.
    """
    try:
        # Connect to the database
        with engine.connect() as connection:
            # Execute the query
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)
    except Exception as e:
        raise RuntimeError(f"Failed to execute query: {query}") from e