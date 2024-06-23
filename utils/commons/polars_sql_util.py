import polars as pl
from sqlalchemy.engine import Engine


def read_sql_query(engine: Engine, query: str) -> pl.DataFrame:
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
