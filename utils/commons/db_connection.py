import logging
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import Engine, create_engine
from typing import Dict, Any

LOGGER = logging.getLogger(__name__)


def get_db_config(db_name: str, config: Dict[str, Any]) -> URL:
    if db_name == 'redshift':
        return URL.create(
            drivername='redshift+redshift_connector',
            host=config['redshift_host'],
            port=config.get('redshift_port', 5439),
            database=config['redshift_database'],
            username=config['redshift_username'],
            password=config['redshift_password'],
        )
    elif db_name == 'aurora':
        return URL.create(
            drivername='postgresql+psycopg2',
            host=config['aurora_host'],
            port=config.get('aurora_port', 5432),
            database=config['aurora_database'],
            username=config['aurora_username'],
            password=config['aurora_password'],
        )
    else:
        LOGGER.error(f"Unsupported database: {db_name}")
        raise ValueError(f"Unsupported database: {db_name}")


def create_db_engine(db_name: str, config: Dict[str, Any]) -> Engine:
    try:
        url = get_db_config(db_name, config)
        engine = create_engine(url, connect_args={'sslmode': config.get('sslmode', 'allow')})
        LOGGER.debug(f"Database engine created for {db_name} at {url.host}")
        return engine
    except KeyError as e:
        LOGGER.error(f"Missing required configuration key: {e}")
        raise
    except Exception as e:
        LOGGER.error(f"Error creating database engine: {e}")
        raise


def close_db_engine(engine: Engine) -> None:
    try:
        engine.dispose()
        LOGGER.debug("Database engine connection closed.")
    except Exception as e:
        LOGGER.error(f"Error closing database engine: {e}")
        raise
