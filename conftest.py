import sys
import os
import json
from datetime import datetime

import pytest
import logging
from custom_conf.initialization import initialize_config
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.engine import Engine
from utils.commons.db_connection import create_db_engine, close_db_engine

LOGGER = logging.getLogger(__name__)

# Add the project directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def pytest_configure(config):
    if not hasattr(config, 'workerinput'):
        # Create a timestamped sub-folder in the logs directory only in the main process
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        logs_dir = os.path.join(config.rootdir, 'logs', current_time)
        os.makedirs(logs_dir, exist_ok=True)
        # Store the logs directory path in an environment variable for workers
        os.environ["LOGS_DIR"] = logs_dir
    else:
        # Workers read the logs directory path from the environment variable
        logs_dir = os.environ["LOGS_DIR"]

    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is not None:
        log_file = os.path.join(logs_dir, f"tests_{worker_id}.log")
    else:
        log_file = os.path.join(logs_dir, "tests.log")

    logging.basicConfig(
        format=config.getini("log_file_format"),
        filename=log_file,
        level=config.getini("log_file_level"),
    )

    # Store the logs directory path in the config object for use in tests if needed
    config.logs_dir = logs_dir


@pytest.fixture(scope="session")
def logs_dir(request):
    return request.config.logs_dir


def load_configs():
    config_file_path = os.path.join(os.path.dirname(__file__), 'input_params', 'main_conf.json')
    with open(config_file_path, 'r') as file:
        config_data = json.load(file)

    all_configs = []
    for team in config_data['teams']:
        team_key = team['team_key']
        detect_env_vars = team['detect_env_vars']
        remote_config_src_type = team['remote_config_src_type']
        allow_remote_update = team['allow_remote_update']

        for environment in team['environments']:
            all_configs.append({
                'team_key': team_key,
                'environment': environment,
                'detect_env_vars': detect_env_vars,
                'remote_config_src_type': remote_config_src_type,
                'allow_remote_update': allow_remote_update
            })

    return all_configs


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def initialize_configuration(team_key, environment, detect_env_vars, remote_config_src_type, allow_remote_update):
    return initialize_config(
        team_key=team_key,
        environment=environment,
        detect_env_vars=detect_env_vars,
        remote_config_src_type=remote_config_src_type,
        allow_remote_update=allow_remote_update
    )


@pytest.fixture(scope="function", params=load_configs())
def config_fixture(request):
    """
    Fixture to initialize configuration for different teams and environments.

    Uses the `initialize_configuration` function to set up the configuration.
    """
    LOGGER.info(f"Initializing config for {request.param['team_key']} - {request.param['environment']}")

    config = initialize_configuration(
        team_key=request.param['team_key'],
        environment=request.param['environment'],
        detect_env_vars=request.param['detect_env_vars'],
        remote_config_src_type=request.param['remote_config_src_type'],
        allow_remote_update=request.param['allow_remote_update']
    )

    # Verify that the configuration is loaded without empty or
    assert config is not None, "Configuration manager should not be None"
    assert hasattr(config, 'settings'), "Configuration manager should have settings attribute"

    yield config

    LOGGER.info(f"Tearing down config for {request.param['team_key']} - {request.param['environment']}")


@pytest.fixture(scope='function')
def redshift_engine(config_fixture) -> Engine:
    db_config = config_fixture.settings
    engine = create_db_engine('redshift', db_config)
    yield engine
    close_db_engine(engine)


@pytest.fixture(scope='function')
def aurora_engine(config_fixture) -> Engine:
    db_config = config_fixture.settings
    engine = create_db_engine('aurora', db_config)
    yield engine
    close_db_engine(engine)
