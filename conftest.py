import sys
import os
import json
from datetime import datetime
import pytest
import logging
from custom_conf.initialization import initialize_config
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.engine import Engine

from utils.commons.cloud_connection import get_s3_client
from utils.commons.file_util import load_yaml_file, file_exists
from utils.commons.db_connection import create_db_engine, close_db_engine
from utils.framework.path_util import get_team_folder_path, get_team_folder_path_with_key, get_input_params_path

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
    config_file_path = get_input_params_path() / 'main_conf.json'
    with open(config_file_path, 'r') as file:
        config_data = json.load(file)

    all_configs = []
    for team in config_data['teams']:
        team_key = team['team_key']
        detect_env_vars = team['detect_env_vars']
        remote_config_src_type = team['remote_config_src_type']
        allow_remote_update = team['allow_remote_update']
        table_names = team.get('table_names', None)  # Get table_names from JSON, if present

        for environment in team['environments']:
            all_configs.append({
                'team_key': team_key,
                'environment': environment,
                'detect_env_vars': detect_env_vars,
                'remote_config_src_type': remote_config_src_type,
                'allow_remote_update': allow_remote_update,
                'table_names': table_names  # Include table_names in the config
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


def pytest_addoption(parser):
    parser.addoption("--table_names", action="store", default=None,
                     help="Comma-separated list of table names for loading YAML configurations")


def pytest_generate_tests(metafunc):
    table_names = metafunc.config.option.table_names
    if table_names:
        table_names = table_names.split(',')
    else:
        # If no table_names are provided via command-line, use the ones from main_conf.json
        table_names = []
        for config in load_configs():
            if config['table_names']:
                table_names.extend(config['table_names'])
    if "table_name" in metafunc.fixturenames:
        metafunc.parametrize("table_name", table_names)


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

    # Determine the table name to use
    table_names = request.param.get('table_names')
    if not table_names:
        table_names = request.config.getoption("table_names")
        if not table_names:
            raise ValueError("Table names must be provided either via command line or JSON configuration")
    config.settings['table_names'] = table_names

    # Load additional settings from YAML file
    base_path = get_team_folder_path()
    team_path, _ = get_team_folder_path_with_key(base_path, request.param['team_key'])
    for table_name in table_names:
        table_config_path = team_path / 'tables' / f"{table_name}.yaml"
        if file_exists(table_config_path):
            table_config = load_yaml_file(table_config_path)
            config.settings[table_name] = table_config
        else:
            raise FileNotFoundError(f"YAML configuration file not found: {table_config_path}")

    # Verify that the configuration is loaded without empty or
    assert config is not None, "Configuration manager should not be None"
    assert hasattr(config, 'settings'), "Configuration manager should have settings attribute"

    yield config

    LOGGER.info(f"Tearing down config for {request.param['team_key']} - {request.param['environment']}")


@pytest.fixture(scope='function')
def etl_db_engine_fixture(config_fixture) -> Engine:
    db_config = config_fixture.settings
    engine = create_db_engine(db_config['etl_db_engine'], db_config)
    yield engine
    close_db_engine(engine)


@pytest.fixture(scope='function')
def stg_client_fixture(config_fixture):
    """
    Fixture to create and return an S3 client for staging data.
    """
    return get_s3_client(config_fixture.settings)
