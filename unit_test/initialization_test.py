import unittest
from pathlib import Path
from unittest.mock import patch
import shutil
from custom_conf.initialization import load_local_config, load_env_vars, load_remote_secrets
from custom_conf.conf_manager import ConfManager
from custom_conf.environments_manager import EnvironmentsManager


class TestInitialization(unittest.TestCase):
    sandbox = None

    @classmethod
    def setUpClass(cls):
        cls.sandbox = Path(__file__).parent / 'scratch_unittest_folder/unittesting'
        cls.sandbox.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.sandbox)

    def setUp(self):
        self.conf_manager = ConfManager()
        self.env_manager = EnvironmentsManager(self.conf_manager)

    @staticmethod
    def create_dummy_file(path, content='key = "value"'):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    @patch('custom_conf.initialization.load_layered_settings')
    @patch('custom_conf.initialization.Path.exists', return_value=True)
    def test_load_local_config(self, mock_path_exists, mock_load_layered_settings):
        mock_load_layered_settings.return_value = {'key1': 'value1'}
        dummy_file = TestInitialization.sandbox / 'settings.toml'
        self.create_dummy_file(dummy_file)
        load_local_config(self.env_manager, dummy_file.parent, 'dev')
        self.assertEqual(self.conf_manager.get('key1'), 'value1')

    @patch.dict('os.environ', {'SEED_CONF_KEY2': 'value2'})
    def test_load_env_vars(self):
        load_env_vars(self.conf_manager)
        self.assertEqual(self.conf_manager.get('SEED_CONF_KEY2'), 'value2')

    @patch('custom_conf.initialization.RemoteSource.load', return_value={'key3': 'value3'})
    def test_load_remote_secrets(self, mock_load):
        dummy_file = TestInitialization.sandbox / 'settings.toml'
        self.create_dummy_file(dummy_file)
        secrets_path = TestInitialization.sandbox / 'relative/path/.secrets.toml'
        secrets_path.parent.mkdir(parents=True, exist_ok=True)
        load_remote_secrets(self.conf_manager, TestInitialization.sandbox,
                            'relative/path', 'dev', 'vault',
                            False)
        self.assertEqual(self.conf_manager.get('key3'), 'value3')


if __name__ == "__main__":
    unittest.main()
