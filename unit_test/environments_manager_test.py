import unittest
from unittest.mock import patch
from pathlib import Path
import shutil
from custom_conf.conf_manager import ConfManager
from custom_conf.environments_manager import EnvironmentsManager


class TestEnvironmentsManager(unittest.TestCase):
    sandbox = None
    SETTINGS_FILENAME = 'settings.toml'

    @classmethod
    def setUpClass(cls):
        cls.sandbox = Path(__file__).parent / 'sandbox/unittesting'
        cls.sandbox.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.sandbox)

    def setUp(self):
        self.conf_manager = ConfManager()
        self.env_manager = EnvironmentsManager(self.conf_manager)

    @staticmethod
    def create_dummy_files(team, env):
        team_dir = TestEnvironmentsManager.sandbox / team / env
        team_dir.mkdir(parents=True, exist_ok=True)
        (team_dir / TestEnvironmentsManager.SETTINGS_FILENAME).write_text('key = "value"')

    @patch("custom_conf.environments_manager.construct_path")
    def test_switch_environment(self, mock_construct_path):
        team = 'test_team'
        env = 'dev'
        self.create_dummy_files(team, env)
        mock_construct_path.return_value = (TestEnvironmentsManager.sandbox / team / env
                                            / TestEnvironmentsManager.SETTINGS_FILENAME)
        self.env_manager.switch_environment(team, env)
        self.assertEqual(self.conf_manager.get('key'), 'value')

    @patch("custom_conf.environments_manager.construct_path")
    def test_switch_environment_file_not_found(self, mock_construct_path):
        mock_construct_path.return_value = (TestEnvironmentsManager.sandbox / 'invalid_team' / 'invalid_env'
                                            / TestEnvironmentsManager.SETTINGS_FILENAME)
        with self.assertRaises(FileNotFoundError):
            self.env_manager.switch_environment('invalid_team', 'invalid_env')


if __name__ == "__main__":
    unittest.main()
