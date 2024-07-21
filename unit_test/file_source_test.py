import unittest
from pathlib import Path
from unittest.mock import patch, mock_open
import shutil
from custom_conf.config_sources.file_source import LocalSource


class TestLocalSource(unittest.TestCase):
    sandbox = None

    @classmethod
    def setUpClass(cls):
        cls.sandbox = Path(__file__).parent / 'scratch_unittest_folder/unittesting'
        cls.sandbox.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.sandbox)

    def setUp(self):
        self.file_path = TestLocalSource.sandbox / 'settings.toml'
        self.create_dummy_file(self.file_path)

    @staticmethod
    def create_dummy_file(path, content='key = "value"'):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    @patch('utils.commons.file_util.load_toml_file', return_value={'key': 'value'})
    def test_load(self, mock_load_toml_file):
        source = LocalSource(self.file_path)
        result = source.load()
        self.assertEqual(result, {'key': 'value'})

    def test_load_invalid_file(self):
        invalid_path = TestLocalSource.sandbox / 'invalid_settings.toml'
        source = LocalSource(invalid_path)
        with self.assertRaises(FileNotFoundError):
            source.load()


if __name__ == "__main__":
    unittest.main()
