import unittest
from unittest.mock import patch
from custom_conf.loaders.remote_loader import RemoteLoader


class TestRemoteLoader(unittest.TestCase):

    @patch('custom_conf.loaders.remote_loader.RemoteSource')
    def test_load(self, mock_remote_src):
        mock_remote_source = mock_remote_src.return_value
        mock_remote_source.load.return_value = {'key1': 'value1'}
        loader = RemoteLoader('vault')
        result = loader.load()
        self.assertEqual(result, {'key1': 'value1'})

    def test_load_unsupported_source(self):
        loader = RemoteLoader('unsupported_source')
        with self.assertRaises(ValueError):
            loader.load()


if __name__ == "__main__":
    unittest.main()
