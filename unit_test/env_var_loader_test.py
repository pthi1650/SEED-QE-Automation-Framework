import unittest
from unittest.mock import patch
from custom_conf.loaders.env_var_loader import EnvVarLoader


class TestEnvVarLoader(unittest.TestCase):

    @patch.dict('os.environ', {'SEED_CONF_KEY1': 'value1', 'OTHER_KEY': 'value2'})
    def test_load(self):
        loader = EnvVarLoader()
        result = loader.load()
        self.assertEqual(result, {'SEED_CONF_KEY1': 'value1'})


if __name__ == "__main__":
    unittest.main()
