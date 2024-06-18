import unittest
from custom_conf.conf_manager import ConfManager


class TestConfManager(unittest.TestCase):

    def setUp(self):
        self.conf_manager = ConfManager()

    def test_load(self):
        loader = MockLoader({"key1": "value1"})
        self.conf_manager.load(loader)
        self.assertEqual(self.conf_manager.get("key1"), "value1")

    def test_get_default(self):
        self.assertEqual(self.conf_manager.get("nonexistent_key", "default_value"), "default_value")

    def test_set_and_get(self):
        self.conf_manager.set("key2", "value2")
        self.assertEqual(self.conf_manager.get("key2"), "value2")

    def test_clear(self):
        self.conf_manager.set("key3", "value3")
        self.conf_manager.clear()
        self.assertIsNone(self.conf_manager.get("key3"))


class MockLoader:
    def __init__(self, data):
        self.data = data

    def load(self):
        return self.data


if __name__ == "__main__":
    unittest.main()
