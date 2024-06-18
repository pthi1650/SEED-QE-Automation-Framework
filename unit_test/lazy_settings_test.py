import unittest
from custom_conf.lazy_settings import LazySettings


class TestLazySettings(unittest.TestCase):

    def setUp(self):
        self.lazy_settings = LazySettings()

    def test_getattr(self):
        self.lazy_settings.set('key1', 'value1')
        self.assertEqual(self.lazy_settings.get('key1'), 'value1')

    def test_setattr(self):
        self.lazy_settings.set('key2', 'value2')
        self.assertEqual(self.lazy_settings.get('key2'), 'value2')

    def test_setup(self):
        self.lazy_settings._setup()
        self.assertIsNotNone(self.lazy_settings._conf_manager)


if __name__ == "__main__":
    unittest.main()
