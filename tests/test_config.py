import unittest
from mock import patch
from expyrimenter import Config
from configparser import ConfigParser


class TestConfig(unittest.TestCase):

    section, key, value = None, None, None
    nonexistent = 'doesnotexist'

    @classmethod
    def setUpClass(cls):
        parser = ConfigParser()
        parser.read(Config._default_ini)
        TestConfig.section = parser.sections()[0]
        sections = parser[TestConfig.section].keys()
        TestConfig.key = list(sections)[0]
        TestConfig.value = parser[TestConfig.section][TestConfig.key]

    def test_verify_assumptions(self):
        parser = ConfigParser()
        parser.read(Config._default_ini)
        section = parser[TestConfig.section]

        self.assertNotIn(TestConfig.nonexistent, parser.sections())
        self.assertNotIn(TestConfig.nonexistent, section.keys())

    def test_default_ini_if_user_file_does_not_exist(self):
        with patch.object(Config, 'user_ini', TestConfig.nonexistent):
            cfg = Config(TestConfig.section)
        self.assertIsNotNone(cfg.get(TestConfig.key))

    def test_nonexistent_section(self):
        cfg = Config(TestConfig.nonexistent)
        self.assertIsNone(cfg.get('test'))

    def test_nonexistent_key(self):
        cfg = Config(TestConfig.section)
        self.assertIsNone(cfg.get(TestConfig.nonexistent))

    def test_no_default_value_for_existent_key(self):
        cfg = Config(TestConfig.section)
        value = cfg.get(TestConfig.key, TestConfig.nonexistent)
        self.assertEqual(TestConfig.value, value)

    def test_default_value_for_nonexistent_key(self):
        cfg = Config(TestConfig.section)
        default = 'default'
        value = cfg.get(TestConfig.nonexistent, default)
        self.assertIs(default, value)

if __name__ == '__main__':
    unittest.main()
