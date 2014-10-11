from os.path import join, expanduser, exists
from configparser import ConfigParser
import logging


class Config:
    """Consider the following expyrimenter.ini:
    [my_section]
    var1 = one

    >>> from expyrimenter import Config
    >>> config = Config('my_section')
    >>> print(config.get('var1'))
    'one'
    >>> print(config.get('var2', 'default_value'))
    'default_value'

    """
    _cfg_file = join(expanduser('~'), '.expyrimenter', 'config.ini')
    _cfg = None
    _logger = logging.getLogger('config')

    def __init__(self, section):
        if Config._cfg is None:
            Config._cfg = ConfigParser()
            self._check_file()
            Config.read()

        if section in Config._cfg.sections():
            self._section = Config._cfg[section]
        else:
            self._section = {}

    def get(self, key, default=None):
        return self._section.get(key, default)

    @staticmethod
    def read():
        Config._cfg.read(Config._cfg_file)
        Config._logger.debug('%s read' % Config._cfg_file)

    def _check_file(self):
        if not exists(Config._cfg_file):
            self._logger.error('%s not found.' % self._cfg_file)
