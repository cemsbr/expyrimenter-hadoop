from expyrimenter import SSH
from datetime import datetime
import logging


class NTP:
    def __init__(self, host):
        self._log = logging.getLogger('ntp')
        self._host = host

    def run(self):
        self._set_time()

        if not self._is_ntp_installed():
            self._install_ntp()
        else:
            self._log.debug('NTP already installed in ' + self._host)
            self._restart()

    def _is_ntp_installed(self):
        cmd = 'dpkg-query -W ntp'
        return SSH(self._host, cmd).run() == 0

    def _set_time(self):
        now = datetime.now()
        now_str = now.strftime('%m%d%H%M.%S')
        cmd = 'date ' + now_str
        title = 'Set time on "%s" with "%s"' % (self._host, cmd)
        SSH(self._host, cmd, title).run()

    def _install_ntp(self):
        cmd = 'aptitude -y install ntp'
        title = 'Install ntp on ' + self._host
        SSH(self._host, cmd, title).run()

    def _restart(self):
        cmd = '/etc/init.d/ntp restart'
        title = 'restarting'
        SSH(self._host, cmd, title).run()
