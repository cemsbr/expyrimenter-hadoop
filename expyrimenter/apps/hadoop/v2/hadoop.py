from expyrimenter.apps.hadoop import HadoopBase
from expyrimenter import SSH
import os


class Hadoop:
    def __init__(self, dfs, yarn, executor):
        self._executor = executor
        self._dfs = dfs
        self._yarn = yarn

        dfs.set_executor(executor)
        yarn.set_executor(executor)

    def start(self):
        self._dfs.start()
        self._executor.wait()
        self._yarn.start()

    def stop(self):
        self._dfs.stop()
        self._yarn.stop()

    # TODO After stopping, there are no more slaves to be cleared.
    def clear(self, tmp, home):
        """Make sure that Hadoop is stopped."""
        for host in self.get_all_hosts():
            ssh = SSH(host, 'rm -rf %s %s/logs/*' % (tmp, home))
            self._executor.run(ssh)

    def get_all_hosts(self):
        all_hosts = self._yarn.get_all_hosts()
        all_hosts |= self._dfs.get_all_hosts()
        return all_hosts

    def add_slave(self, host):
        self._dfs.add_slave(host)
        self._yarn.add_slave(host)

    def n_slaves(self):
        return self._dfs.n_slaves()
