from expyrimenter import SSH
from . import Base
import logging


class HDFSBase(Base):
    def __init__(self, name_node, data_nodes):
        super().__init__()
        self._nn = name_node

        super()._add_master(name_node)
        for dn in data_nodes:
            super()._add_slave(dn)

    def start(self):
        self.name_node('start')
        super()._wait()
        for host in self._slaves:
            self.data_node(host, 'start')

    # Starts a data node in a slave
    def add_slave(self, host):
        super()._add_slave(host)
        self.data_node(host, 'start')

    def stop(self, host=None):
        if host is None or host == self._nn:
            self.name_node('stop', self._nn)

        if host is None:
            hosts = self._slaves
        else:
            hosts = [host]

        for host in hosts:
            self.data_node(host, 'stop')


    def name_node(self, action, host=None):
        if host is None:
            host = self._nn
        super()._daemon(host, 'hadoop', action, 'namenode')

    def data_node(self, host, action):
        super()._daemon(host, 'hadoop', action, 'datanode')

    def _format_name_node(self, hadoop_exec):
        logging.getLogger('hdfs').info('Formatting name node')
        cmd = '%s namenode -format >/dev/null' % hadoop_exec
        ssh = SSH(self._nn, cmd, 'format namenode', True)
        super()._exec(ssh)
