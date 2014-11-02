from expyrimenter.apps.hadoop import HadoopBase


class HadoopYarn(HadoopBase):
    def __init__(self, resource_manager, history_server, node_managers):
        super().__init__()
        self._rm, self._hs = resource_manager, history_server
        for nm in node_managers:
            super().add_slave(nm)

    # Higher level methods with multiple hosts and/or components

    def start(self):
        self.resource_manager('start')
        self._executor.wait()
        for host in self._slaves:
            self.node_manager(host, 'start')
        self.history_server('start')

    def stop(self):
        for host in self._slaves:
            self.node_manager(host, 'stop')
        super().remove_slaves()
        self.history_server('stop')
        self.resource_manager('stop')

    def add_slave(self, host):
        super().add_slave(host)
        self.node_manager(host, 'start')

    def get_all_hosts(self):
        all_hosts = super().get_slaves()
        all_hosts |= set([self._rm, self._hs])
        return all_hosts

    # Methods with one node and one hadoop component

    def resource_manager(self, action):
        self._daemon(self._rm, 'yarn', action, 'resourcemanager')

    def node_manager(self, host, action):
        self._daemon(host, 'yarn', action, 'nodemanager')

    def history_server(self, action):
        self._daemon(self._hs, 'mr-jobhistory', action, 'historyserver')
