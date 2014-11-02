from expyrimenter import SSH


class HadoopBase:
    def __init__(self):
        self._slaves = set()

    def set_executor(self, executor):
        self._executor = executor

    def wait(self):
        self._executor.wait()

    def add_slave(self, host):
        self._slaves.add(host)

    def get_slaves(self):
        return self._slaves

    def remove_slaves(self):
        self._slaves.clear()

    def n_slaves(self):
        return len(self._slaves)

    def _execute(self, ssh_opts, cmd, name):
        ssh = SSH(ssh_opts, cmd, name)
        self._executor.run(ssh)

    def _daemon(self, host, daemon, action, component):
        cmd = '%s-daemon.sh %s %s >/dev/null' % (daemon, action, component)
        name = '%s %s in %s' % (action, component, host)
        self._execute(host, cmd, name)
