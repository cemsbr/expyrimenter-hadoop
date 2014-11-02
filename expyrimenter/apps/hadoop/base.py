from expyrimenter import SSH


# TODO s.executor -> s._executor
# Manage masters, slaves + executor shortcuts
class Base:
    def __init__(s):
        s._slaves = []
        s._masters = []

        # Set the variables below:
        s.executor = None

    def get_hosts(s):
        return s._masters + s._slaves

    @property
    def slaves(s):
        return s._slaves

    def _add_master(s, host):
        s._masters.append(host)

    def _add_slave(s, host):
        s._slaves.append(host)

    def _exec(s, runnable):
        s.executor.run(runnable)

    def _wait(s):
        s.executor.wait()

    def _daemon(s, host, daemon, action, component):
        cmd = '%s-daemon.sh %s %s >/dev/null' % (daemon, action, component)
        title = '%s %s in %s' % (action, component, host)
        ssh = SSH(host, cmd, title)
        s._exec(ssh)
