from expyrimenter import Shell


# For both v1 and v2
class HadoopBase:
    def __init__(s, executor):
        s._exec = executor

        # Set the variables below:
        s.root_dir = None
        s.cfg_dir = None

    # Requires s.cfg_dir, s.root_dir
    def configure(s, host=None):
        hosts = [host] if not host is None else s.hosts
        for host in hosts:
            title = '%s:configure hadoop' % host
            cmd = 'scp -qr %s %s:%s' % (s.cfg_dir, host, s.root_dir)
            shell = Shell(cmd, title)
            s._exec.run(shell)
