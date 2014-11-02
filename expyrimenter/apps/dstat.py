from expyrimenter import SSH, Filesystem
import logging


class Dstat:
    def __init__(s, hosts, executor, base_dir):
        s._hosts = set()
        s._executor = executor
        s._base_dir = base_dir
        s._logger = logging.getLogger('dstat')

        for host in hosts:
            s._hosts.add(host)

    def add_host(s, host):
        s._logger.info('adding %s' % host)
        s._hosts.add(host)

    def setup_hosts(s):
        for host in s._hosts:
            title = '%s:dstat output dir' % host
            Filesystem(s._base_dir, s._executor).mkdir(host, title)

    def start(s, filename, host=None):
        s._logger.info('starting')
        for host in s._host2hosts(host):
            title = '%s:start dstat' % host
            fullname = '%s/%s_%s' % (s._base_dir, host, filename)
            cmd = 'rm -f %s; ' % fullname
            cmd += 'dstat -tcmn --output %s 1 >/dev/null &' % fullname
            s._executor.run(SSH(host, cmd, title))

    def stop(s, host=None):
        s._logger.info('stopping')
        cmd = 'PID=$(ps -eo pid,args | grep "[p]ython.*dstat" | \
              sed "s/^ \+//" | cut -f 1 -d" " | xargs); \
              test -z "$PID" || kill $PID'

        for host in s._host2hosts(host):
            title = '%s:stop dstat' % host
            ssh = SSH(host, cmd, title, stdout=False, stderr=False)
            ssh.ignore_error = True
            s._executor.run(ssh)

    def _host2hosts(s, host):
        return s._hosts if host is None else [host]
