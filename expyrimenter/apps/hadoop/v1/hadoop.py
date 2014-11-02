from . import HDFS, MapRed
from .. import HadoopBase
from expyrimenter import SSH
import logging


class Hadoop(HadoopBase):
    def __init__(s, nn, jt, slaves, executor):
        super().__init__(executor)
        s.dfs = HDFS(nn, slaves)
        s._map_red = MapRed(jt, slaves)

        s.dfs.executor = executor
        s._map_red.executor = executor

        # Set the variables below:
        s.tmp_dir = None

        s._logger = logging.getLogger('hadoop')

    def setup_hosts(s, out_dir):
        title = '%s:hadoop b4_all'
        target = '%s/logs' % s.root_dir
        link_dir = '%s/hadoop/' % out_dir
        link = '%s/logs_%%s' % link_dir
        cmd = 'test -d %s || mkdir -p %s' % (link_dir, link_dir)
        cmd += ';test -d %s || mkdir -p %s' % (s.tmp_dir, s.tmp_dir)
        cmd += '; ln -nsf %s %s' % (target, link)

        for host in s.hosts:
            ssh = SSH(host, cmd % host, title % host)
            s._exec.run(ssh)

    def start(s):
        msg = 'starting Hadoop with %d slave(s)' % len(s.slaves)
        s._logger.info(msg)

        s.dfs.start()
        s._exec.wait()
        s._map_red.start()

    # Initiate slave services
    def add_slave(s, host):
        s._logger.info('adding slave %s' % host)
        s.dfs.add_slave(host)
        s._map_red.add_slave(host)

    def stop(s, host=None):
        s._logger.info('stopping %d hosts' % (len(s.slaves) + 1))
        s._map_red.stop(host)
        s.dfs.stop(host)

    def reformat_name_node(s):
        title = '%s:erasing hadoop tmp folder'
        cmd = 'rm -rf %s; mkdir -p %s' % (s.tmp_dir, s.tmp_dir)
        for host in s.hosts:
            ssh = SSH(host, cmd, title % host)
            s._exec.run(ssh)

        s._exec.wait()
        s.dfs.format_name_node()

    @property
    def slaves(s):
        return s.dfs.slaves

    # Returns a sorted unique list of all masters and slaves
    @property
    def hosts(s):
        hosts = s.dfs.get_hosts() + s._map_red.get_hosts()
        return s._unique_sort(hosts)

    # Requires s.tmp_dir, s.root_dir
    # TODO maybe move to parent, depends on logs/*
    def clean(s, host=None):
        """Make sure that Hadoop is stopped."""
        hosts = [host] if host is not None else s.hosts
        for host in hosts:
            cmd = 'rm -rf %s %s/logs/*' % (s.tmp_dir, s.root_dir)
            title = '%s:clean hadoop' % host
            ssh = SSH(host, cmd, title)
            s._exec.run(ssh)

    def _unique_sort(s, a_list):
        uniq = list(set(a_list))
        uniq.sort()
        return uniq
