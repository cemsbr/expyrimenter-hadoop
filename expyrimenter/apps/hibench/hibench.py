from expyrimenter import SSH, Filesystem
import logging


class HiBench:

    _REPORT = 'hibench.report'

    # root, out_dir are absolute
    # path, hdfs_path are relative to root, /HiBench/
    def __init__(s, host, root, out_dir, executor, hdfs_root=None,
                 hdfs_path=None, path=None):
        s._root = root
        s._executor = executor
        s._host = host
        s._out_dir = out_dir
        s._logger = logging.getLogger('hibench')

        s._hdfs_root = hdfs_root
        if path is not None:
            s._path = root + '/' + path
        if hdfs_path is not None:
            s._hdfs_path = hdfs_root + hdfs_path

    def set_reduces(s, reduces):
        s._config('NUM_REDS', reduces)

    def set_data_size(s, data_size):
        s._config('DATASIZE', data_size)

    def set_maps(s, maps):
        s._config('NUM_MAPS', maps)

    def prepare(s):
        s._logger.info('preparing experiment')
        cmd = 'bin/prepare.sh'
        s._exec_exp(cmd, 'HiBench: prepare experiment')

    # Needed only once. Creates output dirs
    def setup_host(s):
        Filesystem(s._out_dir).mkdir(s._host)
        cmd = 'ln -nsf %s/%s %s/' % (s._root, HiBench._REPORT, s._out_dir)
        s._exec(cmd, 'Symlinking HiBench report')

    def start(s, filename):
        s._logger.info('running experiment')
        cmd = 'bin/run.sh &> %s/%s' % (s._out_dir, filename)
        s._exec_exp(cmd, 'HiBench: run experiment')

    def clean(s):
        cmd = 'rm -f %s/hibench.report' % s._root
        title = 'HiBench: deleting report'
        s._exec(cmd, title)

    def get_hdfs_path(s):
        return s._hdfs_path

    def _config(s, key, value):
        s._logger.info('setting %s to %s' % (key, value))
        filename = 'conf/configure.sh'
        cmd = 'sed -i "s/^%s=.*/%s=%s/" %s' % (key, key, value, filename)
        title = 'HiBench config: setting %s to %s' % (key, value)
        s._exec_exp(cmd, title)

    # Runs cmd in the host
    def _exec(s, cmd, title):
        ssh = SSH(s._host, cmd, title)
        s._executor.run(ssh)

    # Runs cmd in the host and in the exp path
    def _exec_exp(s, cmd, title):
        cd_cmd = 'cd %s; %s' % (s._path, cmd)
        s._exec(cd_cmd, title)
