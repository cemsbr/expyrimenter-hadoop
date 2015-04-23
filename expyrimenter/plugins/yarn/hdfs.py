from expyrimenter.core import Executor, SSH
from shlex import quote


class HDFS:
    def __init__(self, home, name_node, data_nodes=None, executor=None):
        if data_nodes is None:
            data_nodes = []
        if executor is None:
            executor = Executor()

        self._home = home
        self._master = name_node
        self.logger_name = 'hdfs'
        self.executor = executor
        self._slaves = data_nodes

    def set_slaves(self, hosts):
        self._slaves = hosts
        if hosts:
            self._write_slaves_file()

    def start(self):
        """Starts HDFS master and slaves."""
        self._ssh_master('start-dfs.sh', 'start dfs')

    def stop(self):
        self._ssh_master('stop-dfs.sh', 'stop dfs')

    def format(self, tmp_folder):
        rm_dfs = self._rm_dfs_cmd(tmp_folder)
        cmd = rm_dfs + ' && hdfs namenode -format'
        self._ssh_master(cmd, title='format namenode')
        self.rm_dfs(tmp_folder, self._slaves)

    def rm_dfs(self, tmp_folder, hosts):
        cmd = self._rm_dfs_cmd(tmp_folder)
        self._ssh_hosts(cmd, 'rm dfs folder', hosts)

    def _rm_dfs_cmd(self, tmp_folder):
        return 'rm -rf ' + quote(tmp_folder + '/dfs')

    def upload(self, host, src, dst):
        cmd = 'hadoop fs -copyFromLocal {} {}'.format(src, dst)
        ssh = SSH(host, cmd,
                  title='upload ' + src,
                  logger_name=self.logger_name)
        self.executor.run(ssh)

    def put_from_pipe(self, host, pipe_in, hdfs_path, replication=3):
        cmd = '{} | hadoop fs -D dfs.replication={} -put - hdfs://{}/{}'.format(
            pipe_in, replication, self._master, hdfs_path)
        title = 'pipe to ' + hdfs_path
        ssh = SSH(host, cmd, title=title, logger_name=self.logger_name)
        self.executor.run(ssh)

    def clean_logs(self, hosts=None):
        cmd = 'rm -rf {}/logs/*'.format(self._home)
        self._ssh_hosts(cmd, 'clean logs -', hosts)

    def save_block_locations(self, hdfs_path, filename):
        cmd = 'hdfs fsck {} -files -blocks -locations >{}'.format(
            quote(hdfs_path), quote(filename))
        title = 'block locations'
        ssh = SSH(self._master, cmd,
                  title=title,
                  stdout=True,
                  logger_name=self.logger_name)
        self.executor.run(ssh)

    def _write_slaves_file(self):
        filename = quote(self._home + '/etc/hadoop/slaves')
        cmd = '>{}; for slave in {}; do echo $slave >>{}; done'.format(
            filename, ' '.join(self._slaves), filename)
        self._ssh_master(cmd, 'slaves file')

    def _ssh_master(self, cmd, title):
        ssh = SSH(self._master, cmd, title=title, logger_name=self.logger_name)
        self.executor.run(ssh)

    def _ssh_hosts(self, cmd, title, hosts=None):
        if hosts is None:
            hosts = [self._master] + self._slaves
        for host in hosts:
            host_title = '{} {}'.format(title, host)
            ssh = SSH(host, cmd,
                      title=host_title,
                      logger_name=self.logger_name)
            self.executor.run(ssh)
