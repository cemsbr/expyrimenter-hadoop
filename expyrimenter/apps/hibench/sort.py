from expyrimenter.apps.hibench.hibench import HiBench


class SortExperiment(HiBench):
    def __init__(self, host, root, out_dir, executor, hdfs_root=None):
        path = self.name = 'sort/'
        hdfs_path = 'Sort/'
        super().__init__(host, root, out_dir, executor, hdfs_root, hdfs_path,
                         path)
