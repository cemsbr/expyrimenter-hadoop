from expyrimenter.apps.hibench.hibench import HiBench


class TeraSortExperiment(HiBench):
    def __init__(self, host, root, hdfs_root, out_dir, executor):
        path = self.name = 'terasort'
        dfs_path = 'Terasort'
        super().__init__(host, root, out_dir, executor, hdfs_root, hdfs_path,
                         path)
