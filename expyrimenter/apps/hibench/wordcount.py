from expyrimenter.apps.hibench.hibench import HiBench


class WordCountExperiment(HiBench):
    def __init__(self, host, root, out_dir, executor, hdfs_root=None):
        path = self.name = 'wordcount/'
        hdfs_path = 'Wordcount/'
        super().__init__(host, root, out_dir, executor, hdfs_root, hdfs_path,
                         path)
