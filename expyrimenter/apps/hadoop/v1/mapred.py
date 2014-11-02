from .. import Base


class MapRed(Base):
    """HDFS must be online with space available."""
    def __init__(self, job_tracker, task_trackers):
        super().__init__()

        self._jt = job_tracker
        super()._add_master(job_tracker)

        for tt in task_trackers:
            super()._add_slave(tt)

    # Higher level methods with multiple hosts and/or components:

    def start(self):
        self.job_tracker('start')
        super()._wait()
        for host in self._slaves:
            self.task_tracker(host, 'start')

    # Starts a task tracker in a slave
    def add_slave(self, host):
        super()._add_slave(host)
        self.task_tracker(host, 'start')

    def stop(self, host=None):
        if host is None or host == self._jt:
            self.job_tracker('stop', self._jt)

        if host is None:
            hosts = self._slaves
        else:
            hosts = [host]

        for host in hosts:
            self.task_tracker(host, 'stop')

    # Methods with one node and one hadoop component

    def job_tracker(self, action, host=None):
        if host is None:
            host = self._jt
        self._daemon(host, 'hadoop', action, 'jobtracker')

    def task_tracker(self, host, action):
        self._daemon(host, 'hadoop', action, 'tasktracker')
