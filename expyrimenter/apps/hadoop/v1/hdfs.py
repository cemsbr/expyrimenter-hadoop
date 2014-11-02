from .. import HDFSBase


class HDFS(HDFSBase):
    def format_name_node(self):
        super()._format_name_node('hadoop')
