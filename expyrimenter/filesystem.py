import os
from . import Shell, SSH

class FilesystemException(Exception):
    pass

class Filesystem:
    def __init__(s, path, executor=None):
        s._path = path
        s._executor = executor

    def mkdir(s, host=None, title=None):
        if host is None:
            s.mkdir_local(title)
        else:
            s.mkdir_remote(host, title)

    def mkdir_local(s, title):
        if not os.path.lexists(s._path):
            # Must not use quotes in dir name - no ~/ expansion
            cmd = 'test -d %s || mkdir %s' % (s._path, s._path)
            s._executor.run(Shell(cmd, title))
        elif not os.path.isdir(s._path):
            msg = '"%s" is not a directory' % path
            raise FilesystemException(msg)

    def mkdir_remote(s, host, title):
        cmd = 'test -d %s || mkdir -p %s' % (s._path, s._path)
        if title is None:
            title = '%s:create dir "%s"' % (host, s._path)
        ssh = SSH(host, cmd, title)

        if s._executor is None:
            ssh.run()
        else:
            s._executor.run(ssh)

