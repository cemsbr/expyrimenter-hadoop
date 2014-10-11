SSH
===

Extends :class:`expyrimenter.Shell` to run commands in a remote host.
One parameter was added (the first one), which is the shell SSH arguments
(e.g. ``hostname``, ``user@hostname``, ``-p 2222 user@hostname``, etc).

  >>> from expyrimenter import SSH
  >>> SSH('localhost', 'echo Hello', stdout=True).run()
  'Hello'
  >>> # You can use try/except
  >>> from subprocess import CalledProcessError
  >>> try:
  >>>     SSH('localhost','wrongcommand').run()
  >>> except CalledProcessError as e:
  >>>     print("Failed: %s" % e.output)
  Failed: bash: wrongcommand: command not found

.. automodule:: expyrimenter

.. autoclass:: SSH
    :members:
    :show-inheritance:
