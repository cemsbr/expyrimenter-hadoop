Shell
=====

This is one of the innermost classes.
The parameters are passed to the constructor and then
you can call :func:`run() <expyrimenter.Shell.run>` or
use an :class:`Executor <expyrimenter.Executor>` instance to run in parallel.

  >>> from expyrimenter import Shell
  >>> Shell('echo Hello', stdout=True).run()
  'Hello'
  >>> # You can use try/except
  >>> from subprocess import CalledProcessError
  >>> try:
  >>>     Shell('wrongcommand').run()
  >>> except CalledProcessError as e:
  >>>     print("Failed: %s" % e.output)
  Failed: /bin/sh: 1: wrongcommand: not found

.. automodule:: expyrimenter

.. autoclass:: Shell
    :members:
    :show-inheritance:
