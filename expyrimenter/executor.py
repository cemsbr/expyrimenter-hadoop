from concurrent.futures import ThreadPoolExecutor
from . import Config
import concurrent.futures
import logging
from subprocess import CalledProcessError


class Executor:
    def __init__(s, max_workers=None, cls=None):
        if max_workers is None:
            max_workers = int(Config('workers').get('max', 100))

        if cls is None:
            cls = ThreadPoolExecutor

        s._executor = cls(max_workers)
        s._future_to_runnable = {}  # for submitted runnables
        s._future_to_title = {}     # for submitted functions
        s.results = []
        s._log = logging.getLogger('executor')

    def run_fn(s, fn, title, *args, **kwargs):
        """Submits a function to the PoolExecutor.

        If you only want to submit a function, use this method.
        It is not mandatory to call wait() or shutdown() later.
        """
        future = s._executor.submit(fn, *args, **kwargs)
        s._future_to_title[future] = title
        future.add_done_callback(s._done_fn)

        return future

    def run(s, runnable, *args, **kwargs):
        """Submits Runnable objects to the PoolExector.

        Using Runnable objects, you have more control and verbosity.
        Useful when things go wrong (we know it always happens).
        If you are in a hurry, submit a function using :py:func:`run_fn`.
        """
        future = s._executor.submit(runnable.run, *args, **kwargs)
        s._future_to_runnable[future] = runnable
        future.add_done_callback(s._done_runnable)

        return future

    def _done_fn(s, future):
        title = s._future_to_title[future]
        s._done(future, title)
        del s._future_to_title[future]

    def _done_runnable(s, future):
        runnable = s._future_to_runnable[future]
        title = runnable.title
        s._done(future, title)
        del s._future_to_runnable[future]

    def _done(s, future, title):
        if title is None:
            title = 'no given title'
        result = None

        if future.cancelled():
            s._log.error('cancelled:' + title)
        else:
            ex = future.exception()
            if ex is None:
                s._log.debug('success:%s' % title)
                result = future.result()
            else:
                if type(ex) is CalledProcessError:
                    msg = 'CalledProcessError:'
                    if title != ex.cmd:
                        msg += '\n\tTitle   : %s' % title
                    msg += '\n\tCmd     : %s' % ex.cmd
                    msg += '\n\tReturned: %s' % ex.returncode
                    msg += '\n\tOutput  : %s' % ex.output
                    result = ex.output
                else:
                    msg = 'exception:%s:%s' % (title, ex)
                    result = future.result()
                s._log.error(msg)

        s.results.append(result)

    def wait(s):
        futures = list(s._future_to_runnable.keys())
        futures += list(s._future_to_title.keys())

        concurrent.futures.wait(futures)

    def shutdown(s):
        s._executor.shutdown()
        s._future_to_runnable.clear()
        s.results.clear()
