import unittest
from mock import Mock, patch
from expyrimenter import Executor
from expyrimenter.runnable import Runnable
from subprocess import CalledProcessError
from concurrent.futures import ThreadPoolExecutor
import re


class TestExecutor(unittest.TestCase):

    output = 'TestExecutor output'
    outputs = ['TestExecutor 1', 'TestExecutor 2']

    def test_runnable_output(self):
        executor = Executor()

        with patch.object(Runnable, 'run', return_value=TestExecutor.output):
            executor.run(Runnable())
        executor.wait()
        results = executor.results

        self.assertEqual(1, len(results))
        self.assertEqual(TestExecutor.output, results[0])

    def test_runnable_outputs(self):
        executor = Executor()
        runnable = Runnable()

        with patch.object(Runnable, 'run', side_effect=TestExecutor.outputs):
            executor.run(runnable)
            executor.run(runnable)
        executor.wait()
        results = executor.results

        self.assertListEqual(TestExecutor.outputs, results)

    def test_function_output(self):
        executor = Executor()
        executor.run_function(test_function)
        executor.wait()
        output = executor.results[0]
        self.assertEqual(TestExecutor.output, output)

    def test_function_outputs(self):
        executor = Executor()
        runnable = Runnable()

        with patch.object(Runnable, 'run', side_effect=TestExecutor.outputs):
            executor.run(runnable)
            executor.run(runnable)
        executor.wait()
        results = executor.results

        self.assertListEqual(TestExecutor.outputs, results)

    def test_against_runnable_memory_leak(self):
        executor = Executor()
        with patch.object(Runnable, 'run'):
            executor.run(Runnable())
        executor.wait()
        self.assertEqual(0, len(executor._future_runnables))

    def test_against_function_memory_leak(self):
        executor = Executor()
        executor.run_function(test_function)
        executor.wait()
        self.assertEqual(0, len(executor._function_titles))

    def test_if_shutdown_shutdowns_executor(self):
        executor = Executor()
        executor._executor = Mock()
        executor.shutdown()
        executor._executor.shutdown.called_once_with()

    def test_if_shutdown_clears_function_resources(self):
        executor = Executor()
        executor._function_titles = Mock()
        executor.shutdown()
        executor._function_titles.clear.assert_called_once_with()

    def test_if_shutdown_clears_runnable_resources(self):
        executor = Executor()
        executor._future_runnables = Mock()
        executor.shutdown()
        executor._future_runnables.clear.assert_called_once_with()

    def test_exception_logging(self):
        executor = Executor()
        executor._log = Mock()

        with patch.object(Runnable, 'run', side_effect=Exception):
            executor.run(Runnable)
        executor.wait()

        self.assertEqual(1, executor._log.error.call_count)

    @patch.object(ThreadPoolExecutor, '__init__', return_value=None)
    def test_specified_max_workers(self, pool_mock):
        max = 42
        Executor(max)
        pool_mock.assert_called_once_with(42)

    def test_calledprocesserror_logging(self):
        executor = Executor()
        executor._log = Mock()
        exception = CalledProcessError(returncode=1, cmd='command')

        with patch.object(Runnable, 'run', side_effect=exception):
            executor.run(Runnable)
        executor.wait()

        self.assertEqual(1, executor._log.error.call_count)

    def test_if_logged_title_is_hidden_if_it_equals_command(self):
        command = 'command'

        runnable = Runnable()
        runnable.title = command
        exception = CalledProcessError(returncode=1, cmd=command)
        runnable.run = Mock(side_effect=exception)

        executor = Executor()
        executor._log = Mock()
        executor.run(runnable)
        executor.wait()

        executor._log.error.assert_called_once_with(Matcher(has_not_title))

    def test_logged_title_when_it_differs_from_command(self):
        command, title = 'command', 'title'

        runnable = Runnable()
        runnable.title = title
        exception = CalledProcessError(returncode=1, cmd=command)
        runnable.run = Mock(side_effect=exception)

        executor = Executor()
        executor._log = Mock()
        executor.run(runnable)
        executor.wait()

        executor._log.error.assert_called_once_with(Matcher(has_title))


def has_title(msg):
    return re.match("(?ims).*Title", msg) is not None


def has_not_title(msg):
    return re.match("(?ims).*Title", msg) is None


class Matcher:

    def __init__(self, compare):
        self.compare = compare

    def __eq__(self, msg):
        return self.compare(msg)


def test_function():
    return TestExecutor.output

if __name__ == '__main__':
    unittest.main()
