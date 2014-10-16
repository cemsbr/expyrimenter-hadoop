import unittest
from mock import Mock, patch
import subprocess
from subprocess import CalledProcessError
from expyrimenter import Shell


class TestShell(unittest.TestCase):

    cmd = 'test'

    def test_stdout_true_stderr_true(self):
        shell = Shell(TestShell.cmd, stdout=True, stderr=True)
        self.assertEqual(TestShell.cmd, shell.command)

    def test_stdout_true_stderr_false(self):
        shell = Shell(TestShell.cmd, stdout=True, stderr=False)
        expected = TestShell.cmd + ' 2>/dev/null'
        self.assertEqual(expected, shell.command)

    def test_stdout_false_stderr_true(self):
        shell = Shell(TestShell.cmd, stdout=False, stderr=True)
        expected = TestShell.cmd + ' 1>/dev/null'
        self.assertEqual(expected, shell.command)

    def test_stdout_false_stderr_false(self):
        shell = Shell(TestShell.cmd, stdout=False, stderr=False)
        expected = TestShell.cmd + ' 1>/dev/null 2>/dev/null'
        self.assertEqual(expected, shell.command)

    def test_title_not_set_returns_command(self):
        shell = Shell(TestShell.cmd, stdout=True)
        self.assertEqual(TestShell.cmd, shell.title)

    def test_title(self):
        title = TestShell.cmd + 'title'
        shell = Shell(TestShell.cmd, title=title)
        self.assertEqual(title, shell.title)

    def test_run_with_stdout(self):
        shell = Shell(TestShell.cmd, stdout=True, stderr=False)
        expected = 'output'
        with patch('subprocess.check_output', return_value=expected):
            actual = shell.run()
        self.assertEqual(expected, actual)

    def test_run_with_stderr(self):
        shell = Shell(TestShell.cmd, stdout=False, stderr=True)
        expected = 'output'
        with patch('subprocess.check_output', return_value=expected):
            actual = shell.run()
        self.assertEqual(expected, actual)

    def test_run_with_stdout_stderr(self):
        shell = Shell(TestShell.cmd, stdout=True, stderr=True)
        expected = 'output'
        with patch('subprocess.check_output', return_value=expected):
            actual = shell.run()
        self.assertEqual(expected, actual)

    def test_run_without_output(self):
        shell = Shell(TestShell.cmd, stdout=False, stderr=False)
        expected = 0
        with patch('subprocess.call', return_value=expected):
            actual = shell.run()
        self.assertEqual(expected, actual)

    def test_run_exception(self):
        shell = Shell(TestShell.cmd, stdout=True)
        exception = CalledProcessError(returncode=1, cmd='')
        with patch('subprocess.check_output', side_effect=exception):
            self.assertRaises(CalledProcessError, shell.run)

    def test_run_pre_is_called_once(self):
        shell = Shell(TestShell.cmd, stdout=True)
        shell.run_pre = Mock()
        with patch('subprocess.check_output', return_value=''):
            shell.run()
        shell.run_pre.assert_called_once_with()

    def test_run_pos_is_called_once(self):
        shell = Shell(TestShell.cmd, stdout=True)
        shell.run_pos = Mock()
        with patch('subprocess.check_output', return_value=''):
            shell.run()
        shell.run_pos.assert_called_once_with()

    def test_was_successfull_true(self):
        shell = Shell(TestShell.cmd, stdout=True)
        with patch('subprocess.check_output', return_value=''):
            self.assertTrue(shell.was_successful())

    def test_has_successfull_false(self):
        shell = Shell(TestShell.cmd, stdout=True)
        exception = CalledProcessError(returncode=1, cmd='')
        with patch('subprocess.check_output', side_effect=exception):
            self.assertFalse(shell.was_successful())

    def test_has_failed_false(self):
        shell = Shell(TestShell.cmd, stdout=True)
        with patch('subprocess.check_output', return_value=''):
            self.assertFalse(shell.has_failed())

    def test_has_failed_true(self):
        shell = Shell(TestShell.cmd, stdout=True)
        exception = CalledProcessError(returncode=1, cmd='')
        with patch('subprocess.check_output', side_effect=exception):
            self.assertTrue(shell.has_failed())

if __name__ == '__main__':
    unittest.main()
