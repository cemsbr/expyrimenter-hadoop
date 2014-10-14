import unittest
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

if __name__ == '__main__':
    unittest.main()
