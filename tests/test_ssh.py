import unittest
from mock import patch
from expyrimenter import SSH


class TestSSH(unittest.TestCase):

    def test_command_has_params(self):
        params = '-p 2222 user@hostname'
        cmd = SSH(params, '').command
        # self.assertIsNotNone(re.match('.*{!s}'.format(params), cmd))
        self.assertRegex(cmd, '.*{!s}'.format(params))

    def test_command_has_remote_cmd(self):
        remote_cmd = 'echo Hello'
        cmd = SSH('', remote_cmd).command
        # self.assertIsNotNone(re.match('.*{!s}'.format(remote_cmd), cmd))
        self.assertRegex(cmd, '.*{!s}'.format(remote_cmd))

    @patch('expyrimenter.SSH._redirect_outputs', return_value='')
    def test_output_redirection(self, mock):
        SSH('user@host', 'echo Hello')
        self.assertTrue(mock.called)

    @patch('expyrimenter.Shell.has_failed', side_effect=[True, False])
    @patch('expyrimenter.ssh.sleep')
    def test_availability_at_second_attempt(self, sleep_mock, failed_mock):
        SSH.await_availability('')
        self.assertEqual(2, failed_mock.call_count)
        self.assertTrue(sleep_mock.called)

    @patch('expyrimenter.SSH.has_failed', return_value=False)
    @patch('expyrimenter.ssh.sleep')
    def test_availability_at_first_attempt(self, sleep_mock, failed_mock):
        SSH.await_availability('')
        self.assertEqual(1, failed_mock.call_count)
        self.assertFalse(sleep_mock.called)

if __name__ == '__main__':
    unittest.main()
