import unittest
from unittest import mock
import main


class MainTest(unittest.TestCase):

    def setUp(self) -> None:

        self.main = main.Main()

    @mock.patch('main.input', create=True)
    def test_run_program(self, mocked_input):

        mocked_input.side_effect = ['country', 'MX', 'pm1,pm10,pm25,um010,um100', 'overwrite']

        self.assertEqual(self.main.run_program(), 0)

    @mock.patch('main.input', create=True)
    def test_run_program_fail(self, mocked_input):
        mocked_input.side_effect = ['country', 'MXN', 'pm1,pm10,pm25,um010,um100', 'write']

        self.assertEqual(self.main.run_program(), -1)
