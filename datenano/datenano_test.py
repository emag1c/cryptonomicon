import unittest
import math
from datenano import datetime, MINUTE


class DateNanoTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.test_date = datetime(2020, 1, 2, 3, 4, 5, 6)

    def test_ceil(self):
        rounded_up = self.test_date.ceil(MINUTE)
        rounded_up_actual = datetime(2020, 1, 2, 3, 5)
        print(f"{rounded_up.timestamp_nano()} ? {rounded_up_actual.timestamp_nano()}")
        self.assertEqual(rounded_up.timestamp_nano(), rounded_up_actual.timestamp_nano())

    def test_floor(self):
        rounded_up = self.test_date.floor(MINUTE)
        rounded_up_actual = datetime(2020, 1, 2, 3, 4)
        print(f"{rounded_up.timestamp_nano()} ? {rounded_up_actual.timestamp_nano()}")
        self.assertEqual(rounded_up.timestamp_nano(), rounded_up_actual.timestamp_nano())


if __name__ == '__main__':
    unittest.main()
