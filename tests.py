import FIX44
import unittest
from FIX44 import SOH
from Message import make_pair


class TestCalculations(unittest.TestCase):
    def testSpread(self):
        self.assertEqual(FIX44.calculate_spread('113', '113.015', 2), 15)
        self.assertEqual(FIX44.calculate_spread('1.09553', '1.09553', 4), 0)
        self.assertEqual(FIX44.calculate_spread('9.59', '10', 1), 41)
        self.assertEqual(FIX44.calculate_spread('113.1', '113.2', 2), 100)

    def testPipValue(self):
        self.assertEqual(FIX44.calculate_pip_value('19.00570', 100000, 4), '0.52616')
        self.assertEqual(FIX44.calculate_pip_value('1.3348', 100000, 4), '7.49176')
        self.assertEqual(FIX44.calculate_pip_value('112.585', 10000, 2), '0.88822')

    def test_commission(self):
        self.assertEqual(FIX44.calculate_commission(10000, 1, 0.000030), 0.6)

    def test_make_valid_tuple(self):
        self.assertEqual(
            make_pair(('first', 'second')),
            'first=second{}'.format(SOH)
        )


if __name__ == '__main__':
    unittest.main()
