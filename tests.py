import FIX44
import unittest


class TestCalculations(unittest.TestCase):
    def testSpread(self):
        self.assertEqual(FIX44.calculate_spread('113', '113.015'), 15)
        self.assertEqual(FIX44.calculate_spread('1.09553', '1.09553'), 0)
