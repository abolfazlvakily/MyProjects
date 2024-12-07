a = 1
assert a == 1
assert a == 2, "اگه متن من به خطا خورد این پیام نمایش داده می شود"

import unittest


class SimpleTest(unittest.TestCase):
    """
    حتما هر تست را مستند سازی کن
    """

    def setUp(self):
        pass

    def test_bla_blah(self):
        self.assertTrue()
        self.assertIn()
        self.assertIsInstance()
        self.assertIsNone()

    def tearDown(self):
        pass


# How to Run? --> python -m unittest discover tests/test_sample.py
