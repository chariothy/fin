import unittest

from utils import parse_y, parse_ym, parse_ymd

class CoreTestCase(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_parse_y(self):
        self.assertEqual(parse_y('2022'), '2022')
        self.assertEqual(parse_y('2022年'), '2022')
        self.assertEqual(parse_y('2022年1月31日'), '2022')


    def test_parse_ym(self):
        self.assertEqual(parse_ym('2022-01'), '2022-01')
        self.assertEqual(parse_ym('2022-1'), '2022-01')
        self.assertEqual(parse_ym('2022.01'), '2022-01')
        self.assertEqual(parse_ym('2022.1'), '2022-01')
        self.assertEqual(parse_ym('2022年1月'), '2022-01')
        self.assertEqual(parse_ymd('2022年1月31日'), '2022-01-31')

        
    def test_parse_ymd(self):
        self.assertEqual(parse_ymd('2022-01-31'), '2022-01-31')
        self.assertEqual(parse_ymd('2022-1-31'), '2022-01-31')
        self.assertEqual(parse_ymd('2022.01.31'), '2022-01-31')
        self.assertEqual(parse_ymd('2022.1.31'), '2022-01-31')
        self.assertEqual(parse_ymd('2022年1月31日'), '2022-01-31')
        


if __name__ == '__main__':
    unittest.main()