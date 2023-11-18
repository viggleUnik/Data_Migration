import unittest
import scripts.datagen.generate_data as gn

class test_data_generator(unittest.TestCase):
    def gen_data_fake_regions(self):

        df = gn.gen_data_fake_regions(10)

        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
