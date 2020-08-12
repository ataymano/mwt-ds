import unittest
import VwOpts

class TestStringHash(unittest.TestCase):
    def test_equal_after_normalize(self):
        self.assertEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash(' --ccb_explore_adf --dsjson --epsilon 0.1'))

        self.assertEqual(        
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash('--dsjson  --ccb_explore_adf  --epsilon 0.1  '))

        self.assertEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            VwOpts.string_hash('--dsjson  --ccb_explore_adf --l 0.2 --epsilon 0.1  '))

    def test_not_equal_after_normalize(self):
        self.assertNotEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash(' --ccb_explore_adf --dsjson --epsilon 0.2'))

        self.assertNotEqual(        
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash('--dsjson  --cb_explore_adf  --epsilon 0.1  '))

        self.assertNotEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            VwOpts.string_hash('--dsjson  --ccb_explore_adf --l 0.1 --epsilon 0.2  '))

class TestToString(unittest.TestCase):
    def test_to_string(self):
        self.assertEqual(
            VwOpts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson'}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson')

        self.assertEqual(
            VwOpts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                                '--l': 0.1}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.1')

        self.assertEqual(
            VwOpts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                                '--l': 0.1,
                                '--cb_type': 'mtr'}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --cb_type mtr --l 0.1')

if __name__ == '__main__':
    unittest.main()

