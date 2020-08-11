import unittest
import VwOpts

class TestNormalize(unittest.TestCase):
    def test_equal_after_normalize(self):
        self.assertEqual(
            VwOpts.normalize('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.normalize(' --ccb_explore_adf --dsjson --epsilon 0.1'))

        self.assertEqual(        
            VwOpts.normalize('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.normalize('--dsjson  --ccb_explore_adf  --epsilon 0.1  '))

        self.assertEqual(
            VwOpts.normalize('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            VwOpts.normalize('--dsjson  --ccb_explore_adf --l 0.2 --epsilon 0.1  '))

    def test_not_equal_after_normalize(self):
        self.assertNotEqual(
            VwOpts.normalize('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.normalize(' --ccb_explore_adf --dsjson --epsilon 0.2'))

        self.assertNotEqual(        
            VwOpts.normalize('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.normalize('--dsjson  --cb_explore_adf  --epsilon 0.1  '))

        self.assertNotEqual(
            VwOpts.normalize('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            VwOpts.normalize('--dsjson  --ccb_explore_adf --l 0.1 --epsilon 0.2  '))

if __name__ == '__main__':
    unittest.main()

