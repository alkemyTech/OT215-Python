import unittest

from test_ratio_ans_views import TestRatioAnsViews
from test_posts_per_date import TestPostsPerDate
from test_avg_res_time import TestAvgResTime


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestAvgResTime))
    suite.addTest(unittest.makeSuite(TestPostsPerDate))
    suite.addTest(unittest.makeSuite(TestRatioAnsViews))
    return suite

if __name__ == '__main__':
    unittest.main()