from utils.get_ratio import get_ratio
import pytest

@pytest.mark.parametrize(
    #parameters to test
    "imput_test,expected",
    [(get_ratio(),
     [['82', 27.8], 
     ['116', 239.0], 
     ['182', 249.33333333333334], 
     ['293', 54.666666666666664], 
     ['378', 365.0], 
     ['418', 102.66666666666667], 
     ['868', 133.83333333333334], 
     ['912', 86.0], 
     ['944', 38.666666666666664], 
     ['984', 69.8]])
     ]
    )

def test_ratio(imput_test,expected):
    """ Compare results first 10 elem. ratio psot
    Arguments:
        imput_test: list of first 10 elem.
        expected: test result of list 10 elem.
    """
    assert(imput_test == expected)
