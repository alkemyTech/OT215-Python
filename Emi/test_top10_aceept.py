from utils.get_top import get_top
import pytest

@pytest.mark.parametrize(
    #parameters to test
    "imput_test,expected",
    [(get_top(),
     [('discussion', 2874), 
     ('feature-request', 1775), 
     ('support', 1723), 
     ('bug', 1252),
     ('status-completed', 1188), 
     ('stackoverflow', 679), 
     ('reputation', 518), 
     ('tags', 449), 
     ('questions', 408), 
     ('status-bydesign', 382)]),
     (len(get_top()),10 )
     ]
    )

def test_top10_aceept(imput_test,expected):
    """ Compare result top 10 aceepted post
    Arguments:
        imput_test: list of top 10 elem. and
        length of element list
        expected: test results of top 10 elem.
    """
    assert(imput_test == expected)