from utils.get_favorite import get_favorite
import pytest

@pytest.mark.parametrize(
    #parameters to test
    "imput_test,expected",
    [(get_favorite(),
     {'1': '6.48', 
     '115866': '2.21',
     '140171': '1.95',
     '130154': '1.82',
     '22164': '1.45',
     '17174': '1.43',
     '3043': '1.33', 
     '2915': '1.31',
     '130024': '1.31',
     '22656': '1.26'}),
     (len(get_favorite()),10 )
     ]
    )

def test_top10_aceept(imput_test,expected):
  """ Compare result top 10 most favorite
    Arguments:
        imput_test: dictionary of top 10 elem. and
        length of element list
        expected: test results of top 10 elem.
  """
  assert(imput_test == expected)