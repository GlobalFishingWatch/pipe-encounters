from apache_beam.testing.util import BeamAssertException
import warnings
import math

def _approx_equal(expected, actual, fp_abs_tol, fp_rel_tol):
    try:
        if isinstance(expected, (list, tuple)):
            return (len(actual) == len(expected) and 
                    all(_approx_equal(a, b, fp_abs_tol, fp_rel_tol) for (a, b) in zip(expected, actual)))
        if isinstance(expected, dict):
            return (sorted(actual.keys()) == sorted(expected.keys()) and
                    all(_approx_equal(expected[k], actual[k], fp_abs_tol, fp_rel_tol) for k in expected))
        if isinstance(expected, float) or isinstance(actual, float):
            return ((math.isinf(actual) and math.isinf(expected)) or
                    (math.isnan(actual) and math.isnan(expected)) or
                    abs(actual - expected) <= max(fp_abs_tol,
                            0.5 * fp_rel_tol * (abs(actual) + abs(expected)))) 
    except:
        warnings.warn('comparison failure in approx_equal_to')
        return False
    return actual == expected


# Once we have updated to recent version of Beam, we could simplify this 
# by using Beam's `equal_to` with a custom `equal_fn`

def approx_equal_to(expected, fp_abs_tol=1e-8, fp_rel_tol=1e-5):
    """Predicate for apache_beam.testing.util.assert_that with floating tolerance

    Parameters
    ----------
    expected : obj
        The expected result that we are comparing too
    fp_abs_tol : float
        Absolute tolerance for comparing floating point numbers
    fp_abs_rel : float
        Relative tolerance for comparing floating point numbers

    Returns
    -------
    function(actual) -> None
        Raises BeamAssertException if actual is not approximately equal to expected.
    """
    def _check(actual):
        expected_list = list(expected)
        unexpected = []
        for element in actual:
          found = False
          for i, v in enumerate(expected_list):
            if _approx_equal(v, element, fp_abs_tol, fp_rel_tol):
              found = True
              expected_list.pop(i)
              break
          if not found:
            unexpected.append(element)
        if unexpected or expected_list:
          msg = 'Failed assert: %r == %r' % (expected, actual)
          if unexpected:
            msg = msg + ', unexpected elements %r' % unexpected
          if expected_list:
            msg = msg + ', missing elements %r' % expected_list
          raise BeamAssertException(msg)
    return _check
