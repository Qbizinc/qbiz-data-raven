

def test_predicate_gt(measure_result, threshold):
    """
    :param measure_result:
    :param threshold:
    :return:
    """
    if measure_result > threshold:
        return "test_fail"
    else:
        return "test_pass"


def test_predicate_lt(measure_result, threshold):
    if measure_result < threshold:
        return "test_fail"
    else:
        return "test_pass"


def test_predicate_ne(measure_result, threshold):
    if measure_result != threshold:
        return "test_fail"
    else:
        return "test_pass"
