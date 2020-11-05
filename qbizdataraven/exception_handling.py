import traceback
from functools import wraps
from datetime import datetime


class TestFailure(Exception):
    def __init__(self, message):
        super().__init__(message)


def try_except(logger, msg=None):
    """
    :param logger: logging function
    :param msg: optional exception message. should contain place holders for: current timestamp, traceback
    :return: function call contained in wrapper or raise appropriate exception
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                tb = traceback.format_exc()
                if msg:
                    error_msg = msg.format(now=now, tb=tb)
                else:
                    error_msg = f"caught exception while calling {func.__name__} at {now}. traceback: {tb}"
                logger(error_msg)
                raise ex
        return wrapper
    return decorator
