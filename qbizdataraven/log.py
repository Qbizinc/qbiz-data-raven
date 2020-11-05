import logging


def get_null_logger():
    handler = logging.NullHandler()
    logger = logging.getLogger()
    logger.addHandler(handler)
    return logger