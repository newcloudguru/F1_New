import time
from functools import wraps
import logging
from pyspark.sql import SparkSession


def my_logger(orig_func):
    logging.basicConfig(filename='{}.log'.format('f1'), filemode="w",
                        level=logging.INFO, format='%(levelname)s %(asctime)s %(''message)s')

    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        logging.info(f'ran {orig_func.__name__} function')
        # logging.info(f'ran {orig_func.__name__} function')
        return orig_func(*args, **kwargs)

    return wrapper


def my_timer(orig_func):
    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = orig_func(*args, **kwargs)
        t2 = time.time() - t1
        print('{} ran in: {} sec'.format(orig_func.__name__, t2))
        logging.info('{} execution time: {} sec'.format(orig_func.__name__, round(t2, 3)))
        return result

    return wrapper



