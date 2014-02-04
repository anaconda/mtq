from __future__ import print_function

import time
from mtq.utils import stream_logging
import logging
import io

logger = logging.getLogger('job.foo.bar')
logger.setLevel(logging.INFO)

def raise_error():
    x = 1 + 1
    logger = logging.getLogger('job.foo.bar')
    logger.info("this is an info")
    logging.getLogger('woc').info("this is from the woc log")
    logging.getLogger('job').info("this is an info 2333")
    asdf

def do_something():
    logger.info("this is a log")
    print("this is a print")
    
def long_running_task():
    try:
        print("this is long_running_task ...")
        for _ in range(60):
            print("this is still a long_running_task ...")
            time.sleep(10)
    
    except Exception as err:
        print("This is an exception")
        raise err
    except BaseException as err:
        print("This is a base exception")
        raise err
    
def main():
    record = io.BytesIO()
    hndlr = logging.StreamHandler(record)
    logger = logging.getLogger('job')
    logger.setLevel(logging.INFO)
    logger.addHandler(hndlr)
    hndlr.setLevel(logging.INFO)
    logger.warn("asdfasdf")
    
    print(record)
    record.seek(0)
    print(record.read())


if __name__ == '__main__':
    main()