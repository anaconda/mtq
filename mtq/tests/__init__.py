from __future__ import print_function

import io
import logging

logger = logging.getLogger('job.foo.bar')
logger.setLevel(logging.INFO)


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
