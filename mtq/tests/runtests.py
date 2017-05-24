'''
Created on Aug 5, 2013

@author: sean
'''
from __future__ import print_function
from os.path import dirname

import sys
import unittest


def main():
    import coverage
    cov = coverage.coverage(include='**/mtq/**', omit=['**/tests/**'])
    cov.start()
    import mtq
    print(mtq)
    loader = unittest.loader.TestLoader()
    tests = loader.discover(dirname(__file__))
    runner = unittest.TextTestRunner()
    result = runner.run(tests)
    cov.stop()
    cov.save()
    cov.report()

    # Exit code depends on tests result
    sys.exit(0 if result.wasSuccessful() else -1)

if __name__ == '__main__':
    main()
