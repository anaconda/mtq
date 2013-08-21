'''
Created on Aug 5, 2013

@author: sean
'''
from __future__ import print_function
import unittest
from os.path import dirname

def main():
    import coverage
    cov = coverage.coverage(include='**/mtq/**', omit=['**/tests/**'])
    cov.start()
    import mtq
    print(mtq)
    loader = unittest.loader.TestLoader()
    tests = loader.discover(dirname(__file__))
    runner = unittest.TextTestRunner()
    runner.run(tests) 
    cov.stop()
    cov.save()
    cov.report()

if __name__ == '__main__':
    main()
