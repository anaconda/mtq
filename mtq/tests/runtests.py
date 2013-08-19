'''
Created on Aug 5, 2013

@author: sean
'''
import unittest
from os.path import dirname

def main():
    import coverage
    cov = coverage.coverage(omit=['**/site-packages/**', '**/tests/**'])
    cov.start()
    
    loader = unittest.loader.TestLoader()
    tests = loader.discover(dirname(__file__))
    runner = unittest.TextTestRunner()
    runner.run(tests) 
    cov.stop()
    cov.save()
    cov.report()

if __name__ == '__main__':
    main()
