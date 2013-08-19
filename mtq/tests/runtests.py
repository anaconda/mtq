'''
Created on Aug 19, 2013

@author: sean
'''
import sys
from os.path import dirname
import unittest

def runtests():
    loader = unittest.loader.TestLoader()
    tests = loader.discover(dirname(__file__))
    runner = unittest.TextTestRunner()
    failed = runner.run(tests)
    sys.exit(failed)
    
if __name__ == '__main__':
    runtests()
