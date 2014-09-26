'''
Created on Aug 4, 2013

@author: sean
'''
import unittest
import mtq
import pymongo
def test_func(*args, **kwargs):
    return args, kwargs

def test_func_fail(*args, **kwargs):
    raise Exception()



class MTQTestCase(unittest.TestCase):

    def setUp(self):

        self.connection = pymongo.MongoClient()
        self.db = self.connection['mtq_testing']
        self.factory = mtq.create_connection(db=self.db)

    def tearDown(self):
        self.connection.drop_database('mtq_testing')





