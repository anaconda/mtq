'''
Created on Aug 4, 2013

@author: sean
'''
import unittest
import mtq
import mongomock



class MTQTestCase(unittest.TestCase):
    
    def setUp(self):
        def create_collection_test(collection_name, **kwargs):
            return self.db[collection_name]
        
        self.connection = mongomock.Connection()
        self.db = mongomock.Database(self.connection, 'mq')
        self.db.create_collection = create_collection_test 
        self.factory = mtq.create(db=self.db) 
