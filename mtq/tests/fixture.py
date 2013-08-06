'''
Created on Aug 4, 2013

@author: sean
'''
import unittest
import mtq
import mongomock

def distinct(self, tag):
    result = set()
    for doc in self:
        if isinstance(doc[tag], (list,tuple)):
            result.update(doc[tag])
        else:
            result.add(doc[tag])
    return result
    return [doc[tag] for doc in self]

mongomock.Cursor.distinct = distinct

def test_func(*args, **kwargs):
    return args, kwargs

def test_func_fail(*args, **kwargs):
    raise Exception()



class MTQTestCase(unittest.TestCase):
    
    def setUp(self):
        def create_collection_test(collection_name, **kwargs):
            return self.db[collection_name]
        
        self.connection = mongomock.Connection()
        self.db = mongomock.Database(self.connection, 'mq')
        self.db.create_collection = create_collection_test 
        self.factory = mtq.create_connection(db=self.db) 
