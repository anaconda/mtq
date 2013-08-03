'''
Created on Aug 2, 2013

@author: sean
'''
from mq.utils import import_string, now
from mq.log import MongoStream
from datetime import datetime

class Job(object):
    def __init__(self, factory, doc):
        self.factory = factory
        self.doc = doc
    
    @property
    def tags(self):
        return self.doc['tags']

    @property
    def qname(self):
        return self.doc['qname']
    
    @property
    def func_str(self):
        return self.doc['execute']['func_str']
    
    @property
    def id(self):
        return self.doc['_id']
    
    
    @property
    def func(self):
        return import_string(self.doc['execute']['func_str'])
    
    @property
    def args(self):
        return self.doc['execute']['args']
    
    @property
    def kwargs(self):
        return self.doc['execute']['kwargs']
    
    def apply(self):
        return self.func(*self.args, **self.kwargs)
    
    def set_finished(self, failed=False):
        update = {'$set':{'processed':True,
                          'failed':failed,
                          'finsished_at': now()}
                  }
        
        self.factory.queue_collection.update({'_id':self.id}, update)

    def stream(self):
            
        return MongoStream(self.factory.logging_collection, 
                           doc={'job_id': self.id}, 
                           finished=self.finished)
    def finished(self):
        collection = self.factory.queue_collection
        cursor = collection.find({'_id':self.id, 'processed':True})
        return bool(cursor.count())
    
     
