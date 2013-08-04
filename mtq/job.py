'''
Created on Aug 2, 2013

@author: sean
'''
from mtq.utils import import_string, now
from mtq.log import MongoStream

class Job(object):
    '''
    A Job is just a convenient datastructure to pass around job (meta) data.
    
    Do not create directly, use MTQFactory.get_job
    '''
    def __init__(self, factory, doc):
        self.factory = factory
        self.doc = doc
    
    @property
    def tags(self):
        'List of tags for this job'
        return self.doc['tags']

    @property
    def qname(self):
        'The name of the queue that this job is in'
        return self.doc['qname']
    
    @property
    def func_name(self):
        'The name of the task to execute'
        return self.doc['execute']['func_str']
    
    @property
    def id(self):
        'the identifier for this job'
        return self.doc['_id']
    
    
    @property
    def func(self):
        'a callable function for workers to execute'
        return import_string(self.doc['execute']['func_str'])
    
    @property
    def args(self):
        'The arguments to call func with'
        return self.doc['execute']['args']
    
    @property
    def kwargs(self):
        'The keyword arguments to call func with'
        return self.doc['execute']['kwargs']
    
    def apply(self):
        'Execute this task syncronusly'
        return self.func(*self.args, **self.kwargs)
    
    def set_finished(self, failed=False):
        '''
        Mark this jog as finished.
        
        :param failed: if true, this was a failed job
        '''
        update = {'$set':{'processed':True,
                          'failed':failed,
                          'finsished_at': now()}
                  }
        
        self.factory.queue_collection.update({'_id':self.id}, update)

    def stream(self):
        '''
        Get a stream to read log lines from this job  
        '''
        return MongoStream(self.factory.logging_collection, 
                           doc={'job_id': self.id}, 
                           finished=self.finished)
        
    def finished(self):
        '''
        test if this job has finished
        '''
        collection = self.factory.queue_collection
        cursor = collection.find({'_id':self.id, 'processed':True})
        return bool(cursor.count())
    
     
