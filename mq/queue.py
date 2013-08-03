'''
Created on Aug 2, 2013

@author: sean
'''
from bson.objectid import ObjectId
from datetime import datetime
from mq.job import Job
from mq.defaults import _collection_base, _logsize
from mq.utils import ensure_capped_collection, now

class QueueError(Exception):
    pass

class Queue(object):
    
    def __str__(self):
        return self.name
        
    def __init__(self, factory, name, tags=(), priority=0):
        
        if not isinstance(name, basestring):
            raise TypeError('name must be a string')
        
        if not isinstance(tags, (list, tuple)):
            raise TypeError('tags must be sequence')
        
        self.name = name
        self.factory = factory
        self.tags = tuple(tags)
        self.priority = priority
        

    def enqueue(self, func_or_str, *args, **kwargs):
        return self.enqueue_call(func_or_str, args, kwargs)
    
    def enqueue_call(self, func_or_str, args=(), kwargs=None, tags=(), priority=None, timeout=None):
        
        if not isinstance(func_or_str, basestring):
            name = getattr(func_or_str, '__name__', None)
            module = getattr(func_or_str, '__module__', None)
            
            if not (name and module):
                raise QueueError('can not enqueue %r (type %r)' % (func_or_str, type(func_or_str)))
            
            func_or_str = '%s.%s' % (module, name)
            
        if args is None:
            args = ()
        elif not isinstance(args, (list, tuple)):
            raise TypeError('argument args must be a tuple')
        if kwargs is None:
            kwargs = {}
        elif not isinstance(kwargs, dict):
            raise TypeError('argument kwargs must be a dict')
        
        execute = {'func_str': func_or_str, 'args':tuple(args), 'kwargs': dict(kwargs)}
        
        if priority is None:
            priority = self.priority
             
        doc = {
               'qname':self.name,
               'tags': self.tags + tuple(tags),
               
               'execute': execute,
               'process_after': now(),
               'enqueued_at': now(),
               'started_at': datetime.fromtimestamp(0),
               'finsished_at': datetime.fromtimestamp(0),
               'priority': priority,
               'processed': False,
               'failed': False,
               'timeout':timeout,
               'worker_id': ObjectId('000000000000000000000000'),
               }
    
        collection = self.factory.queue_collection
        collection.insert(doc)
        
        return Job(self.factory, doc)
    
    
    @property
    def _query(self):
        query = {'processed':False,
                'priority':{'$gte':self.priority},
                'process_after': {'$lte': now()},
                 }
        
        if self.name:
            query.update(qname=self.name)
            
        query.update(self._tag_query)
        return query
        
    @property
    def count(self):
        collection = self.factory.queue_collection
        query = self.factory.make_query([self.name], self.tags, self.priority)
        return collection.find(query).count()
    
    def pop(self, worker_id=None):
        return self.factory.pop_item(worker_id, [self.name], self.tags, self.priority)
    
    @property
    def all_tags(self):
        collection = self.factory.queue_collection
        return collection.find({'qname':self.name}).distinct('tags')


    def tag_count(self, tag):
        collection = self.factory.queue_collection
        return collection.find({'qname':self.name, 'tags':tag, 'processed':False}).count()
