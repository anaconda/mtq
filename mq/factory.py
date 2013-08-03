'''
Created on Aug 2, 2013

@author: sean
'''
import mq
from pymongo.mongo_client import MongoClient
from mq.defaults import _collection_base, _qsize, _workersize, _logsize
from mq.log import MongoStream
from mq.utils import ensure_capped_collection, now
from datetime import datetime

class MQFactory(object):
    
    def __init__(self, db, collection_base=_collection_base, qsize=_qsize):
        self.db = db
        self.collection_base = collection_base
        self.qsize = qsize
    
    @classmethod
    def default(cls):
        return cls.from_config()

    def job_stream(self, job_id):
        job = self.get_job(job_id)
        return job.stream()
    
    def worker_stream(self, worker_name, worker_id):
        worker = self.get_worker(worker_name, worker_id)
        return worker.stream()
    
    @property
    def queue_collection(self):
        collection_name = '%s.queue' % (self.collection_base)
        return ensure_capped_collection(self.db, collection_name, self.qsize)

    @property
    def logging_collection(self):
        db = self.db
        collection_name = '%s.log' % self.collection_base
        return ensure_capped_collection(db, collection_name, _logsize)

    def make_query(self, queues, tags, priority=0):
        query = {'processed':False,
                 'priority':{'$gte':priority},
                 'process_after': {'$lte':now()},
                 }
        if queues:
            query['qname'] = {'$in': queues}
            
        query.update(self.make_tag_query(tags))
        return query
    
    def make_tag_query(self, tags):
        if len(tags) == 0:
            return {}
        elif len(tags) == 1:
            return {'tags': tags[0]}
        else:
            return {'$and': [{'tags':tag} for tag in tags]}

    def pop_item(self, worker_id, queues, tags, priority=0):
        update = {'$set':{'processed':True,
                          'started_at': now(),
                          'worker_id':worker_id}
                  }
        
        query = self.make_query(queues, tags, priority) 
        doc = self.queue_collection.find_and_modify(query, update)
        
        if doc is None:
            return None
        else:
            return mq.Job(self, doc)
    
    @classmethod
    def from_config(cls, config=None, client=None):
        
        if config is None:
            config = {}
            
        if client is None:
            client = MongoClient(config.get('DB_HOST', 'mongodb://localhost/?journal=true'))
        
        db = getattr(client, config.get('DB', 'mq'))
        
        base = config.get('COLLECTION_BASE', _collection_base)
        qsize = config.get('COLLECTION_SIZE', _qsize)
        return cls(db, base, qsize)
    
    def queue(self, name, tags=(), priority=0):
        return mq.Queue(self, name, tags, priority)
    

        
    def new_worker(self, queues=(), tags=(), priority=0, log_worker_output=False):
        return mq.Worker(self, queues, tags, priority, 
                         log_worker_output=log_worker_output)
    
    #===========================================================================
    # Workers
    #===========================================================================
    @property
    def worker_collection(self):
        collection_name = '%s.workers' % self.collection_base
        return ensure_capped_collection(self.db, collection_name, _workersize)

    @property
    def queues(self):
        collection = self.queue_collection
        qnames = collection.find().distinct('qname')
        return [mq.Queue(self, qname) for qname in qnames]
    
    @property
    def workers(self):
        collection = self.worker_collection
        return [mq.WorkerProxy(self, item) for item in collection.find({'working':True})]
    
        
    def get_job(self, job_id):
        collection = self.queue_collection
        doc = collection.find_one({'_id':job_id})
        if doc is None:
            return None
        
        return mq.Job(self, doc)
    
    def get_worker(self, worker_name=None, worker_id=None):
        coll = self.worker_collection
        if worker_name:
            query = {'name':worker_name}
        elif worker_id:
            query = {'_id':worker_id}
        else:
            raise TypeError('must give one of worker_name or worker_id')
        doc = coll.find_one(query)
        if doc is None:
            raise TypeError('Could not find worker')
        
        return mq.WorkerProxy(self, doc)
    

