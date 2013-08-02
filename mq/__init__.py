from datetime import datetime
from multiprocessing import Process
import sys
import os
import time
import logging
from .log import StreamHandler
from mq.utils import import_string
from contextlib import contextmanager
from bson.objectid import ObjectId

_collection_base = 'mq'

class QueueError(Exception):
    pass

class ImportStringError(Exception):
    pass

class Worker(object):
    def __init__(self, queue, poll_interval=1, exception_handler=None):
        self.name = '%s.%s' % (os.uname()[1], os.getpid())
        self.queue = queue
        self.poll_interval = poll_interval
        
        self.logger = logging.getLogger('mq.Worker')
        self.logger.setLevel(logging.INFO)
        hdlr = StreamHandler()
        hdlr.setLevel(logging.INFO)
        self.logger.addHandler(hdlr) 
        self._current = None
        self._handler = exception_handler
        self._pre_call = None
        self._post_call = None
        
    
    @classmethod
    def all_workers(cls, db, collection_base=_collection_base):
        collection_name = '%s.workers' % collection_base
        collection = getattr(db, collection_name)
        return collection.find({'working':True})
    
    @classmethod    
    def processed_count(self, worker_id, db, collection_base=_collection_base):
        collection_name = '%s.queue' % collection_base
        collection = getattr(db, collection_name)
        return collection.find({'worker_id':worker_id}).count()

    @contextmanager
    def register(self):
        self.name
        collection_name = '%s.workers' % self.queue.collection_base
        
        db = self.queue.db
        if collection_name not in db.collection_names():
            db.create_collection(collection_name, capped=True,
                              size=(1024.**2) * 10)  # Mb

        self.collection = getattr(db, collection_name)
        
        self.worker_id = self.collection.insert({'name': self.name,
                                                 'started':datetime.now(),
                                                 'finished':datetime.now(),
                                                 'working':True,
                                                 'tags': self.queue.tags,
                                                 })
        try:
            print 'Register'
            yield self.worker_id
        finally:
            print 'Deregister'
            self.collection.update({'_id': self.worker_id},
                                   {'$set':{'finished':datetime.now(),
                                            'working':False
                                            }
                                    })
        
    def work(self, one=False, batch=False):
        
        with self.register():
            try:
                self.start_main_loop(one, batch)
            except KeyboardInterrupt:
                self.logger.exception(None)
                if not self._current:
                    return
                
                self.logger.warn('Warm shutdown requested')
                proc, job = self._current
                proc.join(timeout=job.doc.get('timeout'))

    def start_main_loop(self, one=False, batch=False):
        self.logger.info('Starting Main Loop')
        while 1:
            job = self.queue.pop(worker_id=self.worker_id)
            if job is None:
                if batch: break
                time.sleep(self.poll_interval)
                continue
            self.logger.info('Popped Job %s - %s' % (job.id, job.func_str))
            failed = self.process_job(job)
            job.finished(failed)
            
            if failed:
                self.logger.error('Job %s failed' % (job.doc['_id']))
            else:
                self.logger.info('Job %s finsihed successfully' % (job.doc['_id']))
            
            if one: break
        self.logger.info('Exiting Main Loop')
    
    def process_job(self, job):
        
        proc = Process(target=self._process_job, args=(job,))
        self._current = proc, job
        proc.start()
        proc.join(timeout=job.doc.get('timeout'))
        if proc.is_alive():
            self.logger.error('Timeout occurred: terminating job')
            proc.terminate()
        
        self._current = None
        
        return proc.exitcode != 0
    
    def _process_job(self, job):
        
        try:
            self._pre(job)
            job.apply()
        except:
            if self._handler:
                exc_type, exc_value, traceback = sys.exc_info()
                self._handler(job, exc_type, exc_value, traceback)
            raise
        finally:
            self._post(job)
                
    
    def _pre(self, job):
        if self._pre_call: self._pre_call(job)

    def _post(self, job):
        if self._post_call: self._post_call(job)

    def push_exception_handler(self, handler):
        self._handler = handler
    
    

class Job(object):
    def __init__(self, queue, doc):
        self.queue = queue
        self.doc = doc
    
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
    
    def finished(self, failed=False):
        self.queue._finished(self.doc['_id'], failed)
        
class Queue(object):
        
    @classmethod
    def all_tags(cls, db, collection_base=_collection_base):
        collection_name = '%s.queue' % (collection_base)
        collection = getattr(db, collection_name)
        
        return collection.find().distinct('tags')
        
    @classmethod
    def tag_count(cls, tag, db, collection_base=_collection_base):
        collection_name = '%s.queue' % (collection_base)
        collection = getattr(db, collection_name)
        
        return collection.find({'tags':tag, 'processed':False}).count()

    @classmethod
    def get_job(cls, job_id, db, collection_base=_collection_base):
        collection_name = '%s.queue' % (collection_base)
        collection = getattr(db, collection_name)
        doc = collection.find_one({'_id':job_id})
        if doc is None:
            return None
        queue = cls([], db, collection_base)
        return Job(queue, doc)
    
    def __init__(self, tags, db,
                 collection_base=_collection_base,
                 priority=0,
                 size=50):
        
        self.db = db
        collection_name = '%s.queue' % (collection_base)
        
        if collection_name not in db.collection_names():
            db.create_collection(collection_name, capped=True,
                              size=(1024.**2) * size)  # Mb

        self.tags = tuple(tags)
        self.collection = getattr(db, collection_name)
        self.collection_base = collection_base
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
             
        doc = {'execute': execute,
               'process_after': datetime.now(),
               'enqueued_at': datetime.now(),
               'started_at': datetime.fromtimestamp(0),
               'finsished_at': datetime.fromtimestamp(0),
               'priority': priority,
               'tags': self.tags + tuple(tags),
               'processed': False,
               'failed': False,
               'timeout':timeout,
               'worker_id': ObjectId('000000000000000000000000'),
               }
    
        self.collection.insert(doc)
        
        return Job(self, doc)
    
    
    @property
    def _query(self):
        query = {'processed':False,
                 'priority':{'$gte':self.priority},
                 }
#                  'process_after': {'$lte':datetime.now()}}
        query.update(self._tag_query)
        return query

    @property
    def _tag_query(self):
        if len(self.tags) == 0:
            return {}
        elif len(self.tags) == 1:
            return {'tags': self.tags[0]}
        else:
            return {'$and': [{'tags':tag} for tag in self.tags]}
        
    @property
    def count(self):
        
        return self.collection.find(self._query).count()
    
    def _finished(self, job_id, failed):
        update = {'$set':{'processed':True,
                          'failed':failed,
                          'finsished_at': datetime.now()}
                  }
        
        self.collection.update({'_id':job_id}, update)
        
    def pop(self, worker_id=None):
        update = {'$set':{'processed':True,
                          'started_at': datetime.now(),
                          'worker_id':worker_id}
                  }
        
        doc = self.collection.find_and_modify(self._query, update)
        
        if doc is None:
            return None
        else:
            return Job(self, doc)
    
        
