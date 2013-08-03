'''
Created on Aug 2, 2013

@author: sean
'''
from datetime import datetime
import logging
from mq.log import StreamHandler, MongoStream, MongoHandler
from mq.defaults import _collection_base, _workersize, _logsize
import os
from contextlib import contextmanager
import sys
from multiprocessing import Process
import time
from mq.utils import handle_signals, setup_logging, now

class Worker(object):
    def __init__(self, factory, queues=(), tags=(), priority=0,
                 poll_interval=1, exception_handler=None,
                 _size=_workersize,
                 log_worker_output=False):
        
        self.name = '%s.%s' % (os.uname()[1], os.getpid())
        
        self.queues = queues
        self.tags = tags
        self.priority = priority
        
        self._size = _size
        self._log_worker_output = log_worker_output
        self.factory = factory
        self.poll_interval = poll_interval
        
        self.logger = logging.getLogger('mq.Worker')
        self.logger.setLevel(logging.INFO)
        hdlr = StreamHandler()
        self.logger.addHandler(hdlr)
        
        self._current = None
        self._handler = exception_handler
        self._pre_call = None
        self._post_call = None
        
    
    @contextmanager
    def register(self):
        self.collection = self.factory.worker_collection
        
        self.worker_id = self.collection.insert({'name': self.name,
                                                 'started':now(),
                                                 'finished':datetime.fromtimestamp(0),
                                                 'working':True,
                                                 'queues': self.queues,
                                                 'tags': self.tags,
                                                 'log_output': bool(self._log_worker_output),
                                                 })

        hdlr = MongoHandler(self.factory.logging_collection, {'worker_id':self.worker_id})
        self.logger.addHandler(hdlr)
        try:
            yield self.worker_id
        finally:
            self.logger.removeHandler(hdlr)
            self.collection.update({'_id': self.worker_id},
                                   {'$set':{'finished':now(),
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
        self.logger.info('Starting Main Loop worker=%s _id=%s' % (self.name, self.worker_id))
        while 1:
            job = self.factory.pop_item(worker_id=self.worker_id,
                                        queues=self.queues,
                                        tags=self.tags,
                                        )
            if job is None:
                if batch: break
                time.sleep(self.poll_interval)
                continue
            
            self.logger.info('Popped Job _id=%s queue=%s tags=%s' % (job.id, job.qname, ', '.join(job.tags)))
            failed = self.process_job(job)
            job.set_finished(failed)
            
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
        
        handle_signals()
        
        if self._log_worker_output:
            setup_logging(self.worker_id, job.id, self.factory.logging_collection)
        
        logger = logging.getLogger('job')
        logger.info('Starting Job %s' % job.id)
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
    
    
class WorkerProxy(object):
    
    def __init__(self, factory, doc):
        self.factory = factory
        self.doc = doc
        
    @property
    def id(self):
        return self.doc['_id']
    
    @property
    def name(self):
        return self.doc['name']

    @property
    def qnames(self):
        return self.doc.get('queues', ())
    
    @property
    def tags(self):
        return self.doc['tags']
        
    @property
    def num_processed(self):
        collection = self.factory.worker_collection
        return collection.find({'worker_id': self.id}).count()
    
    def stream(self):
        collection = self.factory.logging_collection
        
        return MongoStream(collection,
                           doc={'worker_id': self.id},
                           finished=self.finished)
    
    def finished(self):
        coll = self.factory.worker_collection
        cursor = coll.find({'_id':self.id, 'working':False})
        return bool(cursor.count())


    
