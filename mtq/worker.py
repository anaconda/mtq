'''
Created on Aug 2, 2013

@author: sean
'''
from contextlib import contextmanager
from datetime import datetime
from mtq.log import StreamHandler, MongoStream, MongoHandler
from mtq.utils import handle_signals, setup_logging, now, is_py3
from multiprocessing import Process
import logging
import os
import sys
import time
import io

class Worker(object):
    '''
    Should create a worker from MTQConnection.new_worker
    '''
    def __init__(self, factory, queues=(), tags=(), priority=0,
                 poll_interval=1, exception_handler=None,
                 log_worker_output=False, silence=False):
        
        self.name = '%s.%s' % (os.uname()[1], os.getpid())
        
        self.queues = queues
        self.tags = tags
        self.priority = priority
        
        self._log_worker_output = log_worker_output
        self.factory = factory
        self.poll_interval = poll_interval
        
        self.logger = logging.getLogger('mq.Worker')
        
        self._current = None
        self._handler = exception_handler
        self._pre_call = None
        self._post_call = None
        self.silence = silence
        
        
    
    @contextmanager
    def register(self):
        '''
        Internal
        Contextmanager, register the birth and death of this worker
        
        eg::
            with worker.register():
                # Work
        '''
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
        '''
        Main work function
        
        :param one: wait for the first job execute and then exit
        :param batch: work until the queue is empty, then exit 
        '''
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
        '''
        Start the main loop and process jobs
        '''
        self.logger.info('Starting Main Loop mogno-host=%s mongo-db=%s' % (self.factory.db.connection.host, 
                                                                           self.factory.db.name))
        self.logger.info('Starting Main Loop worker=%s _id=%s' % (self.name, self.worker_id))
        self.logger.info('Listening for jobs queues=[%s] tags=[%s]' % (', '.join(self.queues), ', '.join(self.tags)))
        while 1:
            
            job = self.factory.pop_item(worker_id=self.worker_id,
                                        queues=self.queues,
                                        tags=self.tags,
                                        )
            if job is None:
                if batch: break
                time.sleep(self.poll_interval)
                continue
            
            failed = self.process_job(job)
            
            if one: break
            
            self.logger.info('Listening for jobs queues=[%s] tags=[%s]' % (', '.join(self.queues), ', '.join(self.tags)))
            
        self.logger.info('Exiting Main Loop')
    
    def process_job(self, job):
        '''
        Process a single job in a multiprocessing.Process
        '''
        self.logger.info('Popped Job _id=%s queue=%s tags=%s' % (job.id, job.qname, ', '.join(job.tags)))
        self.logger.info(job.call_str)

        proc = Process(target=self._process_job, args=(job,))
        self._current = proc, job
        proc.start()
        proc.join(timeout=job.doc.get('timeout'))
        if proc.is_alive():
            self.logger.error('Timeout occurred: terminating job')
            proc.terminate()
        
        self._current = None
        
        failed = proc.exitcode != 0
    
        if failed:
            self.logger.error('Job %s failed' % (job.doc['_id']))
        else:
            self.logger.info('Job %s finsihed successfully' % (job.doc['_id']))
        
        job.set_finished(failed)
        
        return  failed
        
    def _process_job(self, job):
        '''
        '''
        handle_signals()
        
        if self._log_worker_output:
            setup_logging(self.worker_id, job.id, self.factory.logging_collection, self.silence)
        elif self.silence:
            if is_py3():
                sys.stderr = io.StringIO()
                sys.stdout = io.StringIO()
            else:
                sys.stderr = io.BytesIO()
                sys.stdout = io.BytesIO()
        
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

    def set_pre(self, func):
        self._pre_call = func

    def set_post(self, func):
        self._post_call = func
        
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

    
    
