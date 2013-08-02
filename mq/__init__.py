from datetime import datetime
from multiprocessing import Process
import sys
import os
import time
import logging
from .log import StreamHandler

class QueueError(Exception):
    pass

class ImportStringError(Exception):
    pass

class Worker(object):
    def __init__(self, queue, poll_interval=1):
        self.queue = queue
        self.poll_interval = poll_interval
        
        self.logger = logging.getLogger('mq.Worker')
        self.logger.setLevel(logging.INFO)
        hdlr = StreamHandler()
        hdlr.setLevel(logging.INFO)
        self.logger.addHandler(hdlr) 
        
    def work(self):
        while 1:
            self.logger.info()
            job = self.queue.pop()
            if job is None:
                time.sleep(self.poll_interval)
                continue
            
            self.logger.info('')
            code = job.apply_in_process()
            failed = code != 0
            job.finished(failed)
            
    def process_job(self, job):
        job.apply()


class Job(object):
    def __init__(self, queue, doc):
        self.queue = queue
        self.doc = doc
    
    def apply(self):
        execute = self.doc['execute']
        func_str = execute['func_str']
        args = execute['args']
        kwargs = execute['kwargs']
        
        func = import_string(func_str)
        
        return func(*args, **kwargs)
    
    def apply_in_process(self):
        
        proc = Process(target=self.apply)
        proc.start()
        proc.join()
        return proc.exitcode
        
    
    def finished(self, failed=False):
        self.queue._finished(self.doc['_id'], failed)
        
class Queue(object):
    
    @classmethod
    def all_tags(cls, db, collection_base='mq'):
        collection_name = '%s.queue' % (collection_base)
        collection = getattr(db, collection_name)
        
        print collection.find().distinct('tags')
    
    def __init__(self, tags, db,
                 collection_base='mq',
                 priority=0,
                 size=50):
        
        self.db = db
        collection_name = '%s.queue' % (collection_base)
        
        if collection_name not in db.collection_names():
            db.create_collection(collection_name, capped=True,
                              size=(1024.**2) * size)  # Mb

        self.tags = tuple(tags)
        self.collection = getattr(db, collection_name)
        self.priority = priority
        
    def enqueue_call(self, func_or_str, args=(), kwargs=None, tags=(), priority=None):
        
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
            elif not isinstance(args, dict):
                raise TypeError('argument kwargs must be a dict')
            
            execute = {'func_str': func_or_str, 'args':tuple(args), 'kwargs': dict(kwargs)}
            
            if priority is None:
                priority = self.priority
                 
            doc = {'execute': execute,
                   'process_after': datetime.now(),
                   'enqueued_at': datetime.now(),
                   'started_at': datetime.now(),
                   'finsished_at': datetime.now(),
                   'priority': priority,
                   'tags': self.tags + tuple(tags),
                   'processed': False,
                   'failed': False,
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
        update = {'processed':True,
                  'failed':failed
                  'finsished_at': datetime.now()}
        self.collection.update({'_id':job_id}, update)
        
    def pop(self):
        update = {'processed':True,
                  'started_at': datetime.now()}
        doc = self.collection.find_and_modify(self._query, update)
        if doc is None:
            return None
        else:
            return Job(self, doc)
    
        


def import_string(import_name, silent=False):
    """Imports an object based on a string.  This is useful if you want to
    use import paths as endpoints or something similar.  An import path can
    be specified either in dotted notation (``xml.sax.saxutils.escape``)
    or with a colon as object delimiter (``xml.sax.saxutils:escape``).

    If `silent` is True the return value will be `None` if the import fails.

    For better debugging we recommend the new :func:`import_module`
    function to be used instead.

    :param import_name: the dotted name for the object to import.
    :param silent: if set to `True` import errors are ignored and
                   `None` is returned instead.
    :return: imported object
    """
    # force the import name to automatically convert to strings
    if isinstance(import_name, unicode):
        import_name = str(import_name)
    try:
        if ':' in import_name:
            module, obj = import_name.split(':', 1)
        elif '.' in import_name:
            module, obj = import_name.rsplit('.', 1)
        else:
            return __import__(import_name)
        # __import__ is not able to handle unicode strings in the fromlist
        # if the module is a package
        if isinstance(obj, unicode):
            obj = obj.encode('utf-8')
        try:
            return getattr(__import__(module, None, None, [obj]), obj)
        except (ImportError, AttributeError):
            # support importing modules not yet set up by the parent module
            # (or package for that matter)
            modname = module + '.' + obj
            __import__(modname)
            return sys.modules[modname]
    except ImportError, e:
        if not silent:
            raise ImportStringError(import_name, e), None, sys.exc_info()[2]

