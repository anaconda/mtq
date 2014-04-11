'''
Created on Aug 2, 2013

@author: sean
'''
from mtq.utils import import_string, now, nulltime
from mtq.log import MongoStream
from bson.objectid import ObjectId
from time import mktime

class Job(object):
    '''
    A Job is just a convenient datastructure to pass around job (meta) data.
    
    Do not create directly, use MTQConnection.get_job
    '''
    def __init__(self, factory, doc):
        self.factory = factory
        self.doc = doc
    def __repr__(self):
        return '<job queue=%r tags=%r func_name=%r>' % (self.qname, self.tags, self.func_name)

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

        if self.func_name in self.factory._task_map:
            return self.factory._task_map[self.func_name]

        return import_string(self.func_name)

    @property
    def call_str(self):
        args = [repr(arg) for arg in self.args]
        args.extend('%s=%r' % item for item in self.kwargs.items())
        args = ', '.join(args)
        return '%s(%s)' % (self.func_name, args)

    @property
    def enqueued(self):
        return self.doc['enqueued_at']

    @property
    def started(self):
        return self.doc['started_at']

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
        n = now()

        update = {'$set':{'processed':True,
                          'failed':failed,
                          'finished':True,
                          'finished_at': n,
                          'finished_at_': mktime(n.timetuple())
                          }
                  }

        self.factory.queue_collection.update({'_id':self.id}, update)

        if not failed:
            data = self.factory.queue_collection.find_one({'_id':self.id})
            if data:
                self.factory.queue_collection.remove({'_id':self.id})
                self.factory.finished_jobs_collection.insert(data)

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

    def cancel(self):
        self.set_finished()


    @classmethod
    def new(cls, name, tags, priority, execute, timeout, mutex=None):

        n = now()
        no = mktime(n.timetuple())
        return {
               'qname':name,
               'tags': tags,
               'process_after': n,
               'priority': priority,

               'execute': execute,
               'enqueued_at': n,
               'enqueued_at_': no,
               'started_at': nulltime(),
               'started_at_': 0.0,
               'finished_at': nulltime(),
               'finished_at_': 0.0,
               'processed': False,
               'failed': False,
               'finished': False,
               'timeout':timeout,
               'worker_id': ObjectId('000000000000000000000000'),
               'mutex': mutex,
               }




