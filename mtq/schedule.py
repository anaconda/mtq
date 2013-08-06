'''
Created on Aug 5, 2013

@author: sean
'''
from dateutil.rrule import rrulestr
import time
from threading import Timer, Lock
from mtq.utils import now
from functools import wraps
import logging

def sync(func):
    @wraps(func)
    def sync_decorator(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)
        
    return sync_decorator

class Rule(object):
    def __init__(self, factory, doc):
        
        self.logger = logging.getLogger('mtq.Rule')

        self.lock = Lock()
        self.factory = factory
        self.doc = doc
        self.timer = None
        
        self.init_rule()
        
    def init_rule(self):
        doc = self.doc
        self.rrule = rrulestr(doc['rule'], dtstart=now())
        self.irule = iter(self.rrule)
        self.queue = self.factory.queue(doc['queue'], tags=doc['tags'])
        
        if self.timer is not None:
            self.set()
        
    def set(self):

        if self.timer is not None:
            self.timer.cancel()
            self.timer = None

        nw = now()
        self.nxt = nxt = next(self.irule, None)
        
        if nxt is None:
            self.logger.info('No more jobs to process, exiting')
            return
        
        if nxt < nw:
            timeout = 0
        else:
            timeout = (nxt - nw).seconds
            
        self.logger.info('Scheduling task "%s" to enqueue in %i seconds (%s)' % (self.task, timeout, nxt.ctime()))
        self.timer = t = Timer(timeout, self.execute_task)
        t.start()
        
    @property
    def task(self):
        return self.doc['task']
    
    @sync
    def execute_task(self):
        self.prev = self.nxt
        self.nxt = self.timer = None
        self.logger.info('Enquing task %s', self.task)
        self.queue.enqueue(self.task)
        self.set()
    
    def cancel(self):
        if self.timer:
            self.timer.cancel()
    @sync    
    def refresh(self):
        last_modified = self.doc['modified']
        collection = self.factory.schedule_collection
        doc = collection.find_one({'_id':self.id})
        if doc is None:
            if self.timer:
                self.timer.cancel()
            return True
        
        if doc['modified'] > last_modified:
            self.logger.info('Task %s modified, updating' % (self.id))
            self.doc = doc
            self.init_rule()
        
        return False

    @property
    def id(self):
        return self.doc['_id']
            
class Scheduler(object):
    
    def __init__(self, factory):
        self.factory = factory
        
        self.logger = logging.getLogger('mtq.Scheduler')

    
    def add_job(self, rule, task, queue, tags=()):
        collection = self.factory.schedule_collection
        return collection.insert({'rule':rule, 'task':task, 'queue':queue, 'tags':tags,
                                  'paused':False, 'active':True, 'modified':now()})

    def remove_job(self, _id):
        collection = self.factory.schedule_collection
        return collection.remove({'_id':_id})
    
    def update_job(self, _id, rule=None, task=None, queue=None, tags=None):
        collection = self.factory.schedule_collection
        query = {'_id':_id}
        doc = {'$set':{'modified':now()}}
        if rule is not None:
            doc['$set']['rule'] = rule
        if task is not None:
            doc['$set']['task'] = task
        if queue is not None:
            doc['$set']['queue'] = queue
        if tags is not None:
            doc['$set']['tags'] = tags
            
        return collection.update(query, doc)

    
    @property
    def jobs(self):
        collection = self.factory.schedule_collection
        return collection.find({'paused':False, 'active':True})
    
    def update_rules(self):
        pass
    
    def init_rules(self):
        
        self.rules = rules = set()
        
        for job in self.jobs:
            rule = Rule(self.factory, job)
            rules.add(rule)
            
        for r in rules:
            r.set()
    
    def refresh_rules(self):
        to_remove = set()
        for r in self.rules:
            should_remove = r.refresh()
            if should_remove:
                to_remove.add(r)
                
        for r in to_remove:
            self.rules.discard(r)
            
        _ids = {r.id for r in self.rules}
        for doc in self.jobs:
            if doc['_id'] not in _ids:
                rule = Rule(self.factory, doc)
                self.rules.add(rule)
                rule.set()
            
    def run(self):
        self.logger.info('Running Scheduler')
        self.init_rules()
        
        try:
            while 1:
                time.sleep(1)
                self.refresh_rules()
        except KeyboardInterrupt as err:
            self.logger.exception('good bye!')
        finally:
            for r in self.rules:
                r.cancel()
                
        
    
