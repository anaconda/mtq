'''
Created on Aug 5, 2013

@author: sean
'''
from dateutil.rrule import rrulestr
import time
from datetime import timedelta
from mtq.utils import now
import logging

from mtq.pymongo3compat import find_and_modify


class Scheduler(object):
    
    def __init__(self, factory):
        self.factory = factory
        
        self.logger = logging.getLogger('mtq.Scheduler')

    
    def add_job(self, rule, task, queue, tags=(), timeout=None):
        collection = self.factory.schedule_collection
        return collection.insert({'rule':rule, 'task':task, 'queue':queue, 'tags':tags,
                                  'paused':False, 'active':True,
                                  'modified':now(),
                                  'created':now(),
                                  'checked':now(),
                                  'timeout': timeout,
                                  })
 
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
    def rules(self):
        collection = self.factory.schedule_collection
        return collection.find({'paused':False, 'active':True})

    def check_rule(self, doc, n):
        collection = self.factory.schedule_collection
        query = {'_id':doc['_id'], 'checked': doc['checked']}
        update = {'$set': {'checked': n}}
        return bool(find_and_modify(query, update, projection=[], collection=collection))
    

    def enqueue_from_rule(self, rule):
        queue = self.factory.queue(rule['queue'], tags=rule['tags'])
        queue.enqueue_call(rule['task'], timeout=rule.get('timeout'))
    
    def run(self):
        self.logger.info('Running Scheduler')
        poll_interval = 5
        try:
            while 1:
                
                next_event = now() + timedelta(1)
                 
                for rule in self.rules:
                    n = now() 
                    rrule = rrulestr(rule['rule'], dtstart=rule['created'])
                    items = rrule.between(rule['checked'], n)
                    if len(items) > 1:
                        self.logger.warn("Schedular missed %i tasks! Enqueuing latest" % (len(items)))
                    if items:
                        if self.check_rule(rule, n):
                            self.logger.info("Enqueueing task %r (%s)" % (rule['task'], items[0].ctime()))
                            self.enqueue_from_rule(rule)
                        else:
                            self.logger.warn("Another schedular has already run this task. moving on")
                    
                    next_event = min(next_event, rrule.after(n))
                
                self.logger.debug("Next event %s" % next_event.ctime())
                next_event = (next_event - now()).total_seconds()
                self.logger.debug("Next event in %i seconds" % next_event)
                
                sleep = max(1, min(next_event, poll_interval)) 
                self.logger.debug("Sleping for %i seconds" % sleep)
                time.sleep(sleep)
        
        except KeyboardInterrupt:
            self.logger.exception('Exiting main loop')
