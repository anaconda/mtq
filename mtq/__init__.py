'''
MQ module
'''
from factory import MTQFactory
import defaults

def default():
    '''
    Create a default mtq instance to created queues, workers, and jobs
    '''
    return MTQFactory.default()
     
def create(db, 
           collection_base=defaults._collection_base, 
           qsize=defaults._qsize, 
           workersize=defaults._workersize, 
           logsize=defaults._logsize):
    '''
    Create a new mtq instance to created queues, workers, and jobs
    
    :param db: mongo database
    :param collection_base: base name for collection
    :param qsize: the size of the capped collection of the queue

    '''
    return MTQFactory(db, collection_base, qsize, workersize, logsize)
    
def from_config(config=None, client=None):
    '''
    Create a new mtq instance to created queues, workers, and jobs
    
    :param config: configutation dict, with the parameters
        * DB_HOST
        * DB
        * COLLECTION_BASE
        * COLLECTION_SIZE
    :param client: a pymongo.MongoClient or None
    '''
    return MTQFactory.from_config(config, client)

from queue import Queue
from worker import Worker, WorkerProxy
from job import Job

