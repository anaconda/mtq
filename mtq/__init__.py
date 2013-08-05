'''
MQ module
'''
from .connection import MTQConnection
from . import defaults
from .queue import Queue
from .worker import Worker, WorkerProxy
from .job import Job

def default_connection():
    '''
    Create a default mtq instance to created queues, workers, and jobs
    '''
    return MTQConnection.default()
     
def create_connection(db, 
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
    return MTQConnection(db, collection_base, qsize, workersize, logsize)
    
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
    return MTQConnection.from_config(config, client)


