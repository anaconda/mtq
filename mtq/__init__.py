'''
MQ module
'''
from .connection import MTQConnection
from . import defaults
from .queue import Queue
from .worker import Worker, WorkerProxy
from .job import Job
from .schedule import Scheduler
from mtq.defaults import _task_map

try:
    from _version import __version__
except ImportError:
    __version__ = '0.7.1'


def default_connection():
    '''
    Create a default mtq instance to created queues, workers, and jobs
    '''
    return MTQConnection.default()


def create_connection(db,
                      collection_base=defaults._collection_base,
                      qsize=defaults._qsize,
                      workersize=defaults._workersize,
                      logsize=defaults._logsize,
                      extra_lognames=()):
    '''
    Create a new mtq instance to created queues, workers, and jobs

    :param db: mongo database
    :param collection_base: base name for collection
    :param qsize: the size of the capped collection of the queue

    '''
    return MTQConnection(db, collection_base, qsize, workersize, logsize, extra_lognames)


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


def task(func=None, name=None):

    def inner(func_inner):
        task_name = name or '%s.%s' % (func_inner.__module__, func_inner.__name__)
        _task_map[task_name] = func_inner

    if func is None:
        return inner
    else:
        return inner(func)
