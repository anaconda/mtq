'''
Created on Aug 2, 2013

@author: sean
'''
import mtq
from pymongo.mongo_client import MongoClient
from mtq.defaults import _collection_base, _qsize, _workersize, _logsize, \
    _task_map
from mtq.utils import ensure_capped_collection, now
from time import mktime
from pymongo import ASCENDING
from mtq.pymongo3compat import find


class MTQConnection(object):
    '''
    Base object that you should use to create all other TQ objects

    ## Init

    :param db: mongo database
    :param collection_base: base name for collection
    :param qsize: the size of the capped collection of the queue

    ..sealso: MTQConnection.default, MTQConnection.from_config
    '''

    def __init__(self, db, collection_base=_collection_base, qsize=_qsize,
                 workersize=_workersize, logsize=_logsize, extra_lognames=()):
        self.db = db
        self.collection_base = collection_base
        self.qsize = qsize
        self.workersize = workersize
        self.logsize = logsize
        self._task_map = _task_map.copy()
        self.extra_lognames = extra_lognames

    def _destroy(self):
        'Destroy ALL data'
        self.db.connection.drop_database(self.db)

    @classmethod
    def default(cls):
        '''
        Create an MTQConnection default configuration using mongo from localhost
        '''
        return cls.from_config()

    @classmethod
    def from_config(cls, config=None, client=None):
        '''
        Create an MTQConnection from a config dict,

        :param config: configutation dict, with the parameters
            * DB_HOST
            * DB
            * COLLECTION_BASE
            * COLLECTION_SIZE
        :param client: a pymongo.MongoClient or None
        '''
        if config is None:
            config = {}

        if 'connection' in config:
            return config['connection']

        if client is None:
            client = MongoClient(config.get('DB_HOST', 'mongodb://localhost/?journal=true'), tz_aware=True)

        db = getattr(client, config.get('DB', 'mq'))

        base = config.get('COLLECTION_BASE', _collection_base)
        qsize = config.get('COLLECTION_SIZE', _qsize)
        return cls(db, base, qsize, extra_lognames=config.get('extra_lognames', ()))

    def job_stream(self, job_id):
        '''
        Get a file like object for the output of a job
        '''
        job = self.get_job(job_id)
        return job.stream()

    def worker_stream(self, worker_name=None, worker_id=None):
        '''
        Get a file like object for the output of a worker
        '''
        worker = self.get_worker(worker_name, worker_id)
        return worker.stream()

    @property
    def queue_collection(self):
        'The collection to push jobs to'
        collection_name = '%s.queue' % (self.collection_base)
        return self.db[collection_name]

    @property
    def finished_jobs_collection(self):
        'The collection to push jobs to'
        collection_name = '%s.finished_jobs' % (self.collection_base)
        return ensure_capped_collection(self.db, collection_name, self.qsize)

    @property
    def logging_collection(self):
        'The collection to push log lines to'
        db = self.db
        collection_name = '%s.log' % self.collection_base
        return ensure_capped_collection(db, collection_name, self.logsize)

    @property
    def schedule_collection(self):
        'The collection to push log lines to'
        db = self.db
        collection_name = '%s.schedule' % self.collection_base
        return db[collection_name]

    def make_query(self, queues, tags, priority=0, processed=False, failed=False, **query):
        '''
        return a mongodb query dict to get the next task in the queue
        '''
        query.update({
                'priority':{'$gte':priority},
                'process_after': {'$lte':now()},
                 })
        if failed:
            query['failed'] = True
        elif processed is not None:
            query['processed'] = processed

        if queues:
            if len(queues) == 1:
                query['qname'] = queues[0]
            else:
                query['qname'] = {'$in': queues}

        query.update(self.make_tag_query(tags))
        return query

    def make_tag_query(self, tags):
        'Query for tags'
        if not tags:
            tag_query = {}
#         elif len(tags) == 1:
#             return {'tags': tags[0]}
        else:
            # tags on job must be a subset of jobs on the worker
            # i.e. assert job['tags'] in worker['tags']
            tag_query = {'tags': {'$not':{ '$elemMatch' : {'$nin': tags}}}}

        return tag_query

    def add_mutex(self, query):
        running_query = self.make_query(None, None, processed=True)
        cursor = find(running_query, projection={'mutex':1, '_id':0},
                      collection=self.queue_collection)

        if not cursor.count():
            return

        # Populate dictionary of mutex_key:count
        mutex = {}

        # Iterate over running jobs
        for item in cursor:
            item_mutex = item.get('mutex', {})
            if not item_mutex: continue
            mutext_key = item_mutex.get('key')
            if not mutext_key: continue
            mutex.setdefault(mutext_key, 0)
            mutex[mutext_key] += 1

        # Query should inclue jobs with no mutex key or where its key is not in any running jobs
        _or = [{'mutex': None}, {'mutex': {'$exists': False}},
               {'mutex.key': {'$nin': list(mutex.keys())}}]

        # Query should inclue jobs where the mutex.count is > the # already running
        for key, already_running in mutex.items():
            _or.append({'mutex.key': key, 'mutex.count': {'$gt': already_running}})

        query['$or'] = _or

    def pop_item(self, worker_id, queues, tags, priority=0, failed=False):
        'Pop an item from the queue'
        n = now()
        update = {'$set':{'processed':True,
                          'started_at': n,
                          'started_at_': mktime(n.timetuple()),
                          'worker_id':worker_id}
                  }

        query = self.make_query(queues, tags, priority, failed)
        self.add_mutex(query)

        doc = self.queue_collection.find_and_modify(query, update, sort=[('enqueued_at', ASCENDING)])

        if doc is None:
            return None
        else:
            return mtq.Job(self, doc)

    def push_item(self, job_id):
        query = {'_id': job_id}
        update = {'$set':{'processed':False}}
        doc = self.queue_collection.find_and_modify(query, update)

    def _items_cursor(self, queues, tags, priority=0, processed=False, limit=None, reverse=False):
        query = self.make_query(queues, tags, priority, processed=processed)
        cursor = self.queue_collection.find(query)

        if reverse:
            cursor = cursor.sort('enqueued_at', -1)
        if limit:
            cursor = cursor.limit(limit)

        return cursor

    def items(self, queues, tags, priority=0, processed=False, limit=None, reverse=False):
        cursor = self._items_cursor(queues, tags, priority, processed, limit, reverse)
        return [mtq.Job(self, doc) for doc in cursor]

    def queue(self, name='default', tags=(), priority=0):
        '''
        Create a queue object

        :param name: the name of the queue
        :param tags: default tags to give to jobs
        :param priority: (not implemented yet)
        '''

        return mtq.Queue(self, name, tags, priority)

    def new_worker(self, queues=(), tags=(), priority=0, silence=False,
                   log_worker_output=False, poll_interval=3, args=None):
        '''
        Create a worker object

        :param queues: names of queues to pop from (these are OR'd)
        :param tags: jobs *must* have all these tags to be processed by this worker
        :param priority: (not implemented yet)
        :param log_worker_output: if true, log worker output to the db
        '''
        worker = mtq.Worker(self, queues, tags, priority,
                            log_worker_output=log_worker_output,
                            silence=silence, extra_lognames=self.extra_lognames, poll_interval=poll_interval)

        self.args = args
        self.worker = worker
        return worker

    # ===========================================================================
    # Workers
    # ===========================================================================
    @property
    def worker_collection(self):
        'Collection to register workers to'
        collection_name = '%s.workers' % self.collection_base
        return self.db[collection_name]

    @property
    def queues(self):
        'List of existing queues'
        collection = self.queue_collection
        qnames = collection.find().distinct('qname')
        return [mtq.Queue(self, qname) for qname in qnames]

    @property
    def workers(self):
        '''
        List of existing workers

        :returns: a WorkerProxy object
        '''
        collection = self.worker_collection
        return [mtq.WorkerProxy(self, item) for item in collection.find({'working':True})]

    def get_job(self, job_id):
        '''
        retrieve a job
        '''
        collection = self.queue_collection
        doc = collection.find_one({'_id':job_id})
        if doc is None:
            return None

        return mtq.Job(self, doc)

    def get_worker(self, worker_name=None, worker_id=None):
        '''
        retrieve a worker
        '''
        coll = self.worker_collection
        if worker_name:
            query = {'name':worker_name}
        elif worker_id:
            query = {'_id':worker_id}
        else:
            raise TypeError('must give one of worker_name or worker_id')
        doc = coll.find_one(query)
        if doc is None:
            raise TypeError('Could not find worker')

        return mtq.WorkerProxy(self, doc)

    # ===========================================================================
    # Scheduler
    # ===========================================================================

    def scheduler(self):
        return mtq.Scheduler(self)
