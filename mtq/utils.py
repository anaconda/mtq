'''
Created on Aug 1, 2013

@author: sean
'''
import signal
import traceback
import sys
import logging
from datetime import datetime
from bson.errors import InvalidId
from bson.objectid import ObjectId
from contextlib import contextmanager
import io
import pytz
from mtq import errors

class ImportStringError(Exception):
    pass

is_py3 = lambda: sys.version_info.major >= 3
def is_str(obj):
    if is_py3():
        return isinstance(obj, str)
    else:
        return isinstance(obj, basestring)

def is_unicode(obj):
    if is_py3():
        return isinstance(obj, str)
    else:
        return isinstance(obj, unicode)


def handle_signals():
    '''
    Handle signals in multiprocess.Process threads
    '''
    def handler(signum, frame):
        signal.signal(signal.SIGINT, signal.default_int_handler)

    def raise_timeout(signum, frame):
        raise errors.Timeout()

    def term_handler(signum, frame):
        traceback.print_stack(frame)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        raise SystemExit(-signum)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, term_handler)
    signal.signal(signal.SIGALRM, raise_timeout)

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
    if is_unicode(import_name):
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
        if is_unicode(obj) and not is_py3():
            obj = obj.encode('utf-8')
        try:
            return getattr(__import__(module, None, None, [obj]), obj)
        except (ImportError, AttributeError):
            # support importing modules not yet set up by the parent module
            # (or package for that matter)
            modname = module + '.' + obj
            __import__(modname)
            return sys.modules[modname]
    except ImportError as e:
        if not silent:
            raise (ImportStringError(import_name, e), None, sys.exc_info()[2])

def ensure_capped_collection(db, collection_name, size_mb):
    '''
    '''
    if collection_name not in db.collection_names():
        db.create_collection(collection_name, capped=True,
                          size=(1024.**2) * size_mb)  # Mb

    return db[collection_name]


@contextmanager
def stream_logging(silence=False):
    from mtq.log import IOStreamLogger

    stdout = sys.stdout
    sys.stdout = IOStreamLogger(sys.stdout, silence)

    stderr = sys.stderr
    sys.stderr = IOStreamLogger(sys.stderr, silence)
    yield sys.stdout, sys.stderr

    sys.stdout = stdout
    sys.stderr = stderr

class UnicodeFormatter(logging.Formatter):
    def format(self, record):
        msg = logging.Formatter.format(self, record)
        if hasattr(msg, 'decode'):
            msg = msg.decode()
        return msg

mgs_template = """Job %s exited with exception:
Job Log:
   | %s
        
"""
@contextmanager
def setup_logging2(worker_id, job_id, lognames=()):

    record = io.StringIO()
    record_hndlr = logging.StreamHandler(record)
    record_hndlr.setFormatter(UnicodeFormatter())
    record_hndlr.setLevel(logging.INFO)

    logger = logging.getLogger('job')

    logger.setLevel(logging.INFO)
    loggers = [logger] + [logging.getLogger(name) for name in lognames]

    [l.addHandler(record_hndlr) for l in loggers]

    logger.info('Starting Job %s' % job_id)

    try:
        yield loggers
    except:
        text = record.getvalue().replace('\n', '\n   | ')
        msg = mgs_template % (job_id, text,)
        logger.exception(msg)
        raise
    else:
        logger.info("Job %s finished successfully" % (job_id,))
    finally:
        pass

def setup_logging(worker_id, job_id, silence=False):
    '''
    set up logging for worker
    '''
    from mtq.log import mstream, MongoHandler

    doc = {'worker_id':worker_id, 'job_id':job_id}
    sys.stdout = mstream(collection, doc.copy(), sys.stdout, silence)
    sys.sterr = mstream(collection, doc.copy(), sys.stderr, silence)

    logger = logging.getLogger('job')
    logger.setLevel(logging.INFO)
    hndlr = MongoHandler(collection, doc.copy())
    logger.addHandler(hndlr)


def now():
    now = datetime.utcnow()
    return now.replace(tzinfo=pytz.utc)

def nulltime():
    dt = datetime.utcfromtimestamp(0)
    return dt.replace(tzinfo=pytz.utc)

def config_dict(filename):
    config = {}
    if filename:
        return vars(import_string(filename))
    return config



def object_id(oid):
    try:
        return ObjectId(oid)
    except InvalidId:
        raise TypeError()


def wait_times(conn):
    coll = conn.queue_collection
    wait = { '$avg': { '$subtract':['$started_at_', '$enqueued_at_'] } }
    raw = coll.aggregate([{'$match':{'processed':True}}, {'$group':{'_id':'$qname', 'wait': wait } } ])
    result = raw['result']
    return {item['_id']:item['wait'] for item in result}

def job_stats(conn, group_by='$execute.func_str', since=None):
    coll = conn.queue_collection
    duration = { '$avg': { '$subtract':['$finished_at_', '$started_at_'] } }
    wait = { '$avg': { '$subtract':['$started_at_', '$enqueued_at_'] } }
    count = {'$sum': 1}
    failed = {'$sum': {'$cmp':['$failed', False]}}
    queues = {'$addToSet': '$qname'}
    tags = {'$addToSet': '$push'}
    erliest = {'$min': '$finished_at'}
    latest = {'$max': '$finished_at'}

    match = {'$match':{'finished':True}}

    if since:
        match['$match']['finished_at'] = {'$gt':since}

    raw = coll.aggregate([match, {'$group':{'_id':group_by,
                                           'duration': duration,
                                           'wait_in_queue': wait,
                                           'count': count,
                                           'queues': queues,
                                           'tags': tags,
                                           'failed':failed,
                                           'latest':latest,
                                           'erliest':erliest,
                                            } }
                          ])

    result = raw['result']

    return {item.pop('_id'):item for item in result}




def shutdown_worker(conn, worker_id=None):
    coll = conn.worker_collection
    query = {}
    if worker_id:
        query['_id'] = worker_id
    else:
        query = {'working':True}

    print(coll.update(query, {'$set':{'terminate':True}}, multi=True))

def last_job(conn, worker_id):
    coll = conn.queue_collection
    cursor = coll.find({'worker_id': worker_id}).sort('enqueued_at', -1)
    doc = next(cursor, None)

    if doc:
        from mtq.job import Job
        return Job(conn, doc)




