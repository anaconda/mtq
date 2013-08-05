'''
Created on Aug 1, 2013

@author: sean
'''
import signal
import traceback
import sys
import logging
from datetime import datetime
from dateutil.tz import tzlocal

class ImportStringError(Exception):
    pass


def handle_signals():
    '''
    Handle signals in multiprocess.Process threads
    '''
    def handler(signum, frame):
        signal.signal(signal.SIGINT, signal.default_int_handler)
        
    def term_handler(signum, frame):
        traceback.print_stack(frame)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        raise SystemExit(-signum)
        
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, term_handler)

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

def setup_logging(worker_id, job_id, collection):
    '''
    set up logging for worker
    '''
    from mtq.log import mstream, MongoHandler
    
    doc = {'worker_id':worker_id, 'job_id':job_id}
    sys.stdout = mstream(collection, doc.copy(), sys.stdout)
    sys.sterr = mstream(collection, doc.copy(), sys.stderr)
    
    logger = logging.getLogger('job')
    logger.setLevel(logging.INFO) 
    hndlr = MongoHandler(collection, doc.copy())
    logger.addHandler(hndlr)
    
    
def now():
    return datetime.now(tzlocal())    
    

