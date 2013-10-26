'''
Created on Aug 1, 2013

@author: sean
'''
from mtq.utils import now
import io
import logging
import sys
import time

class ColorStreamHandler(logging.Handler):
    '''
    Handler to write to stdout with colored output 
    '''
    WARNING = '\033[93m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = "\033[1m"
    COLOR_MAP = {'ERROR': '%s%s[%%s]%s' % (BOLD, FAIL, ENDC),
                 'WARNING': '%s%s[%%s]%s' % (BOLD, WARNING, ENDC),
                 'DEBUG': '%s%s[%%s]%s' % (BOLD, OKBLUE, ENDC),
                 'INFO': '%s%s[%%s]%s' % (BOLD, OKBLUE, ENDC),
                 }
    
    def color_map(self, header, level):
        return self.COLOR_MAP.get(level, '[%s]') % header  
    
    def __init__(self, level=logging.INFO):
        logging.Handler.__init__(self, level=level)
    def emit(self, record):
        header = ''
        if record.levelno == logging.INFO:
            header = record.name
            message = record.getMessage()
            stream = sys.stdout
        else:
            stream = sys.stderr
            if record.exc_info:
                err = record.exc_info[1]
                header = '%s:%s' % (type(err).__name__, record.name)
                if err.args:
                    message = err.args[0]
                else:
                    message = str(err)
            else:
                header = record.name
                message = record.getMessage()
                
        if stream.isatty() and not sys.platform.startswith('win'):
            header = self.color_map(header, record.levelname)
        stream.write('%s %s\n' % (header, message))


class BSONFormatter(object):
    '''
    format recort to bison dict
    '''
    def __init__(self, *args, **extra_tags):
        object.__init__(self, *args)
        self.extra_tags = extra_tags
        
    def format(self, record):
        if isinstance(record.msg, dict):
            data = record.msg
        elif isinstance(record.msg, (list, tuple)):
            data = {'items': record.msg}
        else:
            data = {'message':'%s\n' % (record.msg,)}
            
        data.update(logLevel=record.levelname, logModule=record.module, logName=record.name, **self.extra_tags)
        return data 

class MongoHandler(logging.Handler):
    '''
    Log to monog db
    '''
    def __init__(self, collection, doc):
        
        logging.Handler.__init__(self, logging.INFO)
        
        self.collection = collection
        self.doc = doc
        self.setFormatter(BSONFormatter())
        
        
    def emit(self, record):
        doc = self.doc.copy()
        data = self.format(record)
        doc.update(data)
        self.collection.insert(doc)

class TextIOWrapperSmart(io.TextIOWrapper):
    '''
    allow print to io.TextIOWrapper
    '''
    def write(self, s):
        if isinstance(s, bytes):
            s = s.decode()
        return io.TextIOWrapper.write(self, s)

class IOStreamLogger(logging.Logger):
    def __init__(self, stream, silence=False):
        
        self.stream_name = name = getattr(stream, 'name', None)
        logging.Logger.__init__(self, name, level=logging.INFO)
        self.stream = stream
        self.silence = silence
        
        hdlr = logging.StreamHandler(stream)
        hdlr.setLevel(logging.INFO)
        self.addHandler(hdlr)
        
    def readable(self):
        return 'r' in self.stream.mode
    
    @property
    def closed(self):
        return self.stream.closed
    
    def writable(self):
        return 'w' in self.stream.mode or 'a' in self.stream.mode  

    def seekable(self):
        return False
    
    def write(self, message):
        self.info(message)
        return len(message)
    
    def flush(self):
        self.stream.flush()
    

class MongoStream(object):
    '''
    File like object to read/write to mongodb
    '''
    def __init__(self, collection, doc, stream=None, finished=None, silence=False):
        self.collection = collection
        self.doc = doc
        self.stream = stream
        self._finished = finished
        self.silence = silence
        self.stream_name = getattr(stream, 'name', None)
        
    def readable(self):
        return False
    
    @property
    def closed(self):
        return False
    
    def writable(self):
        return True

    def seekable(self):
        return False
    
    def write(self, message):
        doc = self.doc.copy()
        doc.update(message=message, time=now())
        
        self.collection.insert(doc)
        
        if self.stream and not self.silence:
            self.stream.write(message)
        return len(message)
    
    def flush(self):
        pass
    
    def loglines(self, follow=False):
        starting_finished = not follow or self._finished()
        
        cursor = self.collection.find(self.doc,
                                      await_data=not starting_finished,
                                      tailable=not starting_finished)
        while 1:
            for item in cursor:
                text = item.get('message', '')
                yield text
            
            if starting_finished or self._finished():
                return
            
            time.sleep(1)
            
        return
    
def mstream(collection, doc, stream=None, silence=False):
    '''
    Create a buffered mongo stream (good for print statements) 
    '''
    return TextIOWrapperSmart(MongoStream(collection, doc, sys.stdout, silence), line_buffering=True)
    
    

def add_all(loggers, handlers):
    for l in loggers:
        for h in handlers:
            l.addHandler(h)

def remove_all(loggers, handlers):
    for l in loggers:
        for h in handlers:
            l.addHandler(h)



