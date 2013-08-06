'''
Created on Aug 1, 2013

@author: sean
'''
from argparse import ArgumentParser
from mtq.connection import MTQConnection
from mtq.log import StreamHandler
from mtq.utils import config_dict, object_id
import logging

def aux(args):
    
    logger = logging.getLogger('mq.Worker')
    logger.setLevel(logging.INFO)
    hdlr = StreamHandler()
    logger.addHandler(hdlr)

    config = config_dict(args.config)
    tags = args.tags or config.get('TAGS', ())
    queues = args.queues or config.get('QUEUES', ())
    
    factory = MTQConnection.from_config(config)
    
    worker = factory.new_worker(queues=queues, tags=tags, log_worker_output=args.log_output)
    
    if config.get('exception_handler'):
        worker.push_exception_handler(config['exception_handler'])
    if config.get('pre_call'):
        worker._pre_call = config.get('pre_call')
    if config.get('post_call'):
        worker._post_call = config.get('post_call')

    if args.job_id:
        job = factory.get_job(args.job_id)
        if job is None:
            worker.logger.error('No job %s' % args.job_id)
            return
        
        worker.process_job(job)
        return
        
    worker.work(one=args.one, batch=args.batch)
    

def main():
    parser = ArgumentParser(description=__doc__, version='0.0')
    parser.add_argument('queues', nargs='*', default=['default'], help='The queues to listen on (default: %(default)r)')
    parser.add_argument('-r', '--reloader', action='store_true', help='Reload the worker when it detects a change')
    parser.add_argument('-t', '--tags', nargs='*', help='only process jobs which contain all of the tags')
    parser.add_argument('-c', '--config', help='Python module containing MTQ settings.')
    parser.add_argument('-l', '--log-output', action='store_true', help='Store job and woker ouput in the db, seealso mtq-tail')
    parser.add_argument('-1', '--one', action='store_true',
                        help='Process only the first job')
    parser.add_argument('-b', '--batch', action='store_true',
                        help='Process jobs until the queue is empty, then exit')
    parser.add_argument('-j', '--job-id', type=object_id,
                        help='Process the job (even if it has already been processed)')
    args = parser.parse_args()
    
    if args.reloader:
        from werkzeug.serving import run_with_reloader
        run_with_reloader(lambda: aux(args))
    else:
        aux(args)
    
    
if __name__ == '__main__':
    main()
