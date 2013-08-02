'''
Created on Aug 1, 2013

@author: sean
'''
from argparse import ArgumentParser, FileType

import mq
import pymongo
import os
import time
from mq.utils import import_string
from bson.objectid import ObjectId

config = {
          'DB_HOST':'mongodb://localhost/?journal=true',
          'DB_NAME':'test_queue',
          'TAGS':['default'],         
          }

def main():
    parser = ArgumentParser(description=__doc__, version='0.0')
    parser.add_argument('tags', nargs='*')
    parser.add_argument('-c', '--config')
    parser.add_argument('-1', '--one', action='store_true', 
                        help='Process only the first job')
    parser.add_argument('-b', '--batch', action='store_true', 
                        help='Process jobs until the queue is empty, then exit')
    parser.add_argument('-j', '--job-id', type=ObjectId, 
                        help='Process the job (even if it has already been processed)')
    args = parser.parse_args()
    
    if args.config:
        execfile(args.config, config, config)
    
    cli = pymongo.MongoClient(config['DB_HOST'])
    db = getattr(cli, config['DB_NAME'])
    tags = args.tags or config['TAGS']
    
    queue = mq.Queue(tags, db)
    worker = mq.Worker(queue)
    
    if config.get('exception_handler'):
        worker.push_exception_handler(config['exception_handler'])
    if config.get('pre_call'):
        worker._pre_call = config.get('pre_call')
    if config.get('post_call'):
        worker._post_call = config.get('post_call')

    if args.job_id:
        job = queue.get_job(args.job_id, db)
        if job is None:
            worker.logger.error('No job %s' % args.job_id)
            return
        worker.process_job(job)
        return
        
    worker.work(one=args.one, batch=args.batch)
    
if __name__ == '__main__':
    main()
