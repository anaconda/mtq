'''
Created on Aug 1, 2013

@author: sean
'''
from argparse import ArgumentParser

import mq
import pymongo
import os
import time


def main():
    parser = ArgumentParser(description=__doc__, version='0.0')
    parser.add_argument('tags', nargs='*')
    args = parser.parse_args()
    
    cli = pymongo.MongoClient('mongodb://localhost/?journal=true')
    db = cli.test_queue
     
    queue = mq.Queue(args.tags, db)
    
    print queue.count
    
    worker = mq.Worker(queue)
    worker.work()
    
if __name__ == '__main__':
    main()
