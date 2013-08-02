'''
Created on Aug 1, 2013

@author: sean
'''
from argparse import ArgumentParser

import mq
import pymongo


def main():
    
    parser = ArgumentParser(description=__doc__, version='0.0')
    args = parser.parse_args()
    
    cli = pymongo.MongoClient('mongodb://localhost/?journal=true')
    db = cli.test_queue 
    
    print 'Tags:'
    for tag in mq.Queue.all_tags(db):
        print ' * %-10s %i' % (tag, mq.Queue.tag_count(tag, db))
    print
    print 'Workers:'
    for worker in mq.Worker.all_workers(db):
        name = worker.get('name')
        print ' * %-10s %i' % (name, mq.Worker.processed_count(worker.get('_id'), db))
        print '   + Tags:', ', '.join(worker.get('tags'))
    
    queue = mq.Queue([], db) 

if __name__ == '__main__':
    main()
