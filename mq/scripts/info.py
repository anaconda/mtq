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
    
    print mq.Queue.all_tags(db) 
    queue = mq.Queue(['default'], db) 
    print queue.count 
    print queue.collection.count()

if __name__ == '__main__':
    main()