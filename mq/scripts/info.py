'''
Created on Aug 1, 2013

@author: sean
'''
from argparse import ArgumentParser

import mq
from mq.factory import MQFactory
import pymongo


def print_stats(factory, args):
    print 'Queues:'
    for queue in factory.queues:
        tags = queue.all_tags
        print ' * name:%s tags:[%s]' % (queue, ', '.join(tags))
        print '   count:%i' % queue.count
        for tag in tags:
            print '     + %10s:%i' % (tag, queue.tag_count(tag))
    print 
    print 'Workers:'
    for worker in factory.workers:
        print ' * %-10s %i' % (worker.name, worker.num_processed)
        print '   + Queues:[%s]' % ', '.join(worker.qnames)
        print '   + Tags:[%s]' % ', '.join(worker.tags)

def main():
    
    parser = ArgumentParser(description=__doc__, version='0.0')
    args = parser.parse_args()
    
    factory = MQFactory.from_config()
    print_stats(factory, args)
    
if __name__ == '__main__':
    main()
