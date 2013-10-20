'''
Created on Oct 19, 2013

@author: sean
'''
from __future__ import print_function
from argparse import ArgumentParser
from mtq.connection import MTQConnection
from mtq.utils import config_dict, now
from bson import ObjectId
from time import mktime

def working(conn, args):
    coll = conn.worker_collection
    query = {'working':True}
    print('Bouncing %i workers' % coll.find(query).count())
    coll.update(query, {'$set':{'working':False}}, multi=True)
    print('Done')

def failed(conn, args):
    coll = conn.queue_collection
    query = {'failed':True}
    
    if args.func:
        query['execute.func_str'] = args.func
        
    if args.id:
        query['_id'] = args.id
    
    print('Flagging %i jobs as not failed' % coll.find(query).count())
    coll.update(query, {'$set':{'failed':False}}, multi=True)

def finish(conn, args):
    coll = conn.queue_collection
    query = {'finished':False}
    
    if args.id:
        query['_id'] = args.id
    print('Flagging %i jobs as finished' % coll.find(query).count())
    n = now()
    coll.update(query, {'$set':{'finished':True,
                                'finished_at': n,
                                'finished_at_': mktime(n.timetuple())}}, multi=True)
    
def shutdown(conn, args):
    coll = conn.worker_collection
    query = {'working':True}
    
    if args.all:
        pass
    if args.hostname:
        query['host'] = args.hostname
    if args.name:
        query['name'] = args.name
        
    if args.id:
        query['_id'] = args.id
    
    print('Shutting down %i workers' % coll.find(query).count())
    coll.update(query, {'$set':{'terminate':True}}, multi=True)
    print('Done')
    
    
def main():
    
    parser = ArgumentParser(description=__doc__, version='0.0')
    
    parser.add_argument('-c', '--config', help='Python module containing MTQ settings.')

    sp = parser.add_subparsers()
    
    wparser = sp.add_parser('working')
    wparser.set_defaults(main=working)
    
    fparser = sp.add_parser('failed')
    fparser.set_defaults(main=failed)
    group = fparser.add_mutually_exclusive_group(required=True)
    group.add_argument('-a', '--all', action='store_true')
    group.add_argument('-f', '--func')
    group.add_argument('-i', '--id', type=ObjectId)

    fparser = sp.add_parser('finish')
    fparser.set_defaults(main=finish)
    fparser.add_argument('-i', '--id', type=ObjectId, required=True)
    
    sparser = sp.add_parser('shutdown')
    sparser.set_defaults(main=shutdown)
    group = sparser.add_mutually_exclusive_group(required=True)
    group.add_argument('-a', '--all', action='store_true')
    group.add_argument('-k', '--hostname')
    group.add_argument('-n', '--name')
    group.add_argument('-i', '--id', type=ObjectId)
    
    
    
    args = parser.parse_args()
    
    config = config_dict(args.config)
    factory = MTQConnection.from_config(config)
    
    args.main(factory, args)
    
if __name__ == '__main__':
    main()
