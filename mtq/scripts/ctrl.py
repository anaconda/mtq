'''
Control workers and jobs
'''
from __future__ import print_function
from argparse import ArgumentParser
from mtq.connection import MTQConnection
from mtq.utils import config_dict, now
from bson import ObjectId
from time import mktime
import mtq

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
    update = {'$set':{'terminate':True,
                      'terminate_status':args.status,
                      'working': False}}
    coll.update(query, update, multi=True)
    print('Done')


def main():

    parser = ArgumentParser(description=__doc__, version='Mongo Task Queue (mtq) v%s' % mtq.__version__)

    parser.add_argument('-c', '--config', help='Python module containing MTQ settings.')

    sp = parser.add_subparsers()

    wparser = sp.add_parser('working',
                            help=('Set all workers as not working. '
                                  'Live workers will reset working status'
                                  'on next check-in'))
    wparser.set_defaults(main=working)

    fparser = sp.add_parser('failed', help='Flag failed jobs as fixed')
    fparser.set_defaults(main=failed)
    group = fparser.add_mutually_exclusive_group(required=True)
    group.add_argument('-a', '--all', action='store_true',
                       help='All jobs')
    group.add_argument('-f', '--func',
                       help='All jobs with function name')
    group.add_argument('-i', '--id', type=ObjectId,
                       help='Job Id')

    fparser = sp.add_parser('finish', help='Flag unfinished jobs as finished (use with caution)')
    fparser.set_defaults(main=finish)
    fparser.add_argument('-i', '--id',
                         type=ObjectId, required=True,
                         help='Job Id')

    sparser = sp.add_parser('shutdown',
                            help=('Schedule workers for shutdown. '
                                  'Workers will self-terminate on next check-in'))
    sparser.add_argument('-s', '--status', type=int,
                       help='Status code', default=1)

    sparser.set_defaults(main=shutdown)

    group = sparser.add_mutually_exclusive_group(required=True)

    group.add_argument('-a', '--all', action='store_true',
                       help='All workers')
    group.add_argument('-k', '--hostname',
                       help='Workers only running on host')
    group.add_argument('-n', '--name',
                       help='Worker name')
    group.add_argument('-i', '--id', type=ObjectId,
                       help='Worker Id')

    args = parser.parse_args()

    config = config_dict(args.config)
    factory = MTQConnection.from_config(config)

    args.main(factory, args)

if __name__ == '__main__':
    main()
