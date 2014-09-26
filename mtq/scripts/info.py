'''
Print stats on workers, queues and jobs
'''
from __future__ import print_function, division
from argparse import ArgumentParser
from mtq.connection import MTQConnection
from mtq.utils import config_dict, wait_times, last_job, now, job_stats
from datetime import timedelta, datetime

def reltime(dt):
    delta = now() - dt
    s = delta.total_seconds()

    hours = s / 3600.
    if hours > 1.5:
        return '%.1f hours ago' % hours

    minutes = s / 60.0
    if minutes > 1.5:
        return '%.1f minutes ago' % minutes
    return '%.1f seconds ago' % s

def queue_stats(factory, args):
    print('Queues:')
    latancy = wait_times(factory)
    for queue in factory.queues:
        tags = queue.all_tags
        print(' * Name: %s' % (queue.name))
        print('   Tags: [%s]' % (', '.join(tags)))
        print('   Count: %i' % queue.count)
        print('   Latancy: %.2f seconds' % latancy.get(queue.name, -1))

t = lambda tm:  (tm.ctime(), reltime(tm))

def worker_stats(factory, args):
    print('Workers:')
    for worker in factory.workers:
        print(' * %-10s' % (worker.name))
        print('   + Last Checked In: %s (%s)' % t(worker.last_check_in))
        print('   + Num-Processed: %i' % worker.num_processed)
        print('   + Backlog: %i' % worker.num_backlog)
        print('   + Queues:[%s]' % ', '.join(worker.qnames))
        print('   + Tags:[%s]' % ', '.join(worker.tags))
        job = last_job(factory, worker.id)
        if job:
            print('   + Last Job:')
            print('      - Id: %s' % job.doc['_id'])
            print('      - Enqueued: %s (%s)' % t(job.enqueued))
            print('      - Started : %s (%s)' % t(job.started))

            if job.doc.get('finished'):
                print('      - Finished: %s (%s)' % t(job.doc['finished_at']))
            else:
                print('      - Finished:  *In-Progress*')
        else:
            print('   *** Worker has not processed any jobs')

def print_job_stats(factory, args):

    stats = job_stats(factory, since=args.since)
    for key, value in stats.items():
        print('+', key)
        for item in value.items():
            if isinstance(item[1], datetime):
                item = item[0], '%s (%s)' % t(item[1])
            else:
                item = item[0], repr(item[1])

            print('   - %s: %s' % item)

    cursor = factory.queue_collection.find({'finished':False})
    if cursor.count():
        print()
        print('Unfinished Jobs:')
        for job in cursor:
            if job['processed']:
                print(' * %s %s -- started %s, enqueued %s' % (job['execute']['func_str'], job['_id'], reltime(job['started_at']), reltime(job['enqueued_at'])))
            else:
                print(' * %s %s -- **waiting**, enqueued %s' % (job['execute']['func_str'], job['_id'], reltime(job['enqueued_at'])))

    cursor = factory.queue_collection.find({'failed':True})
    if cursor.count():
        print()
        print('Failed Jobs:')
        for job in cursor:
            print(' *', job['execute']['func_str'], job['_id'])


def max_age(arg):
    if arg.lower().endswith('s'):
        return now() - timedelta(0, int(arg[:-1]))
    elif arg.lower().endswith('m'):
        return now() - timedelta(0, int(arg[:-1]) * 60)
    elif arg.lower().endswith('h'):
        return now() - timedelta(0, int(arg[:-1]) * 60 * 60)
    elif arg.lower().endswith('d'):
        return now() - timedelta(int(arg[:-1]))
    else:
        return now() - timedelta(0, int(arg))

def print_db_stats(factory, args):
    import pymongo
    print('PyMongo', pymongo.version)
    print('Mongo database', factory.db.name)
    for collection_names in factory.db.collection_names():
        stats = factory.db.command('collStats', collection_names, scale=1024)
        if stats.get('capped'):
            print(" * %(ns)s (%(count)i items)" % stats)
            print("   Using %(size)s of %(storageSize)s Kb" % stats,
                  '%.2f%%' % (100 * stats['size'] / stats['storageSize']),
                  '(capped)' if stats.get('capped') else '',
                  )


def main():

    parser = ArgumentParser(description=__doc__, version='0.0')
    parser.add_argument('-c', '--config', help='Python module containing MTQ settings.')
    parser.add_argument('-m', '--max-age',
                        help='Maximum age of jobs (e.g. 1d) unit may be one of s (seconds), m (minutes), h (hours) or d (days) ',
                        type=max_age, dest='since', metavar='AGE',
                        default=None)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-q', '--queues', action='store_const',
                       const=queue_stats, dest='action',
                       help='print stats on queues')
    group.add_argument('-w', '--worker', action='store_const', const=worker_stats, dest='action',
                       help='print stats on workers')
    group.add_argument('-j', '--jobs', action='store_const', const=print_job_stats, dest='action',
                       help='print stats on jobs')
    group.add_argument('-s', '--storage', '--db', action='store_const', const=print_db_stats, dest='action',
                       help='print stats on database')

    args = parser.parse_args()

    config = config_dict(args.config)
    factory = MTQConnection.from_config(config)
    args.action(factory, args)

if __name__ == '__main__':
    main()
