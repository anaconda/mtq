'''
Run a MTQ schedular or schedule a task

Schedule rule based on the iCal RFC 
    * http://labix.org/python-dateutil#head-e987b581aebacf25c7276d3e9214385a12a091f2
    * http://www.ietf.org/rfc/rfc2445.txt

Examples:

Run a single task now:

 mtq-scheduler --now mymodule.myfunction

Schedule a task to be run every hour:

 mtq-scheduler --add --task mymodule.myfunction --rule 'FREQ=HOURLY' 

Run a schedule server **should only ever be one running!**:
    
 mtq-scheduler --serve-forever   
'''

from __future__ import print_function
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from dateutil.rrule import rrulestr
from bson.objectid import ObjectId
from mtq.utils import config_dict
import mtq
import logging
from mtq.log import ColorStreamHandler


def test_rule(rule_str):
    try:
        _ = rrulestr(rule_str)
    except ValueError as err:
        raise SystemExit("Bad rule:" + err.message)


def pprint_scheduler(scheduler):
    rules = scheduler.rules
    if not rules.count():
        print('No Scheduled tasks')
        return 
    for rule in rules:
        print('%(_id)s | %(rule)30s | %(task)20s | %(queue)s' % rule)


def main():

    logger = logging.getLogger('mtq')
    logger.setLevel(logging.INFO)
    hdlr = ColorStreamHandler()
    logger.addHandler(hdlr)

    parser = ArgumentParser(description=__doc__, version='Mongo Task Queue (mtq) v%s' % mtq.__version__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--config', help='Python module containing MTQ settings.')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-a', '--add', help='Schedule a new job', action='store_true')
    group.add_argument('-n', '--now', help='Run a job now!', metavar='TASK')
    group.add_argument('--remove', help='Remove a rule', type=ObjectId)
    group.add_argument('-u', '--update', help='Update a rule', type=ObjectId)
    group.add_argument('-l', '--list', help='List rules', action='store_true')
    group.add_argument('-s', '--serve-forever', '--run', help='List rules', action='store_true',
                       dest='run')
    parser.add_argument('-r', '--rule', help='Schedule rule based on the iCal RFC (http://www.ietf.org/rfc/rfc2445.txt)')
    parser.add_argument('-t', '--task', help='importable string of the task to be run. Must be a callable object with no arguments')
    parser.add_argument('-q', '--queue', help='name of the queue (default: default)')
    parser.add_argument('--tags', help='tag the job with these tags')
    parser.add_argument('--timeout', type=int, default=None,
                        help='Timeout after N seconds', metavar='N')
    args = parser.parse_args()
    
    config = config_dict(args.config)
    factory = mtq.from_config(config)
        
    scheduler = factory.scheduler()
    
    if args.rule:
        test_rule(args.rule)
        
    if args.add:
        if args.task is None:
            raise Exception('must specify task')
        _id = scheduler.add_job(args.rule, args.task, args.queue or 'default', args.tags, args.timeout)
        print('Added new scheduled task _id=%s' % (_id,))
    if args.update:
        scheduler.update_job(args.update, args.rule, args.task, args.queue, args.tags)
    elif args.list:
        pprint_scheduler(scheduler)
    elif args.remove:
        scheduler.remove_job(args.remove)
        print('Remove scheduled task _id=%s' % (args.remove,))
    elif args.now:
        queue = factory.queue(args.queue, tags=args.tags)
        queue.enqueue_call(args.now, timeout=args.timeout)
    elif args.run:
        scheduler.run()
    
        
        

if __name__ == '__main__':
    main()
