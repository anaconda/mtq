"""
Starts an MTQ worker.
"""
from argparse import ArgumentParser

from mtq.connection import MTQConnection
from mtq.log import ColorStreamHandler
from mtq.utils import config_dict, object_id
import logging
import mtq

logger = logging.getLogger('worker')


def aux(args):
    logger = logging.getLogger('mq.Worker')
    logger.setLevel(logging.INFO)
    hdlr = ColorStreamHandler()
    logger.addHandler(hdlr)

    config = config_dict(args.config)

    tags = config.get('TAGS', ()) or args.tags
    queues = config.get('QUEUES', ()) or args.queues

    factory = MTQConnection.from_config(config)
    worker = factory.new_worker(queues=queues, tags=tags, log_worker_output=args.log_output,
                                poll_interval=args.poll_interval, args=args)

    if args.backlog:
        print(worker.num_backlog)
        return

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

    worker.work(one=args.one, batch=args.batch, failed=args.failed)


def main():
    parser = ArgumentParser(description=__doc__, version='Mongo Task Queue (mtq) v%s'
                            % mtq.__version__, add_help=False)
    parser.add_argument('-c', '--config', help='Python module containing MTQ settings.')

    args, _ = parser.parse_known_args()
    config = config_dict(args.config)
    add_extra_arguments = config.get('extra_arguments')
    init_config = config.get('init')

    parser = ArgumentParser(description=__doc__,
                            version='Mongo Task Queue (mtq) v%s' % mtq.__version__, add_help=True)
    parser.add_argument('-c', '--config', help='Python module containing MTQ settings.')

    parser.add_argument('queues', nargs='*', default=['default'], help='The queues to listen on (default: %(default)r)')
    parser.add_argument('-r', '--reloader', action='store_true', help='Reload the worker when it detects a change')
    parser.add_argument('-p', '--poll-interval', help='Sleep interval to check for jobs', default=3, type=int)
    parser.add_argument('-t', '--tags', nargs='*', help='only process jobs which contain all of the tags', default=[])
    parser.add_argument('-l', '--log-output', action='store_true', help='Store job and woker ouput in the db, seealso mtq-tail')
    parser.add_argument('-1', '--one', action='store_true',
                        help='Process only the first job')
    parser.add_argument('--backlog', '--list-jobs', action='store_true',
                        help='List backlog of jobs and exit', dest='backlog')
    parser.add_argument('-b', '--batch', action='store_true',
                        help='Process jobs until the queue is empty, then exit')
    parser.add_argument('-j', '--job-id', type=object_id,
                        help='Process the job (even if it has already been processed)')
    parser.add_argument('-f', '--failed', action='store_true',
                        help='Process failed jobs')

    if add_extra_arguments:
        add_extra_arguments(parser)

    args = parser.parse_args()

    if init_config:
        init_config(args)

    if args.reloader:
        from werkzeug.serving import run_with_reloader
        run_with_reloader(lambda: aux(args))
    else:
        aux(args)


if __name__ == '__main__':
    main()
