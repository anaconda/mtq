'''
Created on Aug 1, 2013

@author: sean
'''
from argparse import ArgumentParser
from mtq.connection import MTQConnection
from bson.objectid import ObjectId
import sys



def main():
    
    parser = ArgumentParser(description=__doc__, version='0.0')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-j','--job-id', type=ObjectId)
    group.add_argument('-w','--worker-name')
    group.add_argument('--worker-id', type=ObjectId)
    
    parser.add_argument('-f', '--follow', action='store_true', 
                       help=('Dont stop when end of file is reached, but '
                             'rather to wait for additional data to be'
                             'appended to the input.'))
    
    args = parser.parse_args()
    
    factory = MTQConnection.from_config()
    
    if args.job_id:
        stream = factory.job_stream(args.job_id)
    elif args.worker_name or  args.worker_id:
        stream = factory.worker_stream(args.worker_name, args.worker_id)
        
    try:
        for line in stream.loglines(args.follow):
            sys.stdout.write(line)
    except KeyboardInterrupt:
        pass
    
        
    
if __name__ == '__main__':
    main()


