import argparse
import importlib
import os
import sys

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

parser = argparse.ArgumentParser(description='Executes spark jobs.')
parser.add_argument('-j', '--job', type=str, required=True, help='Job name')
args = parser.parse_args()

job_module = importlib.import_module('jobs.%s' % args.job)
job_module.execute()
