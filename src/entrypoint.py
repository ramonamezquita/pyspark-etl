import argparse
import os
import sys


class JobResolver:
    def __init__(self, parsed_args):
        self.parsed_args = parsed_args

    def resolve(self):
        pass

    def get_file_params(self):
        pass

    def get_job(self):
        pass


if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

parser = argparse.ArgumentParser(description='Executes spark jobs.')
parser.add_argument('-j', '--job', type=str, required=True, help='Job name')
parser.add_argument('--file-arg', nargs='*')
parsed_args = parser.parse_args()

job = JobResolver(parsed_args).resolve()
job.execute()
