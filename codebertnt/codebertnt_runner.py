import logging
import os
import sys
from os.path import isdir, expanduser, isfile, join
from pathlib import Path

from cb.job_config import NOCOSINE_JOB_CONFIG, DEFAULT_JOB_CONFIG
from codebertnt.local_cbnt_request import LocalPredictBusinessLocations
from codebertnt.locs_request import BusinessFileRequest

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))


def get_args():
    import argparse
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-project_name', dest='project_name', default=None,
                        help='project name - by default the repo name.')
    parser.add_argument('-repo_path', dest='repo_path', help='target repo/project path.')
    parser.add_argument('-target_classes', dest='target_classes', help="classes to mutate separated by ','.")
    parser.add_argument('-output_dir', dest='output_dir', help="output directory.",
                        default='cbnat_output')
    parser.add_argument('-java_home', dest='java_home', help='java home path', default=os.getenv("JAVA_HOME"))
    parser.add_argument('-all_lines', dest='all_lines', default=True)
    parser.add_argument('-max_processes', dest='max_processes', default=16)
    parser.add_argument('-force_reload', dest='force_reload', default=False)
    parser.add_argument('-cosine', dest='cosine', default=False)

    args = parser.parse_args()

    if args.repo_path is None or not isdir(
            expanduser(args.repo_path)) or args.target_classes is None or len(
        args.target_classes.split(',')) == 0:
        parser.print_help()
        raise AttributeError

    if args.java_home is None or not isdir(args.java_home):
        print("could not load JAVA_HOME automatically.")
        parser.print_help()
        raise AttributeError

    return args


def create_local_request(files, repo_path, output_dir: str, job_config,
                         max_processes_number: int = 4) -> LocalPredictBusinessLocations:
    reqs = {BusinessFileRequest(file) for file in files}
    return LocalPredictBusinessLocations(output_dir, reqs, repo_path, output_dir, max_threads=max_processes_number,
                                         job_config=job_config)


def create_request(repo_path, target, output_dir, class_files,
                   max_processes, job_config) -> LocalPredictBusinessLocations:
    for c in class_files:
        if not isfile(join(repo_path, c)):
            log.error('target_classes should contain the path to the file from the project_path'
                      'such as project_path/target_class is the full path to the target_class.')
            raise AttributeError
    output_dir = join(expanduser(output_dir), target)
    if not isdir(output_dir):
        try:
            os.makedirs(output_dir)
        except FileExistsError:
            log.debug("two threads created the directory concurrently.")

    return create_local_request(class_files, repo_path, output_dir,
                                job_config, max_processes)


def str_to_bool(arg):
    return arg is None or (isinstance(arg, bool) and arg) or (isinstance(arg, str) and eval(arg))


if __name__ == '__main__':
    args = get_args()
    job_name = Path(args.repo_path).name if args.project_name is None else args.project_name
    files = args.target_classes.split(',')
    job_config = DEFAULT_JOB_CONFIG if str_to_bool(args.cosine) else NOCOSINE_JOB_CONFIG

    request: LocalPredictBusinessLocations = create_request(expanduser(args.repo_path),
                                                            job_name,
                                                            expanduser(args.output_dir),
                                                            files,
                                                            args.max_processes, job_config)

    df = request.get_lines_ordered_by_min_conf(expanduser(args.java_home))
    df.describe()
