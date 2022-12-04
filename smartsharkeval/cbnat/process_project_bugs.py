import logging
import sys
from os import listdir, makedirs
from os.path import join, isdir

import numpy

from codebertnt.locs_request import BusinessFileRequest, BusinessLocationsRequest
from codebertnt.remote_cbnt_request import RemotePredictBusinessLocations
from commons.pickle_utils import load_zipped_pickle
from commons.pool_executors import process_parallel_run
from smartsharkeval.smart_shark_bug import Bug
from smartsharkeval.smart_shark_bug import Version

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))


def get_args():
    import argparse
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-project_dir', dest='project_dir',
                        help='directory containing the pickles of the bugs.')
    parser.add_argument('-cbnat_locs_dir', dest='cbnat_locs_dir',
                        help='output directory for the locs.')
    parser.add_argument('-cbnat_preds_dir', dest='cbnat_preds_dir',
                        help='output directory for the predictions (mutants).')
    parser.add_argument('-repos_dir', dest='repos_dir',
                        help='directory where repos will be checked-out.')
    parser.add_argument('-jdk8', dest='jdk8',
                        help='path to java 8 home.')
    parser.add_argument('-max_processes', dest='max_processes', default=4, type=int,
                        help='max number of processes to run in parallel.')
    parser.add_argument('-max_threads', dest='max_threads', default=3, type=int,
                        help='max number of threads to run in parallel by process.')
    parser.add_argument('-force_reload', dest='force_reload', default=False,
                        help='force regenerate, already generated results.')
    parser.add_argument('-versions', dest='versions', default=['f', 'b'],
                        help="versions to process: 'b' for buggy and 'f' for fixed.")
    parser.add_argument('-all_file', dest='all_file', default=0, type=int,
                        help="includes all lines of the files and not only the changed ones.")
    args = parser.parse_args()

    if args.project_dir is None or args.cbnat_locs_dir is None or args.repos_dir is None or args.jdk8 is None:
        parser.print_help()
        raise AttributeError
    return args


def get_repo_path(repos_dir, version, bid):
    return join(repos_dir, version, bid)


ALIAS_GIT_URL_DICT = {'https://github.com/apache/wss4j.git': 'https://github.com/apache/ws-wss4j.git'}


def alias_vcs_url(vcs_url: str):
    return vcs_url if vcs_url not in ALIAS_GIT_URL_DICT else ALIAS_GIT_URL_DICT[vcs_url]


def create_cbnat_request(vcs_url: str, bid: str, v: Version, version: str, mbet_output_dir: str,
                         repos_dir: str, preds_output_dir: str, all_file: bool, max_threads=4,
                         force_reload=False) -> BusinessLocationsRequest:
    vcs_url = alias_vcs_url(vcs_url)
    locs_output_dir = join(mbet_output_dir, version, bid)
    progress_file = join(mbet_output_dir, version, 'progress.csv')
    if not isdir(locs_output_dir):
        makedirs(locs_output_dir)
    preds_dir = join(preds_output_dir, version, bid)
    if not isdir(preds_dir):
        makedirs(preds_dir)
    proj_repo_path = get_repo_path(repos_dir, version, bid)
    # filtering any eventual test class.

    reqs = {BusinessFileRequest(file, None if all_file else str(set(v.changes[file])))
            for file in v.changes if len(v.changes[file]) > 0 and file.endswith('.java') and 'test' not in file}
    return RemotePredictBusinessLocations(preds_dir, vcs_url, v.rev_id, reqs, proj_repo_path, locs_output_dir,
                                          max_threads=max_threads, force_reload=force_reload,
                                          progress_file=progress_file)


def get_reqs(project_dir: str, cbnat_locs_dir: str, repos_dir: str, preds_output_dir: str, all_file, versions,
             max_threads=4, force_reload=False):
    # bugs: listdir to get pickle paths
    bugs_pickles = listdir(project_dir)
    reqs = []
    if len(bugs_pickles) > 0:
        bugs = [Bug.parse_raw(load_zipped_pickle(join(project_dir, b))) for b in bugs_pickles]
        bugs_reqs = numpy.concatenate(
            [bug.create_cbnat_requests(create_cbnat_request, cbnat_locs_dir, repos_dir, preds_output_dir, all_file,
                                       max_threads, force_reload, versions=versions) for bug in bugs], axis=0)
        reqs = [req for req in bugs_reqs if not req.has_executed()]
    return reqs


if __name__ == '__main__':

    # receive path to the project dir
    args = get_args()
    # prepare requests to run on cbnat to extract locations.
    reqs = get_reqs(args.project_dir, args.cbnat_locs_dir, args.repos_dir, args.cbnat_preds_dir,
                    args.all_file, args.versions, max_threads=args.max_threads)

    if len(reqs) > 0:
        # multi-process
        process_parallel_run(BusinessLocationsRequest.call_static, reqs, args.jdk8,
                             max_workers=args.max_processes)
