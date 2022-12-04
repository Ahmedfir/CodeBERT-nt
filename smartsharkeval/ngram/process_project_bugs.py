import logging
import sys
from os import listdir, makedirs
from os.path import join, isdir

import numpy

from commons.pickle_utils import load_zipped_pickle
from commons.pool_executors import process_parallel_run
from ngram.tuna_fl_request import RemoteNgramFlRequest, FileRequest, MultiTokenizerRequest
from smartsharkeval.smart_shark_bug import Version, Bug

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))


def get_args():
    import argparse
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-project_dir', dest='project_dir',
                        help='directory containing the pickles of the bugs.')
    parser.add_argument('-locs_dir', dest='locs_dir',
                        help='output directory for the locs.')
    parser.add_argument('-repos_dir', dest='repos_dir',
                        help='directory where repos will be checked-out.')
    parser.add_argument('-jdk8', dest='jdk8',
                        help='path to java 8 home.')
    parser.add_argument('-max_processes', dest='max_processes', default=6, type=int,
                        help='max number of processes to run in parallel.')
    parser.add_argument('-force_reload', dest='force_reload', default=False,
                        help='force regenerate, already generated results.')
    parser.add_argument('-versions', dest='versions', default=['f', 'b'],
                        help="versions to process: 'b' for buggy and 'f' for fixed.")
    args = parser.parse_args()

    if args.project_dir is None or args.locs_dir is None or args.repos_dir is None or args.jdk8 is None:
        parser.print_help()
        raise AttributeError
    return args


def get_repo_path(repos_dir, version, bid):
    return join(repos_dir, version, bid)


ALIAS_GIT_URL_DICT = {'https://github.com/apache/wss4j.git': 'https://github.com/apache/ws-wss4j.git'}


def alias_vcs_url(vcs_url: str):
    return vcs_url if vcs_url not in ALIAS_GIT_URL_DICT else ALIAS_GIT_URL_DICT[vcs_url]


def create_tunafl_request(vcs_url: str, bid: str, v: Version, version: str, output_dir: str,
                          repos_dir: str, force_reload=False, tokenizers=None) -> RemoteNgramFlRequest:
    vcs_url = alias_vcs_url(vcs_url)
    locs_output_dir = join(output_dir, version, bid)
    progress_file = join(output_dir, version, 'progress.csv')
    if not isdir(locs_output_dir):
        makedirs(locs_output_dir)

    proj_repo_path = get_repo_path(repos_dir, version, bid)
    # filtering any eventual test class.
    reqs = {FileRequest(file)
            for file in v.changes if len(v.changes[file]) > 0 and file.endswith('.java') and 'test' not in file}
    return MultiTokenizerRequest(tokenizers, vcs_url, v.rev_id, reqs, proj_repo_path, locs_output_dir,
                                 force_reload=force_reload, progress_file=progress_file)


def get_reqs(project_dir: str, locs_dir: str, repos_dir: str, versions,
             force_reload=False, tokenizers=None):
    # bugs: listdir to get pickle paths
    bugs_pickles = listdir(project_dir)
    reqs = []
    if len(bugs_pickles) > 0:
        bugs = [Bug.parse_raw(load_zipped_pickle(join(project_dir, b))) for b in bugs_pickles]
        bugs_reqs = numpy.concatenate(
            [bug.create_tunafl_requests(create_tunafl_request, locs_dir, repos_dir, force_reload,
                                        versions=versions, tokenizers=tokenizers) for bug in bugs], axis=0)
        reqs = [req for req in bugs_reqs if not req.has_executed()]
    return reqs


if __name__ == '__main__':

    # receive path to the project dir
    args = get_args()
    # prepare requests to run on tunafl to extract locations.
    reqs = get_reqs(args.project_dir, args.locs_dir, args.repos_dir, args.versions, tokenizers=['JP', 'UTF8'])

    if len(reqs) > 0:
        # multi-process
        process_parallel_run(RemoteNgramFlRequest.call_static, reqs, args.jdk8, max_workers=args.max_processes)
