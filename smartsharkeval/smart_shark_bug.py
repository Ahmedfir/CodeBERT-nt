import logging
import sys
from typing import Dict, Set, List

from pydantic import BaseModel

from cb.json_locs_parser import VersionName
from codebertnt.locs_request import BusinessLocationsRequest
from ngram.tuna_fl_request import RemoteNgramFlRequest

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))
VERSIONS = ['f', 'b']  # fixme use VersionName instead
PICKLES_SUFFIX = '_json.pickle'


class Version(BaseModel):
    rev_id: str
    changes: Dict

    @staticmethod
    def new(commit):
        return Version(rev_id=commit.revision_hash, changes={})

    def add_change(self, file: str, lines: Set[int]):
        if file in self.changes:
            self.changes[file].update(lines)
        else:
            self.changes[file] = lines


class Bug(BaseModel):
    pid: str
    bid: str
    vcs_url: str
    fv: Version
    bv: Version

    def changes_by_version(self, version: VersionName):
        if version is VersionName.b:
            return self.bv.changes
        elif version is VersionName.f:
            return self.fv.changes
        else:
            raise 'Unhandled code version {0}'.format(version)

    def _create_buggy_tunafl_request(self, create_request_func, cbnat_locs_dir, repos_dir,
                                     force_reload, **kargs) -> RemoteNgramFlRequest:
        return create_request_func(self.vcs_url, self.bid, self.bv, 'b', cbnat_locs_dir, repos_dir,
                                   force_reload=force_reload, **kargs)

    def _create_fixed_tunafl_request(self, create_request_func, cbnat_locs_dir, repos_dir,
                                     force_reload, **kargs) -> RemoteNgramFlRequest:
        return create_request_func(self.vcs_url, self.bid, self.fv, 'f', cbnat_locs_dir, repos_dir,
                                   force_reload=force_reload, **kargs)

    def create_tunafl_requests(self, create_request_func, *args, versions=VERSIONS, **kargs) -> List[RemoteNgramFlRequest]:
        res = []
        if 'f' in versions:
            res.append(
                self._create_fixed_tunafl_request(create_request_func, *args, **kargs))
        if 'b' in versions:
            res.append(
                self._create_buggy_tunafl_request(create_request_func, *args, **kargs))
        return res

    def _create_buggy_cbnat_request(self, create_cbnat_request, cbnat_locs_dir, repos_dir, preds_output_dir, all_file,
                                    max_threads, force_reload) -> BusinessLocationsRequest:
        return create_cbnat_request(self.vcs_url, self.bid, self.bv, 'b', cbnat_locs_dir, repos_dir,
                                    preds_output_dir, all_file,
                                    max_threads=max_threads, force_reload=force_reload)

    def _create_fixed_cbnat_request(self, create_cbnat_request, cbnat_locs_dir, repos_dir, preds_output_dir, all_file,
                                    max_threads, force_reload) -> BusinessLocationsRequest:
        return create_cbnat_request(self.vcs_url, self.bid, self.fv, 'f', cbnat_locs_dir, repos_dir, preds_output_dir,
                                    all_file, max_threads=max_threads, force_reload=force_reload)

    def create_cbnat_requests(self, create_cbnat_request, *args, versions=VERSIONS) -> List[BusinessLocationsRequest]:
        res = []
        if 'f' in versions:
            res.append(
                self._create_fixed_cbnat_request(create_cbnat_request, *args))
        if 'b' in versions:
            res.append(
                self._create_buggy_cbnat_request(create_cbnat_request, *args))
        return res

    @staticmethod
    def parse_pickle(path: str):
        from commons.pickle_utils import load_zipped_pickle
        return Bug.parse_raw(load_zipped_pickle(path))

    @staticmethod
    def new(pid: str, bid, vcs_url: str, f_commit, b_commit):
        bug = Bug(pid=pid, bid=bid, vcs_url=vcs_url, fv=Version.new(f_commit), bv=Version.new(b_commit))
        from pycoshark.mongomodels import FileAction
        from pycoshark.mongomodels import File
        from pycoshark.mongomodels import Hunk
        for fa in FileAction.objects(commit_id=f_commit.id):


            file_path = File.objects(id=fa.file_id).get().path

            for hunk in Hunk.objects(file_action_id=fa.id):
                log.debug('hunk content: {0}'.format(hunk.content))
                log.debug('hunk new_start: {0}'.format(str(hunk.new_start)))
                log.debug('hunk old_start: {0}'.format(str(hunk.old_start)))
                log.debug('hunk new_lines: {0}'.format(str(hunk.new_lines)))
                log.debug(
                    'hunk new lines: {0}'.format(str(set(range(hunk.old_start, hunk.old_lines + hunk.old_start)))))
                log.debug(
                    'hunk old_lines: {0}'.format(str(set(range(hunk.new_start, hunk.new_lines + hunk.new_start)))))
                bug.bv.add_change(file_path, set(range(hunk.old_start, hunk.old_lines + hunk.old_start)))
                bug.fv.add_change(file_path, set(range(hunk.new_start, hunk.new_lines + hunk.new_start)))
        return bug


def changed_lines_by_bid(input_projects_dir: str, version: VersionName, force_reload=False) -> dict:
    # [Bug.parse_raw(load_zipped_pickle(join(project_dir, b))) for b in bugs_pickles]
    from os.path import join
    pickle_file = join(input_projects_dir, version.name + '_' + "all_diffs_dict.pickle")
    from os.path import isfile
    if force_reload or not isfile(pickle_file):
        from os import listdir
        res = {b_pickle.replace(PICKLES_SUFFIX, ''): Bug.parse_pickle(
            join(input_projects_dir, project_dir, b_pickle)).changes_by_version(version)
               for project_dir in listdir(input_projects_dir)
               for b_pickle in listdir(join(input_projects_dir, project_dir))}
        from commons.pickle_utils import save_zipped_pickle
        save_zipped_pickle(res, pickle_file)
    else:
        from commons.pickle_utils import load_zipped_pickle
        res = load_zipped_pickle(pickle_file)
    return res
