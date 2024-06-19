import logging
import sys
from os import listdir
from os.path import join, isfile, isdir
from pathlib import Path

from utils.cmd_utils import shellCallTemplate
from utils.file_search import contains
from utils.git_utils import clone_checkout

BUSINESS_LOCATIONS_JAR = join(Path(__file__).parent, 'javabusinesslocs-1.2.2-SNAPSHOT-jar-with-dependencies.jar')
LOCATIONS_FILE_NAME = 'locations.json'
MUTANTS_OUTPUT_CSV = 'mutants_output.csv'

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))


class BusinessFileRequest:
    def __init__(self, file_path: str, lines: str = None):
        self.file_path = file_path
        self.lines = lines  # todo refactor : why is this string?

    def _abs_path(self, repo_path) -> str:
        abs_file_path = join(repo_path, self.file_path)
        if not isfile(abs_file_path):
            log.error('ignored not existing file {0}'.format(abs_file_path))
            return None
        else:
            return abs_file_path

    def to_str(self, repo_path: str) -> str:
        abs_file_path = self._abs_path(repo_path)
        if abs_file_path is None:
            return None
        res = "'-in=" + abs_file_path
        if self.lines is not None and len(self.lines) > 0:
            res = res + '::' + '@'.join(
                s.strip() for s in self.lines.replace('{', '').replace('}', '').split(','))
        res = res + "'"
        return res


class BusinessLocationsRequest:

    def __init__(self, file_requests, repo_path: str, output_dir: str,
                 locs_file=LOCATIONS_FILE_NAME, progress_file='p_log.out',
                 force_reload=False):
        self.repo_path: str = str(Path(repo_path).absolute())
        self.file_requests = file_requests
        self.output_dir = output_dir
        self.locs_output_file = join(output_dir, locs_file)
        self.force_reload = force_reload
        self.progress_file = progress_file

    def has_output(self) -> bool:
        return self.has_locs_output()

    def has_locs_output(self) -> bool:
        return isfile(self.locs_output_file)

    def to_str(self) -> str:
        fr = set(filter(None, {s.to_str(self.repo_path) for s in self.file_requests}))
        if len(fr) > 0:
            return " ".join(fr) + " " + "-out=" + self.output_dir
        else:
            log.error('BusinessLocationsRequest failed to collect any input files {0}'.format(self.repo_path))
            return None

    def preprocess(self) -> bool:
        return True

    def has_executed(self) -> bool:
        line_done = self.locs_output_file + ',' + 'exit'
        return self.progress_file is not None and contains(self.progress_file, line_done)

    def _print_progress(self, status, reason):
        if self.progress_file is not None:
            with open(self.progress_file, mode='a') as p_file:
                print(self.locs_output_file + ',' + status + ',' + reason, file=p_file)

    def _call_business_logic_jar(self, jdk_path: str, locs_jar_path: str) -> bool:
        request = self.to_str()
        if request is None:
            log.error('Empty request {0}'.format(self.repo_path))
            return False
        cmd = "JAVA_HOME='" + jdk_path + "' " + join(jdk_path, 'bin',
                                                     'java') + " -jar " + locs_jar_path + " " + request
        print("call business logic statements extraction cmd ... {0}".format(cmd))
        output = shellCallTemplate(cmd)
        log.info(output)
        return isfile(self.locs_output_file)

    def call(self, jdk_path: str, locs_jar_path: str = BUSINESS_LOCATIONS_JAR) -> str:
        self._print_progress('info', 'call')
        if not self.force_reload and self.has_output():
            self.on_exit('exit_has_output')
            return None
        if not self.preprocess():
            self.on_exit('exit_preprocess')
            return None
        if not self.has_locs_output():
            if not self._call_business_logic_jar(jdk_path, locs_jar_path):
                self.on_exit('exit_call_business_locs')
                return None
        self.postprocess(self.locs_output_file)
        self.on_exit('done')
        return self.locs_output_file

    def postprocess(self, locs_output_file):
        pass

    def on_exit(self, reason):
        self._print_progress('exit', reason)

    @staticmethod
    def call_static(req, jdk_path: str, locs_jar_path: str = BUSINESS_LOCATIONS_JAR):
        req.call(jdk_path, locs_jar_path)
        return req


class RemoteBusinessLocationsRequest(BusinessLocationsRequest):
    def __init__(self, vcs_url: str, rev_id: str, *args, **kargs):
        super(RemoteBusinessLocationsRequest, self).__init__(*args, **kargs)
        self.vcs_url = vcs_url
        self.rev_id = rev_id

    def preprocess(self) -> bool:
        # checkout project.
        from git import GitCommandError
        try:
            if not isdir(self.repo_path) or len(listdir(self.repo_path)) == 0:
                clone_checkout(self.vcs_url, self.repo_path, self.rev_id)
            return isdir(self.repo_path) and len(listdir(self.repo_path)) > 0
        except GitCommandError:
            log.error('failed to clone and checkout repo {0} {1}'.format(self.vcs_url, self.rev_id))
            import traceback
            traceback.print_exc()
            return False

    def on_exit(self, reason):
        super(RemoteBusinessLocationsRequest, self).on_exit(reason)
        if isdir(self.repo_path):
            import shutil
            shutil.rmtree(self.repo_path)
