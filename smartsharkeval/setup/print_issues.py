import logging
import sys
from os import makedirs
from os.path import join, isdir, isfile
from pathlib import Path

from mongoengine import connect
from pycoshark.mongomodels import Project, IssueSystem, Issue, Commit, CommitChanges, FileAction, Hunk, VCSSystem, File
from pycoshark.utils import create_mongodb_uri_string

from commons.pickle_utils import save_zipped_pickle
from smartsharkeval.smart_shark_bug import Bug, PICKLES_SUFFIX

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))

INPUT_DIR = join(Path(__file__).parent.parent, 'input')


def db_connect():
    # You may have to update this dict to match your DB credentials
    credentials = {'db_user': '',
                   'db_password': '',
                   'db_hostname': 'localhost',
                   'db_port': 27017,
                   'db_authentication_database': '',
                   'db_ssl_enabled': False}

    uri = create_mongodb_uri_string(**credentials)
    dbc = connect('smartshark_2_2', host=uri, alias='default')
    db = dbc.smartshark_2_2
    # db server sanity check
    log.info('db server sanity check : {0}'.format(db.command("serverStatus")))
    # db client
    log.info('db client : {0}'.format(dbc))
    # databases
    log.info('db client databases : {0}'.format(dbc.list_database_names()))
    # collections
    log.info('db collections : {0}'.format(db.list_collection_names()))
    return dbc


# filter:
# 1/ validated by researchers and developers
# 2/ has only 1 bug fixing commit
# load the commit changes by that fix commit.
def print_project_issues(pickle_path):
    # all repo urls that are on git4
    git_repo = [repo for repo in VCSSystem.objects(repository_type='git')]
    for repo in git_repo:
        project = Project.objects(id=repo.project_id).get()
        pid = project.name
        project_pickle_path = join(pickle_path, pid)
        if not isdir(project_pickle_path):
            try:
                makedirs(project_pickle_path)
            except FileExistsError:
                log.debug("two threads created the directory concurrently.")
        issue_trackers = IssueSystem.objects(project_id=project.id).all()
        for issue_tracker in issue_trackers:
            issues = Issue.objects(issue_system_id=issue_tracker.id).all()
            for issue in issues:
                if issue.issue_type is not None and issue.issue_type_verified is not None and issue.issue_type.lower() == 'bug' and issue.issue_type_verified.lower() == 'bug':
                    bid = issue.external_id
                    fix_commits_query = Commit.objects(
                        fixed_issue_ids=issue.id)  # or linked_issue_ids : we used combination of both
                    bug_fixing_commits = set()
                    for commit in fix_commits_query:
                        if commit.labels is not None and 'validated_bugfix' in commit.labels and commit.labels[
                            'validated_bugfix']:
                            bug_fixing_commits.add(commit.id)
                    if len(bug_fixing_commits) != 1 or CommitChanges.objects(
                            new_commit_id=list(bug_fixing_commits)[0]).count() != 1:
                        log.info('ignored {0}'.format(bid))
                        continue

                    bug_pickle_path = join(project_pickle_path, bid + PICKLES_SUFFIX)
                    if not isfile(bug_pickle_path):  # , '{0} is a duplicated because {1} exists'.format(bid,
                        # bug_pickle_path)
                        commit_changes = CommitChanges.objects(new_commit_id=list(bug_fixing_commits)[0]).get()
                        bug_fixing_commit = Commit.objects(id=commit_changes.new_commit_id).get()
                        buggy_version_commit = Commit.objects(id=commit_changes.old_commit_id).get()
                        bug = Bug.new(pid, bid, repo.url, bug_fixing_commit, buggy_version_commit)
                        # extract the changed files and lines
                        # extract hunks of the bug_fixing_commit
                        # File actions group all changed hunks in a commit of the same file
                        res_json = bug.json()
                        log.info('{0} - saving to pickle = {1}'.format(bid, bug_pickle_path))
                        save_zipped_pickle(res_json, bug_pickle_path)
                    else:
                        log.info('{0} is skipped because {1} exists'.format(bid, bug_pickle_path))


if __name__ == '__main__':
    bugs_dir = join(INPUT_DIR, 'bugs')

    dbc = db_connect()
    # print projects
    print_project_issues(bugs_dir)
