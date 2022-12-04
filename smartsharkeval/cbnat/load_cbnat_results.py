from os import listdir
from os.path import join, isfile

import pandas as pd
from pandas import DataFrame

from cb import ListFileLocations
from cb.json_locs_parser import VersionName
from commons.pickle_utils import load_zipped_pickle, save_zipped_pickle
from commons.pool_executors import process_parallel_run
from cb import PREDICTIONS_FILE_NAME


def load_pickle_to_df(req, version_filter) -> DataFrame:
    proj_bug_id = req[0]
    pickle_file = req[1]
    changes = req[2]

    def line_filter(line):
        from cb.job_config import DEFAULT_JOB_CONFIG
        return DEFAULT_JOB_CONFIG.cosine_func == line.cos_func

    return ListFileLocations.parse_raw(load_zipped_pickle(pickle_file)).to_mutants_versionfilter(version_filter,
                                                                                                 line_filter,
                                                                                                 proj_bug_id,
                                                                                                 changes)


def load_preds_df(diffs, cbnat_preds_dir, version: VersionName, version_filter, force_reload,
                  max_processes):
    mutants_df_pickle = join(cbnat_preds_dir, version.name + '_all_mutants_df.pickle')
    if force_reload or not isfile(mutants_df_pickle):
        reqs = [(f, join(cbnat_preds_dir, version.name, f, PREDICTIONS_FILE_NAME), diffs[f])

                for f in listdir(join(cbnat_preds_dir, version.name)) if
                isfile(join(cbnat_preds_dir, version.name, f, PREDICTIONS_FILE_NAME))]

        df = pd.concat(process_parallel_run(load_pickle_to_df, reqs, version_filter, max_workers=max_processes,
                                            ignore_results=False))

        save_zipped_pickle(df, mutants_df_pickle)
    else:
        df = load_zipped_pickle(mutants_df_pickle)
    return df

