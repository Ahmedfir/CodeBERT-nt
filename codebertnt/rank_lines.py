from os import makedirs
from os.path import isfile, isdir, join

import pandas as pd

from cb import ListFileLocations
from commons.pickle_utils import load_zipped_pickle, save_zipped_pickle

INTERMEDIATE_PICKLE_SUFFIX = '_cbnt.pickle'
FL_COLUMN = 'fl'


def order_lines_from_pickle(predictions_file_project_name_tuple, intermediate_dir=None, version='f', preds_per_token=1,
                            fl_column=FL_COLUMN, force_reload=False):
    predictions_file = predictions_file_project_name_tuple[0]
    project_name = predictions_file_project_name_tuple[1]
    if intermediate_dir is not None:
        interm_pickle_file = join(intermediate_dir, project_name + INTERMEDIATE_PICKLE_SUFFIX)
        if isfile(interm_pickle_file) and not force_reload:
            return load_zipped_pickle(interm_pickle_file)
    assert isfile(predictions_file)
    normal_mutants_df = ListFileLocations.parse_raw(load_zipped_pickle(predictions_file)).to_mutants(
        project_name, version, exclude_matching=False, no_duplicates=False)
    result_df = order_lines_by_naturalness(normal_mutants_df, preds_per_token, fl_column)
    if intermediate_dir is not None:
        if not isdir(intermediate_dir):
            makedirs(intermediate_dir)
        save_zipped_pickle(result_df, interm_pickle_file)
    return result_df


def order_lines_by_naturalness(predictions_df, preds_per_token=1, fl_column=FL_COLUMN):
    predictions_df = predictions_df[predictions_df['rank'] <= preds_per_token]
    predictions_df[fl_column] = predictions_df['file_path'] + predictions_df['line'].astype(str)
    line_score_column = str(preds_per_token) + 'score_min'
    result_df = predictions_df.groupby(['proj_bug_id', fl_column, 'version']).apply(
        lambda x: pd.Series({line_score_column: x['score'].min()})).reset_index()
    result_df.sort_values(by=[line_score_column], inplace=True)
    return result_df
