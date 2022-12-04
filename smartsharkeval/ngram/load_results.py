from os import listdir
from os.path import join, isfile

import pandas as pd
from pandas import DataFrame

from cb.json_locs_parser import VersionName  # fixme refactor cut dependency
from ngram.tuna_fl_request import JP_TUNA_FL_OUTPUT_CSV_FILE_NAME, UTF8_TUNA_FL_OUTPUT_CSV_FILE_NAME


def load_csv_to_df(req, version_filter):
    proj_bug_id = req[0]
    jp_csv_file = req[1]
    ut8_csv_file = req[2]
    changes = req[3]
    tokenizers = {'UTF8': ut8_csv_file, 'JP': jp_csv_file}
    columns_to_rename = ["entropy", "tokens_count"]
    columns_to_groupby = ["file", "line"]

    def load_df(tokenizer, file_path, columns):
        df = pd.read_csv(file_path)
        df.rename(columns={c: tokenizer + '_' + c for c in columns}, inplace=True)
        return df

    utf8_df = load_df('UTF8', tokenizers['UTF8'], columns_to_rename)
    jp_df = load_df('JP', tokenizers['JP'], columns_to_rename)

    df = pd.merge(utf8_df, jp_df, how="inner", left_on=columns_to_groupby, right_on=columns_to_groupby)
    df['proj_bug_id'] = proj_bug_id
    df['version'] = [version_filter(x.file, x.line, changes) for i, x in df.iterrows()]
    # remove repo from path.
    df['file'].apply(lambda x: ('/'.join(x.split('/')[2:])))
    return df


def load_tunafl_csvs(diffs, tunafl_output_dir, version: VersionName, version_filter, force_reload, max_processes) -> DataFrame:
    tunafl_df_pickle = join(tunafl_output_dir, version.name + '_all_ranked_lines_df.pickle')
    if force_reload or not isfile(tunafl_df_pickle):
        tunafl_output_dir_v = join(tunafl_output_dir, version.name)
        reqs = [(f, join(tunafl_output_dir_v, f, JP_TUNA_FL_OUTPUT_CSV_FILE_NAME),
                 join(tunafl_output_dir_v, f, UTF8_TUNA_FL_OUTPUT_CSV_FILE_NAME), diffs[f])

                for f in listdir(tunafl_output_dir_v)
                if isfile(join(tunafl_output_dir_v, f, JP_TUNA_FL_OUTPUT_CSV_FILE_NAME))
                and isfile(join(tunafl_output_dir_v, f, UTF8_TUNA_FL_OUTPUT_CSV_FILE_NAME))]

        from commons.pool_executors import process_parallel_run
        df = pd.concat(process_parallel_run(load_csv_to_df, reqs, version_filter, max_workers=max_processes,
                                            ignore_results=False))

        from commons.pickle_utils import save_zipped_pickle
        save_zipped_pickle(df, tunafl_df_pickle)
    else:
        from commons.pickle_utils import load_zipped_pickle
        df = load_zipped_pickle(tunafl_df_pickle)
    return df
