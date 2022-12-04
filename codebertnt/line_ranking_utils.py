import logging

import numpy as np
PID_BID = 'proj_bug_id'
P_1ST_HIT_END = "_p_1h"
P_MEAN_END = "_p_m"
FL_C = 'fl'


def _get_order(df, sorting_column, group_by=PID_BID):
    return df[sorting_column].ne(df[sorting_column].shift()).groupby(df[group_by]).cumsum()


def _sort_by(df, sorting_col, output_col, ascending=True, group_by=PID_BID):
    assert ascending, 'not sure this would work with descending order. ' \
                      'a quick hack would be to reverse the sign of the column values: df[column] = -df[column]'
    df = df.sort_values(by=sorting_col, ascending=ascending)
    df = df.reset_index(drop=True)
    df[output_col] = _get_order(df, sorting_col, group_by=group_by)
    return df


def _random_1st_hit(items_count, size):
    return (1.0 / (items_count + 1.0)) * size


def _calculate_1st_hit_line_considering_rand(sub_df, sorting_column_rank, hit_rank, v_col='version', fl_col=FL_C):
    additive_by_rand = 1

    hit_files = sub_df[sub_df[sorting_column_rank] == hit_rank]
    hit_files_count = hit_files[fl_col].nunique()
    if hit_files_count > 1:
        hit_b_files_count = hit_files[hit_files[v_col] == 'b'][fl_col].nunique()
        if hit_b_files_count != hit_files_count:
            additive_by_rand = _random_1st_hit(hit_b_files_count, hit_files_count)

    return sub_df[sub_df[sorting_column_rank] < hit_rank][fl_col].nunique() + additive_by_rand


def _calculate_mean_line_considering_rand(sub_df, buggy_lines_ranks, sorting_column_rank, fl_column=FL_C):
    additive_by_rand = 1
    final_ranks = []
    for rank in buggy_lines_ranks:
        files_same_rank = sub_df[sub_df[sorting_column_rank] == rank]
        files_same_rank_count = files_same_rank[fl_column].nunique()
        if files_same_rank_count > 2:
            additive_by_rand = files_same_rank_count / 2
        final_ranks.append(sub_df[sub_df[sorting_column_rank] < rank][fl_column].nunique() + additive_by_rand)
    return np.mean(final_ranks)


def _add_first_hitpercent_column(count_files_df, mean_byline_df, sorting_column_rank, output_column,
                                 gb_col=PID_BID, v_col='version', fl_col=FL_C):
    output_column_percent_1st_hit = output_column + P_1ST_HIT_END
    output_column_percent_mean = output_column + P_MEAN_END
    buggy_lines_df = mean_byline_df[mean_byline_df[v_col] == 'b']

    count_files_df = count_files_df.assign(
        first_hit_line=lambda x: [
            _calculate_1st_hit_line_considering_rand(mean_byline_df[bid == mean_byline_df[gb_col]],
                                                     sorting_column_rank, buggy_lines_df[
                                                         bid == buggy_lines_df[gb_col]][
                                                         sorting_column_rank].min())
            for bid in x[gb_col]],
        mean_buggy_line_rank=lambda x: [
            _calculate_mean_line_considering_rand(mean_byline_df[bid == mean_byline_df[gb_col]],
                                                  buggy_lines_df[bid == buggy_lines_df[gb_col]][
                                                      sorting_column_rank],
                                                  sorting_column_rank)

            for bid in x[gb_col]]
    )
    count_files_df[output_column] = count_files_df['first_hit_line']
    count_files_df[output_column_percent_1st_hit] = count_files_df['first_hit_line'].astype(float) / count_files_df[
        fl_col].astype(float)
    count_files_df[output_column_percent_mean] = count_files_df['mean_buggy_line_rank'].astype(float) / count_files_df[
        fl_col].astype(float)
    del count_files_df['first_hit_line']
    del count_files_df['mean_buggy_line_rank']
    return count_files_df


def sort_by_col(df_mean, df_count, sorting_col, res_col, ascending=True):
    from utils.delta_time_printer import DeltaTime
    dt = DeltaTime(logging_level=logging.DEBUG)
    df_mean_tmp = _sort_by(df_mean.copy(), sorting_col, 'tmp_order', ascending=ascending)
    dt.print('sorting')
    df_count = _add_first_hitpercent_column(df_count, df_mean_tmp, 'tmp_order', res_col)
    dt.print('added 1st hit')
    return df_count


def calc_entropy(floats) -> float:
    return -(np.sum(np.log10(floats)) / float(len(floats)))
