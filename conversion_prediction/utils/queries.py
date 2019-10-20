import re
import pandas as pd
# TODO: Look into unifying TEXT and Text
from sqlalchemy.types import TIMESTAMP, Float, DATE, ARRAY, TEXT, Text
from sqlalchemy.sql.expression import literal, extract
from sqlalchemy.sql import select
from sqlalchemy import and_, func, case
from sqlalchemy.sql.expression import cast
from datetime import timedelta, datetime
from .db_utils import get_aggregated_browser_days_w_session
from .config import DERIVED_METRICS_CONFIG, JSON_COLUMNS
from sqlalchemy.dialects.postgresql import ARRAY
from typing import List

aggregated_browser_days, session = get_aggregated_browser_days_w_session()


def get_feature_frame_via_sqlalchemy(
    start_time: datetime,
    end_time: datetime,
    moving_window_length: int=7
):
    full_query_current_data = get_full_features_query(
        start_time,
        end_time,
        moving_window_length,
        False
    )

    full_query_past_positives = get_full_features_query(
        start_time,
        end_time,
        moving_window_length,
        True
    )
    
    full_query = full_query_current_data.union(full_query_past_positives)

    feature_frame = pd.read_sql(full_query.statement, full_query.session.bind)
    feature_frame['is_active_on_date'] = feature_frame['is_active_on_date'].astype(bool)
    feature_frame['date'] = pd.to_datetime(feature_frame['date']).dt.date
    feature_frame.drop('date_w_gaps', axis=1, inplace=True)    

    return feature_frame


def get_full_features_query(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int=7,
        retrieving_past_positives: bool=False
):
    if not retrieving_past_positives:
        filtered_data = get_filtered_cte(start_time, end_time)
    else:
        filtered_data = get_data_for_windows_before_conversion(start_time, moving_window_length)

    all_date_browser_combinations = get_subqueries_for_non_gapped_time_series(
        filtered_data,
        start_time,
        end_time
    )

    unique_events = get_unique_events_subquery(filtered_data)

    device_information = get_device_information_subquery(filtered_data)

    filtered_data_with_unpacked_json_fields, json_key_column_names = unpack_json_fields(filtered_data)

    joined_partial_queries = join_all_partial_queries(
        filtered_data_with_unpacked_json_fields,
        all_date_browser_combinations,
        unique_events,
        device_information,
        json_key_column_names
    )

    data_with_rolling_windows = calculate_rolling_windows_features(
        joined_partial_queries,
        moving_window_length,
        start_time,
        json_key_column_names
    )

    filtered_w_derived_metrics = filter_joined_queries_adding_derived_metrics(
        data_with_rolling_windows,
        start_time,
        retrieving_past_positives
    )

    all_time_delta_columns = add_all_time_delta_columns(
        filtered_w_derived_metrics
    )

    return all_time_delta_columns


def get_filtered_cte(
    start_time: datetime,
    end_time: datetime,
):
    filtered_data = session.query(
        aggregated_browser_days).filter(
        aggregated_browser_days.c['date'] >= cast(start_time, TIMESTAMP),
        aggregated_browser_days.c['date'] <= cast(end_time, TIMESTAMP)
    ).cte(
        name="time_filtered_aggregations"
    )

    return filtered_data


def get_data_for_windows_before_conversion(
        start_time: datetime,
        moving_window_length: int=7
):
    conversion_events = session.query(
        aggregated_browser_days.c['browser_id'],
        aggregated_browser_days.c['next_7_days_event'],
        aggregated_browser_days.c['next_event_time'],
    ).filter(
        aggregated_browser_days.c['next_7_days_event'].in_(['conversion', 'shared_account_login']),
        aggregated_browser_days.c['date'] < start_time,
        # This restriction is here because of lack of resources on bonne, we can lift it on Hetzner (possibly)
        aggregated_browser_days.c['date'] >= start_time - timedelta(days=180)
    ).group_by(
        aggregated_browser_days.c['browser_id'],
        aggregated_browser_days.c['next_7_days_event'],
        aggregated_browser_days.c['next_event_time'],
    ).subquery()

    positives_data = session.query(
        aggregated_browser_days,
    ).join(
        conversion_events,
        and_(
            aggregated_browser_days.c['browser_id'] == conversion_events.c['browser_id'],
            aggregated_browser_days.c['date'] <= conversion_events.c['next_event_time'],
            aggregated_browser_days.c['date'] >=
            conversion_events.c['next_event_time'] - timedelta(days=moving_window_length))
    ).cte()

    return positives_data


def get_subqueries_for_non_gapped_time_series(
        filtered_data,
        start_time: datetime,
        end_time: datetime,
):
    generated_time_series = session.query(
        func.generate_series(
            start_time,
            end_time,
            timedelta(days=1)
        ).cast(DATE).label('date_gap_filler')
    ).subquery(name='date_gap_filler')

    browser_ids = session.query(
        filtered_data.c['browser_id'],
        func.array_agg(
            case(
                [
                    (filtered_data.c['user_ids'] == None,
                     ''),
                    (filtered_data.c['user_ids'] == literal([], ARRAY(TEXT)),
                     '')

                ],
                else_= filtered_data.c['user_ids'].cast(TEXT)
            )).label('user_ids')
    ).group_by(filtered_data.c['browser_id']).subquery(name='browser_ids')

    all_date_browser_combinations = session.query(
        select([browser_ids, generated_time_series]).alias('all_date_browser_combinations')).subquery()

    return all_date_browser_combinations


def get_unique_events_subquery(
        filtered_data
):
    unique_events = session.query(
            filtered_data.c['browser_id'].label('event_browser_id'),
            filtered_data.c['next_event_time'].label('next_event_time_filled'),
            filtered_data.c['next_7_days_event'].label('outcome_filled')
        ).filter(
            filtered_data.c['next_event_time'] != None
        ).group_by(
            filtered_data.c['browser_id'],
            filtered_data.c['next_event_time'],
            filtered_data.c['next_7_days_event']).subquery('unique_events')

    return unique_events


def get_device_information_subquery(
        filtered_data
):
    device_information = session.query(
        case(
            [
                (filtered_data.c['device_brand'] == None,
                 'Desktop')
            ],
            else_= filtered_data.c['device_brand']
        ).label('device_brand').label('device'),
        filtered_data.c['browser_family'].label('browser'),
        filtered_data.c['is_desktop'],
        filtered_data.c['is_mobile'],
        filtered_data.c['is_tablet'],
        filtered_data.c['os_family'].label('os'),
        filtered_data.c['is_mobile'],
        filtered_data.c['browser_id']
    ).order_by(
        filtered_data.c['browser_id'],
        filtered_data.c['date'].desc()
    ).distinct(
        filtered_data.c['browser_id'],
    ).subquery('device_information')

    return device_information


def create_rolling_agg_function(
    moving_window_length: int,
    half_window: bool,
    agg_function,
    column,
    partitioning_column,
    ordering_column
):
    window_look_back_adjustment = 1 if moving_window_length % 2 > 0 or half_window is False else 0
    half_window_adjustment = 2 if half_window else 1
    lower_bound = int(-1 * (moving_window_length / half_window_adjustment - window_look_back_adjustment))
    result_function = agg_function(column).over(
        partition_by=partitioning_column,
        order_by=ordering_column,
        rows=(lower_bound, 0)
    )

    return result_function


def get_unique_json_fields_query(filtered_data, column_name):
    # TODO: Once the column type is unified in DB, we can get rid of this branching
    all_keys_query = session.query(
        func.jsonb_object_keys(filtered_data.c[column_name]).label(column_name)
    ).subquery()
    column_keys = session.query(all_keys_query.c[column_name]).distinct(
        all_keys_query.c[column_name]).all()
    column_keys = [json_key[0] for json_key in column_keys]

    return column_keys


def unpack_json_fields(filtered_data):
    json_key_based_columns = {}
    json_column_keys = {}
    for json_column in JSON_COLUMNS:
        json_column_keys[json_column] = get_unique_json_fields_query(filtered_data, json_column)
        if json_column != 'hour_interval_pageviews':
            json_key_based_columns[json_column] = {
                f'{json_column}_{json_key}': filtered_data.c[json_column][json_key].cast(Text).cast(Float)
                for json_key in json_column_keys[json_column]
            }

        else:
            json_key_based_columns[json_column] = sum_hourly_intervals_into_4_hour_ranges(
                filtered_data,
                json_column_keys['hour_interval_pageviews']
            )

    unpacked_time_fields_query = session.query(
        filtered_data,
        *[value.label(key)
            for json_keys in json_key_based_columns.values()
            for key, value in json_keys.items()
          ]
    ).subquery()

    json_key_column_names = [
        key for json_keys in json_key_based_columns.values()
        for key in json_keys.keys()
    ]

    return unpacked_time_fields_query, json_key_column_names


def sum_hourly_intervals_into_4_hour_ranges(
        filtered_data,
        json_column_keys
):
    '''
    :param filtered_data:
    :param json_column_keys:
    :return:
    Let's bundle the hourly data into 4 hour intervals (such as 00:00 - 03:59, ...) to avoid having too many columns.
    The division works with the following hypothesis:
    * 0-4: Night Owls
    * 4-8: Morning commute
    * 8-12: Working morning / coffee
    * 12-16: Early afternoon, browsing during lunch
    * 16-20: Evening commute
    * 20-24: Before bed browsing

    We need to use coalesce to avoid having almost all NULL columns
    '''
    range_sums = {}
    for i in range(0, len(json_column_keys), 4):
        column = 'hour_interval_pageviews'
        hours_in_interval = json_column_keys[i:i + 4]
        current_sum_name = (
                            f"hours_{re.sub('-[0-9][0-9][0-9][0-9]$', '', hours_in_interval[0])}" +  
                            f"_{re.sub('^[0-9][0-9][0-9][0-9]-', '', hours_in_interval[3])}"
                           )

        range_sums[current_sum_name] = func.coalesce(
            filtered_data.c[column][hours_in_interval[0]].cast(Text).cast(Float),
            0.0
        )

        for hour in hours_in_interval[1:]:
            range_sums[current_sum_name] = (
                    range_sums[current_sum_name] +
                    func.coalesce(filtered_data.c[column][hour].cast(Text).cast(Float), 0.0))

    return range_sums


def join_all_partial_queries(
        filtered_data_with_unpacked_json_fields,
        all_date_browser_combinations,
        unique_events,
        device_information,
        json_key_column_names
):
    joined_queries = session.query(
        all_date_browser_combinations.c['browser_id'].label('browser_id'),
        all_date_browser_combinations.c['user_ids'].label('user_ids'),
        all_date_browser_combinations.c['date_gap_filler'].label('date'),
        func.extract('dow', all_date_browser_combinations.c['date_gap_filler']).cast(Text).label('day_of_week'),
        filtered_data_with_unpacked_json_fields.c['date'].label('date_w_gaps'),
        (filtered_data_with_unpacked_json_fields.c['pageviews'] > 0.0).label('is_active_on_date'),
        unique_events.c['outcome_filled'],
        filtered_data_with_unpacked_json_fields.c['next_7_days_event'].label('outcome_original'),
        filtered_data_with_unpacked_json_fields.c['pageviews'],
        filtered_data_with_unpacked_json_fields.c['timespent'],
        filtered_data_with_unpacked_json_fields.c['sessions_without_ref'],
        filtered_data_with_unpacked_json_fields.c['sessions'],
        # Add all columns created from json_fields
        *[filtered_data_with_unpacked_json_fields.c[json_key_column] for json_key_column in json_key_column_names],
        # Unpack all device information columns except ones already present in other queries
        *[device_information.c[column.name] for column in device_information.columns if column.name != 'browser_id'],
    ).outerjoin(
        filtered_data_with_unpacked_json_fields,
        and_(
            all_date_browser_combinations.c['browser_id'] == filtered_data_with_unpacked_json_fields.c['browser_id'],
            all_date_browser_combinations.c['date_gap_filler'] == filtered_data_with_unpacked_json_fields.c['date'])
    ).outerjoin(
        unique_events,
        and_(
            unique_events.c['event_browser_id'] == filtered_data_with_unpacked_json_fields.c['browser_id'],
            unique_events.c['next_event_time_filled'] > filtered_data_with_unpacked_json_fields.c['date'] - timedelta(days=7),
            filtered_data_with_unpacked_json_fields.c['date'] <= unique_events.c['next_event_time_filled']
        )
    ).outerjoin(
        device_information,
        device_information.c['browser_id'] == all_date_browser_combinations.c['browser_id']
    ).subquery()

    return joined_queries


def calculate_rolling_windows_features(
        joined_queries,
        moving_window_length: int,
        start_time: datetime,
        json_key_column_names: List[str]
):
    rolling_agg_columns_base = create_rolling_window_columns_config(
        joined_queries,
        json_key_column_names,
        moving_window_length
    )

    queries_with_basic_window_columns = session.query(
        joined_queries,
        *rolling_agg_columns_base,
        extract('day',
                # last day in the current window
                joined_queries.c['date']
                # last day active in current window
                - func.coalesce(
                    create_rolling_agg_function(
                        moving_window_length,
                        False,
                        func.max,
                        joined_queries.c['date_w_gaps'],
                        joined_queries.c['browser_id'],
                        joined_queries.c['date']),
                    start_time - timedelta(days=2)
                )
                ).label('days_since_last_active'),
        # row number in case deduplication is needed
        func.row_number().over(
            partition_by=[
                joined_queries.c['browser_id'],
                joined_queries.c['date']],
            order_by=[
                joined_queries.c['browser_id'],
                joined_queries.c['date']]
        ).label('row_number')
    ).subquery()

    return queries_with_basic_window_columns


def create_time_window_vs_day_of_week_combinations(
        joined_queries,
        time_key_column_names
):
    # Day of Week with 4-hour intervals
    combinations = {
        f'dow_{i}_{time_key_column}': case(
            [
                (joined_queries.c['day_of_week'] == None,
                 0),
                (joined_queries.c['day_of_week'] != str(i),
                 0)
            ],
            else_=joined_queries.c[time_key_column]
        )
        for i in range(0, 7)
        for time_key_column in time_key_column_names
    }
    # Day of Week only
    combinations.update(
        {
            f'dow_{i}': case(
                [
                    (joined_queries.c['day_of_week'] == None,
                     0),
                    (joined_queries.c['day_of_week'] != str(i),
                     0)
                ],
                else_=1
            )
            for i in range(0, 7)
        }
    )
    # 4-hour intervals
    combinations.update(
        {time_key_column_name: joined_queries.c[time_key_column_name]
         for time_key_column_name in time_key_column_names}
    )

    return combinations


def create_rolling_window_columns_config(
        joined_queries,
        json_key_column_names,
        moving_window_length
):
    # {name of the resulting column : source / calculation}
    column_source_to_name_mapping = {
        'pageview': joined_queries.c['pageviews'],
        'timespent': joined_queries.c['timespent'],
        'direct_visit': joined_queries.c['sessions_without_ref'],
        'visit': joined_queries.c['sessions'],
        'days_active': case(
            [
                (
                    joined_queries.c['date_w_gaps'] == None,
                    0
                )
            ],
            else_=1),
        # All json key columns have their own rolling sums
        **{
            column: joined_queries.c[column] for column in json_key_column_names
            if 'hours_' not in column
        }
    }

    time_column_config = create_time_window_vs_day_of_week_combinations(
        joined_queries,
        [column for column in json_key_column_names if 'hours_' in column]
    )

    column_source_to_name_mapping.update(time_column_config)
    # {naming suffix : related parameter for determining part of full window}
    rolling_agg_variants = {
        'count': False,
        'count_last_window_half': True
    }
    # this generates all basic rolling sum columns for both full and second half of the window
    rolling_agg_columns = [
        create_rolling_agg_function(
            moving_window_length,
            is_half_window,
            func.sum,
            column_source,
            joined_queries.c['browser_id'],
            joined_queries.c['date']
        ).cast(Float).label(f'{column_name}_{suffix}')
        for column_name, column_source in column_source_to_name_mapping.items()
        for suffix, is_half_window in rolling_agg_variants.items()
    ]

    return rolling_agg_columns


def filter_joined_queries_adding_derived_metrics(
    joined_partial_queries,
    start_time: datetime,
    retrieving_past_positives
):

    finalizing_filter = [
        joined_partial_queries.c['pageview_count'] > 0,
        joined_partial_queries.c['row_number'] == 1
    ]

    if not retrieving_past_positives:
        finalizing_filter.append(joined_partial_queries.c['date'] >= start_time)

    filtered_w_derived_metrics = session.query(
        *[column.label(re.sub('timespent_count', 'timespent_sum', column.name))
          for column in joined_partial_queries.columns
          if not re.search('outcome', column.name)
          and column.name != 'row_number'],
        case(
            [
                (joined_partial_queries.c['is_active_on_date'] == True,
                 joined_partial_queries.c['outcome_original']),
                (joined_partial_queries.c['is_active_on_date'] == False,
                 joined_partial_queries.c['outcome_filled']),
            ],
            else_='no_conversion'
        ).label('outcome'),
        *[
            func.coalesce((
                joined_partial_queries.c[DERIVED_METRICS_CONFIG[key]['nominator'] + suffix] /
                joined_partial_queries.c[DERIVED_METRICS_CONFIG[key]['denominator'] + suffix]
            ), 0.0).label(key + suffix)
            for key in DERIVED_METRICS_CONFIG.keys()
            for suffix in ['', '_last_window_half']
        ]
    ).filter(and_(*finalizing_filter)).subquery()

    return filtered_w_derived_metrics


def add_all_time_delta_columns(
        filtered_w_derived_metrics
):
    all_time_delta_columns = session.query(
        filtered_w_derived_metrics,
        *[
            (
                filtered_w_derived_metrics.c[re.sub('_last_window_half', '', column.name)] - filtered_w_derived_metrics.c[column.name]
            ).label(re.sub('_last_window_half', '_first_window_half', column.name))
            for column in filtered_w_derived_metrics.columns
            if re.search('_last_window_half', column.name)
        ],
        *[
            case(
                    [
                        # If the 2nd half of period has 0 for its value, we assign 100 % decline
                        (filtered_w_derived_metrics.c[column.name] == 0, -1),
                        # If the 1st half of period has 0 for its value, we assign 100 % growth
                        (
                            filtered_w_derived_metrics.c[re.sub('_last_window_half', '', column.name)]
                            - filtered_w_derived_metrics.c[column.name] == 0, 1)
                    ],
                else_=(filtered_w_derived_metrics.c[column.name] / (
                    filtered_w_derived_metrics.c[re.sub('_last_window_half', '', column.name)] -
                    filtered_w_derived_metrics.c[column.name]))
            ).label(f'relative_{re.sub("_last_window_half", "", column.name)}_change_first_and_second_half')
            for column in filtered_w_derived_metrics.columns
            if re.search('_last_window_half', column.name)
         ]
    )

    return all_time_delta_columns


queries = dict()
queries['upsert_predictions'] = '''
        INSERT INTO
               conversion_predictions_daily (
                   date,
                   browser_id,
                   user_ids,
                   predicted_outcome, 
                   conversion_probability, 
                   no_conversion_probability,
                   shared_account_login_probability,
                   model_version,
                   created_at,
                   updated_at)
           VALUES (
               :date,
               :browser_id,
               :user_ids,
               :predicted_outcome, 
               :conversion_probability, 
               :no_conversion_probability,
               :shared_account_login_probability,
               :model_version,
               :created_at,
               :updated_at)
           ON CONFLICT
               (browser_id, date)
           DO UPDATE SET
               conversion_probability = :conversion_probability,
               no_conversion_probability = :no_conversion_probability,
               shared_account_login_probability = :shared_account_login_probability,
               model_version = :model_version,
               updated_at = :updated_at
    '''

queries['upsert_prediction_job_log'] = '''
        INSERT INTO
               prediction_job_log (
                   date,
                   rows_predicted,
                   model_version,
                   created_at,
                   updated_at)
           VALUES (
               :date,
               :rows_predicted,
               :model_version,
               :created_at,
               :updated_at)
           ON CONFLICT
               (date, model_version)
           DO UPDATE SET
               rows_predicted = :rows_predicted,
               updated_at = :updated_at
    '''
