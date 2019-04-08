import re
import pandas as pd
import sqlalchemy
from sqlalchemy.types import TIMESTAMP, Float, DATE, Text
from sqlalchemy.sql.expression import literal, extract
from sqlalchemy import and_
from sqlalchemy import func, case
from sqlalchemy.sql.expression import cast
from datetime import timedelta, datetime
from .db_utils import get_aggregated_browser_days_w_session
from .config import DERIVED_METRICS_CONFIG

aggregated_browser_days, session = get_aggregated_browser_days_w_session()


def get_feature_frame_via_sqlalchemy(
    start_time: datetime,
    end_time: datetime,
    moving_window_length: int=7
):
    full_query = get_full_features_query(
        start_time,
        end_time,
        moving_window_length
    )

    feature_frame = pd.read_sql(full_query.statement, full_query.session.bind)
    feature_frame['is_active_on_date'] = feature_frame['is_active_on_date'].astype(bool)
    feature_frame['date'] = pd.to_datetime(feature_frame['date']).dt.date

    return feature_frame


def get_full_features_query(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int=7
):
    filtered_data = get_filtered_cte(start_time, end_time)

    browser_ids, generated_time_series = get_subqueries_for_non_gapped_time_series(
        filtered_data,
        start_time,
        end_time
    )

    unique_events = get_unique_events_subquery(filtered_data)

    device_information = get_device_information_subquery(filtered_data)

    joined_partial_queries = join_all_partial_queries(
        filtered_data,
        browser_ids,
        generated_time_series,
        unique_events,
        device_information,
        moving_window_length,
        start_time
    )

    filtered_w_derived_metrics = filter_joined_queries_adding_derived_metrics(
        joined_partial_queries,
        start_time
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
    ).subquery()

    browser_ids = session.query(
        filtered_data.c['browser_id'],
        func.array_agg(
                filtered_data.c['user_ids']
            ).label('user_ids')
    ).group_by(filtered_data.c['browser_id']).subquery()

    return browser_ids, generated_time_series


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


def join_all_partial_queries(
        filtered_data,
        browser_ids,
        generated_time_series,
        unique_events,
        device_information,
        moving_window_length: int,
        start_time: datetime
):
    # {name of the resulting column : source / calculation}
    column_source_to_name_mapping = {
        'pageview': filtered_data.c['pageviews'],
        'timespent': filtered_data.c['timespent'],
        'direct_visit': filtered_data.c['sessions_without_ref'],
        'visit': filtered_data.c['sessions'],
        'days_active': case(
            [
                (
                    filtered_data.c['date'] == None,
                    0
                )
            ],
            else_= 1)
    }
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
            browser_ids.c['browser_id'],
            generated_time_series.c['date_gap_filler']
        ).cast(Float).label(f'{column_name}_{suffix}')
        for column_name, column_source  in column_source_to_name_mapping.items()
        for suffix, is_half_window in rolling_agg_variants.items()
    ]

    joined_partial_queries = session.query(
        browser_ids.c['browser_id'].label('browser_id'),
        browser_ids.c['user_ids'].label('user_ids'),
        generated_time_series.c['date_gap_filler'].label('date'),
        *rolling_agg_columns,
        (filtered_data.c['pageviews'] > 0.0).label('is_active_on_date'),
        extract('day',
                # last day in the current window
                generated_time_series.c['date_gap_filler']
                # last day active in current window
                - func.coalesce(
                    create_rolling_agg_function(
                        moving_window_length,
                        False,
                        func.max,
                        filtered_data.c['date'],
                        browser_ids.c['browser_id'],
                        generated_time_series.c['date_gap_filler']),
                    start_time - timedelta(days=2)
                )
               ).label('days_since_last_active'),
        unique_events.c['outcome_filled'],
        filtered_data.c['next_7_days_event'].label('outcome_original'),
        # unpack all device information columns except ones already present in other queries
        *[device_information.c[column.name] for column in device_information.columns if column.name != 'browser_id'],
        # row number in case deduplication is needed
        func.row_number().over(
            partition_by=[
                browser_ids.c['browser_id'],
                generated_time_series.c['date_gap_filler']],
            order_by=[
                browser_ids.c['browser_id'],
                generated_time_series.c['date_gap_filler']]
        ).label('row_number')
    ).outerjoin(
        generated_time_series, literal(True)
    ).outerjoin(
        filtered_data,
        and_(
            browser_ids.c['browser_id'] == filtered_data.c['browser_id'],
            generated_time_series.c['date_gap_filler'] == filtered_data.c['date'])
     ).outerjoin(
        unique_events,
        and_(
            unique_events.c['event_browser_id'] == filtered_data.c['browser_id'],
            unique_events.c['next_event_time_filled'] > filtered_data.c['date'] - timedelta(days=7),
            filtered_data.c['date'] <= unique_events.c['next_event_time_filled']
        ),
    ).outerjoin(
        device_information,
        device_information.c['browser_id'] == browser_ids.c['browser_id']
    ).subquery()

    return joined_partial_queries


def filter_joined_queries_adding_derived_metrics(
    joined_partial_queries,
    start_time: datetime
):
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
    ).filter(
        and_(
            joined_partial_queries.c['pageview_count'] > 0,
            joined_partial_queries.c['date'] >= start_time,
            joined_partial_queries.c['row_number'] == 1)
    ).subquery()

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
