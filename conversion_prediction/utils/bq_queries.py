import re
import pandas as pd
from sqlalchemy import select, column
from sqlalchemy.types import Float, DATE, String
from sqlalchemy import and_, func, case, text
from sqlalchemy.sql.expression import cast
from datetime import timedelta, datetime
from .db_utils import get_sqlalchemy_tables_w_session
from .config import build_derived_metrics_config, JSON_COLUMNS, LABELS, generate_4_hour_interval_column_names
from typing import List, Dict

bq_mappings = get_sqlalchemy_tables_w_session(
    'BQ_CONNECTION_STRING',
    schema='pythia',
    table_names=['aggregated_browser_days'],
    engine_kwargs={'credentials_path': '../../client_secrets.json'}
)

bq_session = bq_mappings['session']
aggregated_browser_days = bq_mappings['aggregated_browser_days']


def get_feature_frame_via_sqlalchemy(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int = 7,
        feature_aggregation_functions: Dict[str, func] = None
):
    if feature_aggregation_functions is None:
        feature_aggregation_functions = {'avg': func.avg}

    full_query = bq_session.query(get_full_features_query(
        start_time,
        end_time,
        moving_window_length,
        feature_aggregation_functions,
    ))

    feature_frame = pd.read_sql(full_query.statement, full_query.session.bind)
    feature_frame.columns = [re.sub('anon_1_', '', column) for column in feature_frame.columns]
    feature_frame['is_active_on_date'] = feature_frame['is_active_on_date'].astype(bool)
    feature_frame['date'] = pd.to_datetime(feature_frame['date']).dt.tz_localize(None).dt.date
    feature_frame.drop('date_w_gaps', axis=1, inplace=True)

    return feature_frame


def get_full_features_query(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int = 7,
        feature_aggregation_functions: Dict[str, func] = None
):
    if feature_aggregation_functions is None:
        feature_aggregation_functions = {'count': func.sum}

    filtered_data = filter_by_date(
        # We retrieve an additional window length lookback of records to correctly construct the rolling window
        # for the records on the 1st day of the time period we intend to use
        start_time - timedelta(days=moving_window_length),
        end_time
    )

    all_date_browser_combinations = get_subqueries_for_non_gapped_time_series(
        filtered_data
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
        json_key_column_names,
        feature_aggregation_functions
    )

    filtered_w_derived_metrics = filter_joined_queries_adding_derived_metrics(
        data_with_rolling_windows,
        feature_aggregation_functions
    )

    filtered_w_derived_metrics_w_all_time_delta_columns = add_all_time_delta_columns(
        filtered_w_derived_metrics
    )

    final_query_for_outcome_category = remove_helper_lookback_rows(
        filtered_w_derived_metrics_w_all_time_delta_columns,
        start_time
    )

    return final_query_for_outcome_category


def filter_by_date(
        start_time: datetime,
        end_time: datetime
):
    filtered_data = bq_session.query(
        aggregated_browser_days.c['date'].label('date'),
        aggregated_browser_days.c['browser_id'].label('browser_id'),
        aggregated_browser_days.c['user_ids'].label('user_ids'),
        aggregated_browser_days.c['pageviews'].label('pageviews'),
        aggregated_browser_days.c['timespent'].label('timespent'),
        aggregated_browser_days.c['sessions'].label('sessions'),
        aggregated_browser_days.c['sessions_without_ref'].label('sessions_without_ref'),
        aggregated_browser_days.c['browser_family'].label('browser_family'),
        aggregated_browser_days.c['browser_version'].label('browser_version'),
        aggregated_browser_days.c['os_family'].label('os_family'),
        aggregated_browser_days.c['os_version'].label('os_version'),
        aggregated_browser_days.c['device_family'].label('device_family'),
        aggregated_browser_days.c['device_brand'].label('device_brand'),
        aggregated_browser_days.c['device_model'].label('device_model'),
        aggregated_browser_days.c['is_desktop'].label('is_desktop'),
        aggregated_browser_days.c['is_mobile'].label('is_mobile'),
        aggregated_browser_days.c['is_tablet'].label('is_tablet'),
        aggregated_browser_days.c['next_7_days_event'].label('next_7_days_event'),
        aggregated_browser_days.c['next_event_time'].label('next_event_time'),
        aggregated_browser_days.c['referer_medium_pageviews'].label('referer_medium_pageviews'),
        aggregated_browser_days.c['article_category_pageviews'].label('article_category_pageviews'),
        aggregated_browser_days.c['hour_interval_pageviews'].label('hour_interval_pageviews'),
        aggregated_browser_days.c['pageviews_0h_4h'].label('pvs_0h_4h'),
        aggregated_browser_days.c['pageviews_4h_8h'].label('pvs_4h_8h'),
        aggregated_browser_days.c['pageviews_8h_12h'].label('pvs_8h_12h'),
        aggregated_browser_days.c['pageviews_12h_16h'].label('pvs_12h_16h'),
        aggregated_browser_days.c['pageviews_16h_20h'].label('pvs_16h_20h'),
        aggregated_browser_days.c['pageviews_20h_24h'].label('pvs_20h_24h')
    ).subquery()
    
    # This transforms the 7 day event into 1 day event
    filtered_data_w_1_day_event_window = bq_session.query(
        *[filtered_data.c[column.name].label(column.name) for column in filtered_data.columns
          if column.name != 'next_7_days_event'],
        case(
            [
                (and_(
                    filtered_data.c['next_7_days_event'].in_(
                        [label for label, label_type in LABELS.items() if label_type == 'positive']
                    ),
                    func.date_diff(
                        filtered_data.c['next_event_time'].cast(DATE),
                        filtered_data.c['date'],
                        text('day')
                    ) >= 1
                ),
                 filtered_data.c['next_7_days_event'])
            ],
            else_=[label for label, label_type in LABELS.items() if label_type == 'negative'][0]
        ).label('next_7_days_event')
    ).subquery()

    current_data = bq_session.query(
        *[filtered_data_w_1_day_event_window.c[column.name].label(column.name) for column in filtered_data_w_1_day_event_window.columns]
    ).filter(
        filtered_data_w_1_day_event_window.c['date'] >= cast(start_time, DATE),
        filtered_data_w_1_day_event_window.c['date'] <= cast(end_time, DATE)
    )

    past_positives = bq_session.query(
        *[filtered_data_w_1_day_event_window.c[column.name].label(column.name) for column in filtered_data_w_1_day_event_window.columns]
    ).filter(
        filtered_data_w_1_day_event_window.c['date'] >= cast(start_time - timedelta(days=90), DATE),
        filtered_data_w_1_day_event_window.c['date'] <= cast(start_time, DATE)
    )

    filtered_data = current_data.union_all(past_positives).subquery()

    return filtered_data


def remove_helper_lookback_rows(
        filtered_w_derived_metrics_w_all_time_delta_columns,
        start_time
):
    label_lookback_cause = filtered_w_derived_metrics_w_all_time_delta_columns.c['date'] >= (
            (start_time - timedelta(days=90)).date()
            )

    final_query_for_outcome_category = bq_session.query(
        # We re-alias since adding another layer causes sqlalchemy to abbreviate columns
        *[filtered_w_derived_metrics_w_all_time_delta_columns.c[column.name].label(column.name) for column in
          filtered_w_derived_metrics_w_all_time_delta_columns.columns]
    ).filter(
        label_lookback_cause
    ).subquery()

    return final_query_for_outcome_category


def get_subqueries_for_non_gapped_time_series(
        filtered_data
):
    start_time, end_time = bq_session.query(
        func.min(filtered_data.c['date']),
        func.max(filtered_data.c['date']),
    ).all()[0]

    # generated_time_series = bq_session.query(
    #     func.generate_date_array(start_time, end_time).labe('date_gap_filler')
    # ).subquery()
    #
    # generated_time_series = bq_session.query(
    #     func.unnest(generated_time_series.c['date_gap_filler']).cast(DATE).label('date_gap_filler')
    # ).subquery()

    # generated_time_series = bq_session.query(
    #     func.unnest(
    #         func.generate_date_array(start_time, end_time)
    #     ).cast(DATE).label('date_gap_filler')
    # ).subquery()

    generated_time_series = bq_session.query(select([column('dates').label('date_gap_filler')]).select_from(
        func.unnest(
            func.generate_date_array(start_time, end_time)
            ).alias('dates')
    )).subquery()

    browser_ids = bq_session.query(
        filtered_data.c['browser_id'],
        func.array_agg(
            case(
                [
                    (filtered_data.c['user_ids'] == None,
                     ''),
                    # This is an obvious hack, but while PG accepts a comparison with an empty array that is defined
                    # as literal([], ARRAY[TEXT]), bigquery claims it doesn't know the type of the array
                    (func.array_to_string(filtered_data.c['user_ids'], '') == '',
                     '')
                ],
                else_=func.array_to_string(filtered_data.c['user_ids'], ',')
            )).label('user_ids')
    ).group_by(filtered_data.c['browser_id']).subquery(name='browser_ids')

    all_date_browser_combinations = bq_session.query(
        select([browser_ids, generated_time_series.c['date_gap_filler']]).alias('all_date_browser_combinations')).subquery()

    return all_date_browser_combinations


def get_unique_events_subquery(
        filtered_data
):
    unique_events = bq_session.query(
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
    device_information = bq_session.query(
        case(
            [
                (filtered_data.c['device_brand'] == None,
                 'Desktop')
            ],
            else_=filtered_data.c['device_brand']
        ).label('device_brand').label('device'),
        filtered_data.c['browser_family'].label('browser'),
        filtered_data.c['is_desktop'],
        filtered_data.c['is_mobile'],
        filtered_data.c['is_tablet'],
        filtered_data.c['os_family'].label('os'),
        filtered_data.c['is_mobile'],
        filtered_data.c['browser_id'],
        filtered_data.c['date']
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
    # This is a hacky workaround. Bigquery doesn't have a function to extract all keys from a json column flattening
    # them. What we do here is remove everything but the keys from the json string, split on commas, keep only the ones
    # with 1 key (assuming with enough data all keys occur along, especially since more visitors only tend to have
    # 1 pageview, we than transform these single key arrays to string and group by to get unique ones
    all_keys_query = bq_session.query(
        func.regexp_replace(filtered_data.c[column_name], '{|}|: [0-9]+|"', '').label(column_name)
    ).subquery()

    all_keys_query = bq_session.query(
        func.split(all_keys_query.c[column_name], ',').label(column_name)
    ).subquery()

    all_keys_query = bq_session.query(all_keys_query).filter(
        func.array_length(all_keys_query.c[column_name]) == 1
    ).subquery()

    column_keys = bq_session.query(
        func.array_to_string(all_keys_query.c[column_name], '').label(column_name)
    ).group_by(
        column_name
    ).all()
    
    column_keys = [json_key[0] for json_key in column_keys]

    return column_keys


def unpack_json_fields(filtered_data):
    json_key_based_columns = {}
    json_column_keys = {}
    for json_column in JSON_COLUMNS:
        if json_column != 'hour_interval_pageviews':
            json_column_keys[json_column] = get_unique_json_fields_query(filtered_data, json_column)
            json_key_based_columns[json_column] = {
                f'{json_column}_{re.sub("-", "_" , json_key)}':
                    func.json_extract(filtered_data.c[json_column], f'$.{json_key}').cast(Float)
                for json_key in json_column_keys[json_column]
            }

        else:
            # TODO: This is now technically not a json column anymore
            json_key_based_columns[json_column] = add_4_hour_intervals(
                filtered_data
            )

    json_key_column_names = [
        key for json_keys in json_key_based_columns.values()
        for key in json_keys.keys()
    ]

    unpacked_time_fields_query = bq_session.query(
        *[filtered_data.c[column.name].label(column.name) for column in filtered_data.columns if
          column.name not in json_key_column_names],
        *[column for column in json_key_based_columns['hour_interval_pageviews'].values()],
        *[value.label(key)
          for original_column, json_keys in json_key_based_columns.items()
          for key, value in json_keys.items()
          if original_column != 'hour_interval_pageviews'
          ]
    ).subquery()

    return unpacked_time_fields_query, json_key_column_names


def add_4_hour_intervals(
        filtered_data
):
    '''
    :param filtered_data:
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
    hour_ranges = generate_4_hour_interval_column_names()
    hour_based_columns = {column: filtered_data.c[column].label(column) for column in hour_ranges}

    return hour_based_columns


def join_all_partial_queries(
        filtered_data_with_unpacked_json_fields,
        all_date_browser_combinations,
        unique_events,
        device_information,
        json_key_column_names
):
    joined_queries = bq_session.query(
        all_date_browser_combinations.c['browser_id'].label('browser_id'),
        all_date_browser_combinations.c['user_ids'].label('user_ids'),
        all_date_browser_combinations.c['date_gap_filler'].label('date'),
        func.extract(text('DAYOFWEEK'), all_date_browser_combinations.c['date_gap_filler']).cast(String).label('day_of_week'),
        filtered_data_with_unpacked_json_fields.c['date'].label('date_w_gaps'),
        (filtered_data_with_unpacked_json_fields.c['pageviews'] > 0.0).label('is_active_on_date'),
        unique_events.c['outcome_filled'],
        filtered_data_with_unpacked_json_fields.c['next_7_days_event'].label('outcome_original'),
        filtered_data_with_unpacked_json_fields.c['pageviews'],
        filtered_data_with_unpacked_json_fields.c['timespent'],
        filtered_data_with_unpacked_json_fields.c['sessions_without_ref'],
        filtered_data_with_unpacked_json_fields.c['sessions'],
        # Add all columns created from json_fields
        *[filtered_data_with_unpacked_json_fields.c[json_key_column].label(json_key_column) for json_key_column in json_key_column_names],
        # Unpack all device information columns except ones already present in other queries
        *[device_information.c[column.name] for column in device_information.columns if column.name not in ['browser_id', 'date']],
    ).outerjoin(
        filtered_data_with_unpacked_json_fields,
        and_(
            all_date_browser_combinations.c['browser_id'] == filtered_data_with_unpacked_json_fields.c['browser_id'],
            all_date_browser_combinations.c['date_gap_filler'] == filtered_data_with_unpacked_json_fields.c['date'])
    ).outerjoin(
        unique_events,
        and_(
            unique_events.c['event_browser_id'] == filtered_data_with_unpacked_json_fields.c['browser_id'],
            unique_events.c['next_event_time_filled'].cast(DATE) > func.date_sub(
                filtered_data_with_unpacked_json_fields.c['date'],
                text(f'interval {1} day')
            ),
            filtered_data_with_unpacked_json_fields.c['date'] <= unique_events.c['next_event_time_filled'].cast(DATE)
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
        json_key_column_names: List[str],
        feature_aggregation_functions
):
    rolling_agg_columns_base = create_rolling_window_columns_config(
        joined_queries,
        json_key_column_names,
        moving_window_length,
        feature_aggregation_functions
    )

    queries_with_basic_window_columns = bq_session.query(
        joined_queries,
        *rolling_agg_columns_base,
        func.date_diff(
            # last day in the current window
            joined_queries.c['date'],
            # last day active in current window
            func.coalesce(
                create_rolling_agg_function(
                    moving_window_length,
                    False,
                    func.max,
                    joined_queries.c['date_w_gaps'],
                    joined_queries.c['browser_id'],
                    joined_queries.c['date']),
                (start_time - timedelta(days=2)).date()
            ),
            text('day')).label('days_since_last_active'),
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
        joined_queries
):
    interval_names = generate_4_hour_interval_column_names()
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
        for time_key_column in interval_names
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
         for time_key_column_name in interval_names}
    )

    return combinations


def create_rolling_window_columns_config(
        joined_queries,
        json_key_column_names,
        moving_window_length,
        feature_aggregation_functions
):
    # {name of the resulting column : source / calculation},
    column_source_to_name_mapping = {
        'pageview': joined_queries.c['pageviews'],
        'timespent': joined_queries.c['timespent'],
        'direct_visit': joined_queries.c['sessions_without_ref'],
        'visit': joined_queries.c['sessions'],
        # All json key columns have their own rolling sums
        **{
            column: joined_queries.c[column] for column in json_key_column_names
        }
    }

    time_column_config = create_time_window_vs_day_of_week_combinations(
        joined_queries,
        [column for column in json_key_column_names if 'hours_' in column]
    )

    column_source_to_name_mapping.update(time_column_config)

    def get_rolling_agg_window_variants(aggregation_function_alias):
        # {naming suffix : related parameter for determining part of full window}
        return {
            f'{aggregation_function_alias}': False,
            f'{aggregation_function_alias}_last_window_half': True
        }

    rolling_agg_columns = []
    # this generates all basic rolling sum columns for both full and second half of the window
    rolling_agg_columns = rolling_agg_columns + [
        create_rolling_agg_function(
            moving_window_length,
            is_half_window,
            aggregation_function,
            column_source,
            joined_queries.c['browser_id'],
            joined_queries.c['date']
        ).cast(Float).label(f'{column_name}_{suffix}' if column_name != 'days_active' else 'days_active_count')
        for column_name, column_source in column_source_to_name_mapping.items()
        for aggregation_function_alias, aggregation_function in feature_aggregation_functions.items()
        for suffix, is_half_window in get_rolling_agg_window_variants(aggregation_function_alias).items()
    ]

    # It only makes sense to aggregate active days by summing, all other aggregations would end up with a value
    # of 1 after we eventually filter out windows with no active days in them, this is why we separate this feature
    # out from the loop with all remaining features
    rolling_agg_columns = rolling_agg_columns + [
        create_rolling_agg_function(
            moving_window_length,
            is_half_window,
            func.sum,
            case(
                [
                    (
                        joined_queries.c['date_w_gaps'] == None,
                        0
                    )
                ],
                else_=1),
            joined_queries.c['browser_id'],
            joined_queries.c['date']
        ).cast(Float).label(f'days_active_{suffix}')
        for suffix, is_half_window in get_rolling_agg_window_variants('count').items()
    ]

    return rolling_agg_columns


def filter_joined_queries_adding_derived_metrics(
        joined_partial_queries,
        feature_aggregation_functions
):
    # We will only check the first requested aggregation as we want to make sure there was at least 1 PV during the
    # time window, which works for any aggregation type result for number of pageviews being higher than 0
    first_aggregation_function_alias = next(iter(feature_aggregation_functions.keys()))

    finalizing_filter = [
        joined_partial_queries.c[f'pageview_{first_aggregation_function_alias}'] > 0,
        joined_partial_queries.c['row_number'] == 1
    ]

    derived_metrics_config = {}
    for feature_aggregation_function_alias in feature_aggregation_functions.keys():
        derived_metrics_config.update(build_derived_metrics_config(feature_aggregation_function_alias))

    filtered_w_derived_metrics = bq_session.query(
        *[column.label(column.name)
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
                    joined_partial_queries.c[derived_metrics_config[key]['nominator'] + suffix] /
                    joined_partial_queries.c[derived_metrics_config[key]['denominator'] + suffix]
            ), 0.0).label(key + suffix)
            for key in derived_metrics_config.keys()
            for suffix in ['', '_last_window_half']
        ]
    ).filter(and_(*finalizing_filter)).subquery()

    return filtered_w_derived_metrics


def add_all_time_delta_columns(
        filtered_w_derived_metrics
):
    filtered_w_derived_metrics_w_all_time_delta_columns = bq_session.query(
        filtered_w_derived_metrics,
        *[
            (
                    filtered_w_derived_metrics.c[re.sub('_last_window_half', '', column.name)] -
                    filtered_w_derived_metrics.c[column.name]
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
    ).subquery()

    return filtered_w_derived_metrics_w_all_time_delta_columns


def get_payment_history_features(end_time: datetime):
    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CONNECTION_STRING',
        'predplatne',
        ['payments', 'subscriptions']
    )

    mysql_predplatne_session = predplatne_mysql_mappings['session']
    payments = predplatne_mysql_mappings['payments']
    subscriptions = predplatne_mysql_mappings['subscriptions']

    clv = mysql_predplatne_session.query(
        func.sum(payments.c['amount']).label('clv'),
        payments.c['user_id']
    ).filter(
        and_(
            payments.c['created_at'] <= end_time,
            payments.c['status'] == 'paid'
        )
    ).group_by(
        payments.c['user_id']
    ).subquery()

    days_since_last_subscription = mysql_predplatne_session.query(
        func.datediff(end_time, func.max(subscriptions.c['end_time'])).label('days_since_last_subscription'),
        func.max(subscriptions.c['end_time']).label('last_subscription_end'),
        subscriptions.c['user_id']
    ).filter(
        subscriptions.c['end_time'] <= end_time
    ).group_by(
        subscriptions.c['user_id']
    ).subquery()

    user_payment_history_query = mysql_predplatne_session.query(
        clv.c['clv'],
        clv.c['user_id'],
        days_since_last_subscription.c['days_since_last_subscription'],
        days_since_last_subscription.c['last_subscription_end']
    ).outerjoin(
        days_since_last_subscription,
        clv.c['user_id'] == days_since_last_subscription.c['user_id']
    )

    user_payment_history = pd.read_sql(
        user_payment_history_query.statement,
        user_payment_history_query.session.bind
    )

    user_payment_history['clv'] = user_payment_history['clv'].astype(float)
    user_payment_history['days_since_last_subscription'] = user_payment_history[
        'days_since_last_subscription'
    ].astype(float)

    mysql_predplatne_session.close()

    return user_payment_history


def get_global_context(start_time, end_time):
    beam_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CONNECTION_STRING',
        'remp_beam',
        ['article_pageviews']
    )
    mysql_beam_session = beam_mysql_mappings['session']
    article_pageviews = beam_mysql_mappings['article_pageviews']

    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CONNECTION_STRING',
        'predplatne',
        ['payments']
    )

    mysql_predplatne_session = predplatne_mysql_mappings['session']
    payments = predplatne_mysql_mappings['payments']

    # We create two subqueries using the same data to merge twice in order to get rolling sum in mysql
    def get_payments_filtered():
        payments_filtered = mysql_predplatne_session.query(
            payments.c['created_at'].cast(DATE).label('date'),
            func.count(payments.c['id']).label('payment_count'),
            func.sum(payments.c['amount']).label('sum_paid')
        ).filter(
            payments.c['created_at'] >= start_time,
            payments.c['created_at'] <= end_time,
            payments.c['status'] == 'paid'
        ).group_by(
            'date'
        ).subquery()

        return payments_filtered

    def get_article_pageviews_filtered():
        article_pageviews_filtered = mysql_beam_session.query(
            article_pageviews.c['time_from'].cast(DATE).label('date'),
            func.sum(article_pageviews.c['sum']).label('article_pageviews'),
        ).filter(
            article_pageviews.c['time_from'] >= start_time,
            article_pageviews.c['time_from'] <= end_time
        ).group_by(
            'date'
        ).subquery()

        return article_pageviews_filtered

    payments_filtered_1 = get_payments_filtered()
    payments_filtered_2 = get_payments_filtered()

    payments_context = mysql_predplatne_session.query(
        payments_filtered_1.c['date'].label('date'),
        func.sum(payments_filtered_2.c['payment_count']).label('payment_count'),
        func.sum(payments_filtered_2.c['sum_paid']).label('sum_paid')
    ).join(
        payments_filtered_2,
        func.datediff(payments_filtered_1.c['date'], payments_filtered_2.c['date']).between(0, 7)
    ).group_by(
        payments_filtered_1.c['date']
    ).order_by(
        payments_filtered_1.c['date']
    ).subquery()

    article_pageviews_filtered_1 = get_article_pageviews_filtered()
    article_pageviews_filtered_2 = get_article_pageviews_filtered()

    article_pageviews_context = mysql_beam_session.query(
        article_pageviews_filtered_1.c['date'].label('date'),
        func.sum(article_pageviews_filtered_2.c['article_pageviews']).label('article_pageviews_count'),
    ).join(
        article_pageviews_filtered_2,
        func.datediff(article_pageviews_filtered_1.c['date'], article_pageviews_filtered_2.c['date']).between(0, 7)
    ).group_by(
        article_pageviews_filtered_1.c['date']
    ).order_by(
        article_pageviews_filtered_1.c['date']
    ).subquery()

    context_query = mysql_predplatne_session.query(
        payments_context.c['date'],
        payments_context.c['payment_count'],
        payments_context.c['sum_paid'],
        article_pageviews_context.c['article_pageviews_count']
    ).join(
        article_pageviews_context,
        article_pageviews_context.c['date'] == payments_context.c['date']
    )

    context = pd.read_sql(
        context_query.statement,
        context_query.session.bind
    )

    mysql_predplatne_session.close()
    mysql_beam_session.close()

    return context


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

