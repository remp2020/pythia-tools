import re
import pandas as pd
from sqlalchemy import select, column
from sqlalchemy.types import Float, DATE, String
from sqlalchemy import and_, func, case, text
from sqlalchemy.sql.expression import cast
from datetime import timedelta, datetime
from .config import build_derived_metrics_config, PROFILE_COLUMNS, LABELS, generate_4_hour_interval_column_names, \
    SUPPORTED_JSON_FIELDS_KEYS
from typing import List, Dict, Any
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table
from .db_utils import create_connection
from .enums import DataRetrievalMode


def get_sqla_table(table_name, engine):
    meta = MetaData(bind=engine)
    table = Table(table_name, meta, autoload=True, autoload_with=engine)
    return table


def get_sqlalchemy_tables_w_session(
        db_connection_string_name: str,
        schema: str,
        table_names: List[str],
        engine_kwargs: Dict[str, Any] = None,
) -> Dict:
    table_mapping = {}
    _, db_connection = create_connection(os.getenv(db_connection_string_name), engine_kwargs)
    database = os.getenv('BQ_DATABASE')
    for table in table_names:
        table_mapping[table] = get_sqla_table(
            table_name=f'{database}.{schema}.{table}', engine=db_connection,
        )

    table_mapping['session'] = sessionmaker(bind=db_connection)()

    return table_mapping


tables_to_map = (
    ['aggregated_browser_days', 'events'] +
    [f'aggregated_browser_days_{profile_feature_set_name}' for profile_feature_set_name in PROFILE_COLUMNS
     if profile_feature_set_name != 'hour_interval_pageviews']
)


bq_mappings = get_sqlalchemy_tables_w_session(
    'BQ_CONNECTION_STRING',
    schema='pythia',
    table_names=tables_to_map,
    engine_kwargs={'credentials_path': '../../gcloud_client_secrets.json'}
)

bq_session = bq_mappings['session']
aggregated_browser_days = bq_mappings['aggregated_browser_days']
events = bq_mappings['events']


def get_feature_frame_via_sqlalchemy(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int = 7,
        feature_aggregation_functions: Dict[str, func] = None,
        data_retrieval_mode: DataRetrievalMode = DataRetrievalMode.PREDICT_DATA,
        positive_event_lookahead: int = 1
):
    if feature_aggregation_functions is None:
        feature_aggregation_functions = {'avg': func.avg}

    full_query = bq_session.query(get_full_features_query(
        start_time,
        end_time,
        moving_window_length,
        feature_aggregation_functions,
        data_retrieval_mode,
        positive_event_lookahead
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
        feature_aggregation_functions: Dict[str, func] = None,
        data_retrieval_mode: DataRetrievalMode = DataRetrievalMode.PREDICT_DATA,
        positive_event_lookahead: int = 1
):
    if feature_aggregation_functions is None:
        feature_aggregation_functions = {'count': func.sum}

    filtered_data = filter_by_date(
        # We retrieve an additional window length lookback of records to correctly construct the rolling window
        # for the records on the 1st day of the time period we intend to use
        start_time - timedelta(days=moving_window_length),
        end_time,
        # These determine if we retrieve past positives or not
        data_retrieval_mode
    )

    all_date_browser_combinations = get_subqueries_for_non_gapped_time_series(
        filtered_data
    )

    device_information = get_device_information_subquery(filtered_data)

    filtered_data_with_profile_fields, profile_column_names = add_profile_based_features(filtered_data)

    joined_partial_queries = join_all_partial_queries(
        filtered_data_with_profile_fields,
        all_date_browser_combinations,
        device_information,
        profile_column_names
    )

    data_with_rolling_windows = calculate_rolling_windows_features(
        joined_partial_queries,
        moving_window_length,
        start_time,
        profile_column_names,
        feature_aggregation_functions
    )

    filtered_w_derived_metrics = filter_joined_queries_adding_derived_metrics(
        data_with_rolling_windows,
        feature_aggregation_functions
    )

    filtered_w_derived_metrics_w_all_time_delta_columns = add_all_time_delta_columns(
        filtered_w_derived_metrics
    )

    feature_query = remove_helper_lookback_rows(
        filtered_w_derived_metrics_w_all_time_delta_columns,
        start_time
    )

    full_query = add_outcomes(feature_query)

    return full_query


def add_outcomes(
        feature_query,
        positive_event_lookahead: int = 1
):
    # The events table holds all the events, not just conversion ones
    relevant_events = bq_session.query(
        events.c['time'].cast(DATE).label('date'),
        events.c['type'].label('outcome'),
        events.c['browser_id'].label('browser_id')
    ).filter(
        events.c['type'].in_(
            list(LABELS.keys())
        )
    ).subquery()

    feature_query_w_outcome = bq_session.query(
        feature_query,
        case(
            [
                (
                    relevant_events.c['outcome'].in_(positive_labels()),
                    relevant_events.c['outcome']
                )
            ],
            else_=negative_label()
        ).label('outcome')
    ).outerjoin(
        relevant_events,
        and_(
            feature_query.c['browser_id'] == relevant_events.c['browser_id'],
            feature_query.c['date'] >= func.date_sub(
                relevant_events.c['date'],
                text(f'interval {positive_event_lookahead} day')
            ),
            feature_query.c['date'] < relevant_events.c['date']
        )
    ).subquery()

    return feature_query_w_outcome


def filter_by_date(
        start_time: datetime,
        end_time: datetime,
        data_retrieval_model: DataRetrievalMode,
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
        aggregated_browser_days.c['pageviews_0h_4h'].label('pvs_0h_4h'),
        aggregated_browser_days.c['pageviews_4h_8h'].label('pvs_4h_8h'),
        aggregated_browser_days.c['pageviews_8h_12h'].label('pvs_8h_12h'),
        aggregated_browser_days.c['pageviews_12h_16h'].label('pvs_12h_16h'),
        aggregated_browser_days.c['pageviews_16h_20h'].label('pvs_16h_20h'),
        aggregated_browser_days.c['pageviews_20h_24h'].label('pvs_20h_24h')
    ).subquery()

    current_data = bq_session.query(
        *[filtered_data.c[column.name].label(column.name) for column in
          filtered_data.columns]
    ).filter(
        filtered_data.c['date'] >= cast(start_time, DATE),
        filtered_data.c['date'] <= cast(end_time, DATE)
    )

    if data_retrieval_model == DataRetrievalMode.MODEL_TRAIN_DATA:
        filtered_data = current_data.union_all(past_positive_browser_ids(filtered_data, start_time)).subquery()
    else:
        filtered_data = current_data.subquery()

    return filtered_data


def positive_labels():
    return [label for label, label_type in LABELS.items() if label_type == 'positive']


def negative_label():
    return [label for label, label_type in LABELS.items() if label_type == 'negative'][0]


def past_positive_browser_ids(
        filtered_data,
        start_time: datetime,
):
    browser_ids_with_positive_outcomes = bq_session.query(
        events.c['browser_id']
    ).filter(
        events.c['time'].cast(DATE) >= cast(start_time - timedelta(days=90), DATE),
        events.c['time'].cast(DATE) <= cast(start_time, DATE),
        events.c['type'].in_(positive_labels())
    )

    past_positives = bq_session.query(
        *[filtered_data.c[column.name].label(column.name) for column in
          filtered_data.columns]
    ).filter(
        filtered_data.c['date'] >= cast(start_time - timedelta(days=90), DATE),
        filtered_data.c['date'] <= cast(start_time, DATE),
        filtered_data.c['browser_id'].in_(browser_ids_with_positive_outcomes)
    )

    return past_positives


def remove_helper_lookback_rows(
        filtered_w_derived_metrics_w_all_time_delta_columns,
        start_time
):
    label_lookback_cause = filtered_w_derived_metrics_w_all_time_delta_columns.c['date'] >= (
        (start_time - timedelta(days=90)).date()
    )

    features_query = bq_session.query(
        # We re-alias since adding another layer causes sqlalchemy to abbreviate columns
        *[filtered_w_derived_metrics_w_all_time_delta_columns.c[column.name].label(column.name) for column in
          filtered_w_derived_metrics_w_all_time_delta_columns.columns]
    ).filter(
        label_lookback_cause
    ).subquery()

    return features_query


def get_subqueries_for_non_gapped_time_series(
        filtered_data
):
    start_time, end_time = bq_session.query(
        func.min(filtered_data.c['date']),
        func.max(filtered_data.c['date']),
    ).all()[0]

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
        select([browser_ids, generated_time_series.c['date_gap_filler']]).alias(
            'all_date_browser_combinations')).subquery()

    return all_date_browser_combinations


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


def get_profile_columns(
        filtered_data_w_profile_columns,
        profile_feature_set_name,
        start_time,
        end_time
):
    table = bq_mappings[f'aggregated_browser_days_{profile_feature_set_name}']
    
    pivoted_profile_table = bq_session.query(
        table.c['browser_id'].label('browser_id'),
        table.c['date'].label('date'),
        *[
            func.sum(case(
                [
                    (
                        table.c[profile_feature_set_name] == profile_column,
                        table.c['pageviews']
                    )
                ],
                else_=0)).label(f'{profile_feature_set_name}_{profile_column}'.replace('-', '_'))
            for profile_column in SUPPORTED_JSON_FIELDS_KEYS[profile_feature_set_name]
        ]
    ).filter(
        table.c['date'] >= cast(start_time, DATE),
        table.c['date'] <= cast(end_time, DATE)
    ).group_by(
        table.c['browser_id'],
        table.c['date']
    ).subquery()

    added_profile_columns = [
        # This normalization deals with the fact, that some names might contain dashes which are not allowed in SQL
        # this causes errors when sqlalchemy code gets translated into pure SQL
        f'{profile_feature_set_name}_{profile_column}'.replace('-', '_')
        for profile_column in SUPPORTED_JSON_FIELDS_KEYS[profile_feature_set_name]
    ]

    filtered_data_w_profile_columns = bq_session.query(
        filtered_data_w_profile_columns,
        *[pivoted_profile_table.c[profile_column].label(profile_column)
          for profile_column in added_profile_columns]
    ).outerjoin(
        pivoted_profile_table,
        and_(
            pivoted_profile_table.c['date'] == filtered_data_w_profile_columns.c['date'],
            pivoted_profile_table.c['browser_id'] == filtered_data_w_profile_columns.c['browser_id']
        )
    ).subquery()

    return filtered_data_w_profile_columns, added_profile_columns


def add_profile_based_features(filtered_data):
    profile_based_columns = dict()
    profile_based_columns['hour_interval_pageviews'] = add_4_hour_intervals(
        filtered_data
    )

    profile_columns = list(profile_based_columns['hour_interval_pageviews'].keys())

    start_time, end_time = bq_session.query(
        func.min(filtered_data.c['date']),
        func.max(filtered_data.c['date']),
    ).all()[0]

    filtered_data_w_profile_columns = filtered_data

    for json_column in PROFILE_COLUMNS:
        if json_column != 'hour_interval_pageviews':
            filtered_data_w_profile_columns, new_profile_columns = get_profile_columns(
                filtered_data_w_profile_columns,
                json_column,
                start_time,
                end_time
            )
            profile_columns.extend(new_profile_columns)

    return filtered_data_w_profile_columns, profile_columns


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
        filtered_data_with_profile_fields,
        all_date_browser_combinations,
        device_information,
        profile_column_names
):
    joined_queries = bq_session.query(
        all_date_browser_combinations.c['browser_id'].label('browser_id'),
        all_date_browser_combinations.c['user_ids'].label('user_ids'),
        all_date_browser_combinations.c['date_gap_filler'].label('date'),
        func.extract(text('DAYOFWEEK'), all_date_browser_combinations.c['date_gap_filler']).cast(String).label(
            'day_of_week'),
        filtered_data_with_profile_fields.c['date'].label('date_w_gaps'),
        (filtered_data_with_profile_fields.c['pageviews'] > 0.0).label('is_active_on_date'),
        filtered_data_with_profile_fields.c['pageviews'],
        filtered_data_with_profile_fields.c['timespent'],
        filtered_data_with_profile_fields.c['sessions_without_ref'],
        filtered_data_with_profile_fields.c['sessions'],
        # Add all columns created from json_fields
        *[filtered_data_with_profile_fields.c[profile_column].label(profile_column) for profile_column in
          profile_column_names],
        # Unpack all device information columns except ones already present in other queries
        *[device_information.c[column.name] for column in device_information.columns if
          column.name not in ['browser_id', 'date']],
    ).outerjoin(
        filtered_data_with_profile_fields,
        and_(
            all_date_browser_combinations.c['browser_id'] == filtered_data_with_profile_fields.c['browser_id'],
            all_date_browser_combinations.c['date_gap_filler'] == filtered_data_with_profile_fields.c['date'])
    ).outerjoin(
        device_information,
        device_information.c['browser_id'] == all_date_browser_combinations.c['browser_id']
    ).subquery()

    return joined_queries


def calculate_rolling_windows_features(
        joined_queries,
        moving_window_length: int,
        start_time: datetime,
        profile_column_names: List[str],
        feature_aggregation_functions
):
    rolling_agg_columns_base = create_rolling_window_columns_config(
        joined_queries,
        profile_column_names,
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

    combinations = {
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

    # 4-hour intervals
    combinations.update(
        {time_key_column_name: joined_queries.c[time_key_column_name]
         for time_key_column_name in interval_names}
    )

    return combinations


def create_rolling_window_columns_config(
        joined_queries,
        profile_column_names,
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
            column: joined_queries.c[column] for column in profile_column_names
        }
    }

    time_column_config = create_time_window_vs_day_of_week_combinations(
        joined_queries
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
          if column.name != 'row_number'],
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
