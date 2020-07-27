import re
import pandas as pd
from sqlalchemy import select, column
from sqlalchemy.types import Float, DATE, String, Integer, TIMESTAMP
from sqlalchemy import and_, func, case, text
from sqlalchemy.sql.expression import cast, literal
from datetime import timedelta, datetime
from .config import build_derived_metrics_config, PROFILE_COLUMNS, LABELS, generate_4_hour_interval_column_names, \
    SUPPORTED_JSON_FIELDS_KEYS, FeatureColumns
from typing import List, Dict, Any
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table
from .db_utils import create_connection, UserIdHandler
from .config import sanitize_column_name, EXPECTED_DEVICE_TYPES
from sqlalchemy.engine import Engine
from google.oauth2 import service_account


def get_sqla_table(table_name, engine):
    meta = MetaData(bind=engine)
    table = Table(table_name, meta, autoload=True, autoload_with=engine)
    return table


def get_sqlalchemy_tables_w_session(
        table_names: List[str]
) -> (Dict, Engine):
    table_mapping = {}
    database = os.getenv('BIGQUERY_PROJECT_ID')
    _, db_connection = create_connection(
        f'bigquery://{database}',
        {'credentials_path': os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')}
    )
    schema = os.getenv('BIGQUERY_DATASET')
    for table in table_names:
        table_mapping[table] = get_sqla_table(
            table_name=f'{database}.{schema}.{table}', engine=db_connection,
        )

    table_mapping['session'] = sessionmaker(bind=db_connection)()

    return table_mapping, db_connection


tables_to_map = (
    ['aggregated_user_days', 'events', 'browser_users', 'browsers'] +
    [f'aggregated_user_days_{profile_feature_set_name}' for profile_feature_set_name in PROFILE_COLUMNS
     if profile_feature_set_name != 'hour_interval_pageviews']
)

bq_mappings, bq_engine = get_sqlalchemy_tables_w_session(
    table_names=tables_to_map
)

bq_session = bq_mappings['session']
aggregated_user_days = bq_mappings['aggregated_user_days']
events = bq_mappings['events']
browser_users = bq_mappings['browser_users']
browsers = bq_mappings['browsers']


def get_feature_frame_via_sqlalchemy(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int = 30,
        positive_event_lookahead: int = 33
):
    rolling_daily_user_profile = get_user_profiles_table()

    query = f'''
        SELECT
            user_id,
            date,
            outcome,
            feature_aggregation_functions,
            {','.join([column.name for column in rolling_daily_user_profile.columns if 'features' in column.name])}
        FROM
            {os.getenv('BIGQUERY_DATASET')}.rolling_daily_user_profile
        WHERE
            date >= @start_time
            AND date <= @end_time
            AND window_days = @window_days
            AND event_lookahead = @event_lookahead
    '''

    client_secrets_path = os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')
    database = os.getenv('BIGQUERY_PROJECT_ID')
    _, db_connection = create_connection(
        f'bigquery://{database}',
        engine_kwargs={'credentials_path': client_secrets_path}
    )

    credentials = service_account.Credentials.from_service_account_file(
        client_secrets_path,
    )

    import pandas_gbq
    feature_frame = pandas_gbq.read_gbq(
        query,
        project_id=database,
        credentials=credentials,
        use_bqstorage_api=True,
        configuration={
            'query': {
                'parameterMode': 'NAMED',
                'queryParameters': [
                    {
                        'name': 'start_time',
                        'parameterType': {'type': 'DATE'},
                        'parameterValue': {'value': str(start_time.date())}
                    },
                    {
                        'name': 'end_time',
                        'parameterType': {'type': 'DATE'},
                        'parameterValue': {'value': str(end_time.date())}
                    },
                    {
                        'name': 'window_days',
                        'parameterType': {'type': 'INT64'},
                        'parameterValue': {'value': moving_window_length}
                    },
                    {
                        'name': 'event_lookahead',
                        'parameterType': {'type': 'INT64'},
                        'parameterValue': {'value': positive_event_lookahead}
                    },
                ]
            }
        }
    )

    return feature_frame


def get_user_profiles_table():
    database = os.getenv('BIGQUERY_PROJECT_ID')
    schema = os.getenv('BIGQUERY_DATASET')
    rolling_daily_user_profile = get_sqla_table(f'{database}.{schema}.rolling_daily_user_profile', bq_engine)

    return rolling_daily_user_profile


def insert_daily_feature_frame(
        date: datetime = datetime.utcnow().date(),
        moving_window_length: int = 7,
        feature_aggregation_functions: Dict[str, func] = None,
        positive_event_lookahead: int = 33,
        meta_columns_w_values: Dict[str, Any] = {}
):
    full_query = get_full_features_query(
        date,
        date,
        moving_window_length,
        feature_aggregation_functions,
        positive_event_lookahead
    )

    meta_columns = [
        'date', 'user_id', 'outcome', 'pipeline_version',
        'created_at', 'window_days', 'event_lookahead', 'feature_aggregation_functions'
    ]

    features_in_data = [column.name for column in full_query.columns if column.name not in meta_columns]
    features_expected = FeatureColumns(
        feature_aggregation_functions,
        date,
        date,
    )

    dumper = ColumnJsonDumper(
        full_query,
        features_in_data,
        features_expected
    )
    dumper.dump_feature_sets()
    feature_set_dumps = dumper.get_feature_set_dumps()

    full_query = bq_session.query(
        full_query.c['date'].cast(DATE),
        full_query.c['user_id'],
        full_query.c['outcome'],
        literal(meta_columns_w_values['pipeline_version']).cast(String).label('pipeline_version'),
        literal(meta_columns_w_values['created_at']).cast(TIMESTAMP).label('created_at'),
        literal(meta_columns_w_values['window_days']).cast(Integer).label('window_days'),
        literal(meta_columns_w_values['event_lookahead']).cast(Integer).label('event_lookahead'),
        literal(meta_columns_w_values['feature_aggregation_functions']).cast(String).label(
            'feature_aggregation_functions'
        ),
        *feature_set_dumps
    )

    rolling_daily_user_profile = get_user_profiles_table()
    date_data_sample = bq_session.query(
        *[rolling_daily_user_profile.c[column.name].label(column.name) for column in rolling_daily_user_profile.columns]
    ).filter(
        and_(
            rolling_daily_user_profile.c['date'] == cast(date, DATE),
            *[rolling_daily_user_profile.c[meta_column] == meta_columns_w_values[meta_column]
              for meta_column in meta_columns_w_values.keys() if meta_column != 'created_at'],
        )
    ).limit(1)

    if len(date_data_sample.all()) == 1:
        delete = rolling_daily_user_profile.delete().where(
            and_(
                rolling_daily_user_profile.c['date'] == cast(date, DATE),
                *[rolling_daily_user_profile.c[meta_column] == meta_columns_w_values[meta_column]
                  for meta_column in meta_columns_w_values.keys() if meta_column != 'created_at']
            )
        )

        delete.execute()

    insert = rolling_daily_user_profile.insert().from_select(
        meta_columns + [
            'features__numeric_columns',
            'features__profile_numeric_columns_from_json_fields__referer_mediums',
            'features__profile_numeric_columns_from_json_fields__categories',
            'features__time_based_columns__hour_ranges',
            'features__time_based_columns__days_of_week',
            'features__categorical_columns',
            'features__bool_columns',
            'features__numeric_columns_with_window_variants',
            'features__device_based_columns'
        ],
        full_query
    )

    insert.execute()


class ColumnJsonDumper:
    def __init__(
            self,
            full_query,
            features_in_data,
            features_expected
    ):
        self.full_query = full_query
        self.features_in_data = features_in_data
        self.features_expected = features_expected
        self.feature_set_dumps = []
        self.feature_set_name_being_processed = ''
        self.column_defaults = {
            'numeric': '0.0',
            'bool': 'False',
            'categorical': '',
            'time': '0.0',
            'device': '0.0'
        }

    def get_feature_set_dumps(self):
        return self.feature_set_dumps

    def dump_feature_sets(self):
        for feature_set_name, feature_set in {
            'numeric_columns': self.features_expected.numeric_columns,
            'profile_numeric_columns_from_json_fields':  self.features_expected.profile_numeric_columns_from_json_fields,
            'time_based_columns': self.features_expected.time_based_columns,
            'categorical_columns': self.features_expected.categorical_columns,
            'bool_columns': self.features_expected.bool_columns,
            'numeric_columns_with_window_variants': self.features_expected.numeric_columns_window_variants,
            'device_based_columns': self.features_expected.device_based_features
        }.items():
            self.feature_set_name_being_processed = feature_set_name
            if isinstance(feature_set, list):
                feature_set.sort()
                self.feature_set_dumps.append(
                    self.create_json_string_from_feature_set(
                        feature_set
                    ).label(f'features__{feature_set_name}')
                )

            elif isinstance(feature_set, dict):
                for key in feature_set.keys():
                    feature_set[key].sort()
                    self.feature_set_dumps.append(
                        self.create_json_string_from_feature_set(
                            feature_set[key]
                        ).label(f'features__{feature_set_name}_{key}')
                    )

    def create_json_string_from_feature_set(
            self,
            feature_set: List[str]
    ) -> func:
        feature_columns_json_dump = func.concat(
            '{',
            *[
                func.concat(
                    # Prepare each element to be wrapped inside curly brackets
                    ',' if i != 0 else '',
                    # opening double quotes for the key
                    '"',
                    # name of the column as key
                    re.sub('feature_query_w_outcome_', '', column),
                    # closing double quotes for the key
                    '"',
                    # colon and opening double quotes for the value
                    ': "',
                    # actual value
                    self.handle_single_column(column),
                    # closing double quotes for the value
                    '"'
                )
                for i, column in enumerate(feature_set)
            ],
            '}'
        )

        return feature_columns_json_dump

    def handle_single_column(
            self,
            handled_column
    ):
        default_null_filler_value = [value for key, value in self.column_defaults.items()
                                     if key in self.feature_set_name_being_processed][0]

        if handled_column in self.features_in_data:
            adjusted_null_values = case(
                [
                    (self.full_query.c[handled_column] == None, literal(default_null_filler_value))
                ],
                else_=self.full_query.c[handled_column].cast(String)
            ).label(handled_column)
            return adjusted_null_values

        else:
            missing_column_filler = literal(default_null_filler_value).label(handled_column)
            return missing_column_filler


def get_full_features_query(
        start_time: datetime,
        end_time: datetime,
        moving_window_length: int = 7,
        feature_aggregation_functions: Dict[str, func] = None,
        positive_event_lookahead: int = 1
):
    if feature_aggregation_functions is None:
        feature_aggregation_functions = {'avg': func.avg}

    filtered_data = filter_by_date(
        # We retrieve an additional window length lookback of records to correctly construct the rolling window
        # for the records on the 1st day of the time period we intend to use
        start_time - timedelta(days=moving_window_length),
        end_time
    )

    all_date_user_combinations = get_subqueries_for_non_gapped_time_series(
        filtered_data
    )

    device_information = get_device_information_subquery(
        start_time - timedelta(days=moving_window_length),
        end_time
    )

    filtered_data_with_profile_fields, profile_column_names = add_profile_based_features(filtered_data)

    joined_partial_queries = join_all_partial_queries(
        filtered_data_with_profile_fields,
        all_date_user_combinations,
        device_information,
        profile_column_names
    )

    data_with_rolling_windows = calculate_rolling_windows_features(
        joined_partial_queries,
        moving_window_length,
        start_time,
        end_time,
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

    full_query = add_outcomes(
        feature_query,
        start_time,
        positive_event_lookahead
    )

    return full_query


def add_outcomes(
        feature_query,
        start_time: datetime,
        positive_event_lookahead: int = 1,
):
    # The events table holds all the events, not just conversion ones
    relevant_events = bq_session.query(
        events.c['time'].cast(DATE).label('date'),
        events.c['type'].label('outcome'),
        events.c['user_id'].label('user_id')
    ).filter(
        events.c['type'].in_(
            list(LABELS.keys())
        ),
        cast(events.c['time'], DATE) > cast(start_time, DATE),
        cast(events.c['time'], DATE) <= cast(start_time + timedelta(days=positive_event_lookahead), DATE)
    ).subquery()

    # TODO: Remove deduplication, once the event table doesn't contain any
    relevant_events_deduplicated = bq_session.query(
        relevant_events.c['date'],
        relevant_events.c['user_id'],
        # This case when provides logic for dealing with multiple outcomes during the same time period
        # an example is user_id 195379 during the 4/2020 where the user renews, but then cancels and gets
        # a refund (the current pipeline provides both labels)
        case(
            [
                # If there is at leadt one churn event, we identify the user as churned
                (
                    literal(negative_label()).in_(
                        func.unnest(
                            func.array_agg(relevant_events.c['outcome']
                                           )
                        )
                    ), negative_label())
            ],
            # In case of any number of any positive only events we consider the event as a renewal
            else_=positive_labels()
        ).label('outcome')
    ).group_by(
        relevant_events.c['date'].label('date'),
        relevant_events.c['user_id'].label('user_id')
    ).subquery()

    feature_query_w_outcome = bq_session.query(
        feature_query,
        relevant_events_deduplicated.c['outcome']
    ).outerjoin(
        relevant_events_deduplicated,
        and_(
            feature_query.c['user_id'] == relevant_events_deduplicated.c['user_id'],
            feature_query.c['date'] >= func.date_sub(
                relevant_events_deduplicated.c['date'],
                text(f'interval {positive_event_lookahead} day')
            ),
            feature_query.c['date'] <= relevant_events_deduplicated.c['date']
        )
    ).subquery('feature_query_w_outcome')

    return feature_query_w_outcome


def filter_by_date(
        start_time: datetime,
        end_time: datetime
):
    filtered_data = bq_session.query(
        aggregated_user_days.c['date'].label('date'),
        aggregated_user_days.c['user_id'].label('user_id'),
        aggregated_user_days.c['pageviews'].label('pageviews'),
        aggregated_user_days.c['timespent'].label('timespent'),
        aggregated_user_days.c['sessions'].label('sessions'),
        aggregated_user_days.c['sessions_without_ref'].label('sessions_without_ref'),
        aggregated_user_days.c['pageviews_0h_4h'].label('pvs_0h_4h'),
        aggregated_user_days.c['pageviews_4h_8h'].label('pvs_4h_8h'),
        aggregated_user_days.c['pageviews_8h_12h'].label('pvs_8h_12h'),
        aggregated_user_days.c['pageviews_12h_16h'].label('pvs_12h_16h'),
        aggregated_user_days.c['pageviews_16h_20h'].label('pvs_16h_20h'),
        aggregated_user_days.c['pageviews_20h_24h'].label('pvs_20h_24h')
    ).subquery()

    current_data = bq_session.query(
        *[filtered_data.c[column.name].label(column.name) for column in
          filtered_data.columns]
    ).filter(
        filtered_data.c['date'] >= cast(start_time, DATE),
        filtered_data.c['date'] <= cast(end_time, DATE)
    ).subquery()

    user_id_handler = UserIdHandler(
        end_time
    )
    user_id_handler.upload_user_ids()

    database = os.getenv('BIGQUERY_PROJECT_ID')
    schema = os.getenv('BIGQUERY_DATASET')

    user_id_table = get_sqla_table(
        table_name=f'{database}.{schema}.user_ids_filter', engine=bq_engine,
    )

    filtered_data = bq_session.query(current_data).join(
        user_id_table,
        current_data.c['user_id'] == user_id_table.c['user_id'].cast(String)
    ).subquery('filtered_data')

    return filtered_data


def positive_labels():
    return [label for label, label_type in LABELS.items() if label_type == 'positive'][0]


def negative_label():
    return [label for label, label_type in LABELS.items() if label_type == 'negative'][0]


def neutral_label():
    return [label for label, label_type in LABELS.items() if label_type == 'neutral'][0]


def remove_helper_lookback_rows(
        filtered_w_derived_metrics_w_all_time_delta_columns,
        start_time
):
    label_lookback_cause = filtered_w_derived_metrics_w_all_time_delta_columns.c['date'] >= start_time.date()

    features_query = bq_session.query(
        # We re-alias since adding another layer causes sqlalchemy to abbreviate columns
        *[filtered_w_derived_metrics_w_all_time_delta_columns.c[column.name].label(column.name) for column in
          filtered_w_derived_metrics_w_all_time_delta_columns.columns if column.name != 'is_active_on_date'],
        case(
            [
                (filtered_w_derived_metrics_w_all_time_delta_columns.c['is_active_on_date'] == None, False)
            ],
            else_=filtered_w_derived_metrics_w_all_time_delta_columns.c['is_active_on_date']
        ).label('is_active_on_date')
    ).filter(
        label_lookback_cause
    ).subquery('query_without_lookback_rows')

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

    user_ids = bq_session.query(
        filtered_data.c['user_id'],
    ).group_by(filtered_data.c['user_id']).subquery(name='user_ids')

    all_date_user_combinations = bq_session.query(
        select([user_ids, generated_time_series.c['date_gap_filler']]).alias(
            'all_date_user_combinations')).subquery()

    return all_date_user_combinations


def get_prominent_device_list(
    start_time: datetime,
    end_time: datetime
):
    prominent_device_brands_past_90_days = bq_session.query(
        func.lower(browsers.c['device_brand'])
    ).filter(
        browsers.c['date'] >= cast(start_time - timedelta(days=90), DATE),
        browsers.c['date'] <= cast(end_time, DATE)
    ).group_by(browsers.c['device_brand']).all()

    # This hnadles cases such as Toshiba and TOSHIBA (occurs on 2020-02-17) since resulting
    # column names are not case sensitive
    prominent_device_brands_past_90_days = set(
        [device_brand[0] for device_brand in prominent_device_brands_past_90_days]
    )

    return prominent_device_brands_past_90_days


def get_device_information_subquery(
        start_time: datetime,
        end_time: datetime
):
    prominent_device_brands_past_90_days = get_prominent_device_list(
        start_time,
        end_time
    )

    database = os.getenv('BIGQUERY_PROJECT_ID')
    schema = os.getenv('BIGQUERY_DATASET')

    user_id_table = get_sqla_table(
        table_name=f'{database}.{schema}.user_ids_filter', engine=bq_engine,
    )

    device_features = bq_session.query(
        *[
            func.sum(
                case(
                    [(browsers.c[f'is_{device}'] == 't', 1.0)],
                    else_=0.0
                )
            ).label(f'{device}_device')
            for device in EXPECTED_DEVICE_TYPES
        ],
        *[
            func.sum(
                case(
                    [(func.lower(browsers.c['device_brand']) == device_brand, 1.0)],
                    else_=0.0
                )
            ).label(f'{sanitize_column_name(device_brand)}_device_brand')
            for device_brand in prominent_device_brands_past_90_days
        ],
        browser_users.c['user_id'].label('user_id'),
        browser_users.c['date'].label('date')
    ).join(
        browser_users,
        browsers.c['browser_id'] == browser_users.c['browser_id'],
    ).join(
        user_id_table,
        browser_users.c['user_id'] == user_id_table.c['user_id'].cast(String),
    ).filter(
        browsers.c['date'] >= cast(start_time, DATE),
        browsers.c['date'] <= cast(end_time, DATE),
    ).group_by(
        browser_users.c['user_id'].label('user_id'),
        browser_users.c['date'].label('date')
    ).subquery('device_information')

    return device_features


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
    table = bq_mappings[f'aggregated_user_days_{profile_feature_set_name}']
    # TODO: This is only here because of the mismatched naming of tables and columns, hopefully we can unify this ASAP

    pivoted_profile_table = bq_session.query(
        table.c['user_id'].label('user_id'),
        table.c['date'].label('date'),
        *[
            func.sum(case(
                [
                    (
                        table.c[profile_feature_set_name] == profile_column,
                        table.c['pageviews']
                    )
                ],
                else_=0)).label(sanitize_column_name(f'{profile_feature_set_name}_{profile_column}'))
            for profile_column in SUPPORTED_JSON_FIELDS_KEYS[profile_feature_set_name]
        ]
    ).filter(
        table.c['date'] >= start_time,
        table.c['date'] <= end_time
    ).group_by(
        table.c['user_id'],
        table.c['date']
    ).subquery()

    added_profile_columns = [
        # This normalization deals with the fact, that some names might contain dashes which are not allowed in SQL
        # this causes errors when sqlalchemy code gets translated into pure SQL
        sanitize_column_name(f'{profile_feature_set_name}_{profile_column}')
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
            pivoted_profile_table.c['user_id'] == filtered_data_w_profile_columns.c['user_id']
        )
    ).subquery('filtered_data_w_profile_columns')

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

    if start_time is None and end_time is None:
        raise ValueError(
            'No valid dates found after filtering data, it is likely there is no data for the given time period'
        )

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
        all_date_user_combinations,
        device_information,
        profile_column_names
):
    joined_queries = bq_session.query(
        all_date_user_combinations.c['user_id'].label('user_id'),
        all_date_user_combinations.c['date_gap_filler'].label('date'),
        func.extract(text('DAYOFWEEK'), all_date_user_combinations.c['date_gap_filler']).cast(String).label(
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
          column.name not in ['user_id', 'date']]
    ).outerjoin(
        filtered_data_with_profile_fields,
        and_(
            all_date_user_combinations.c['user_id'] == filtered_data_with_profile_fields.c['user_id'],
            all_date_user_combinations.c['date_gap_filler'] == filtered_data_with_profile_fields.c['date'])
    ).outerjoin(
        device_information,
        and_(
            device_information.c['user_id'] == all_date_user_combinations.c['user_id'],
            device_information.c['date'] == all_date_user_combinations.c['date_gap_filler']
        )
    ).subquery('joined_data')

    return joined_queries


def calculate_rolling_windows_features(
        joined_queries,
        moving_window_length: int,
        start_time: datetime,
        end_time: datetime,
        profile_column_names: List[str],
        feature_aggregation_functions
):
    rolling_agg_columns_base = create_rolling_window_columns_config(
        joined_queries,
        profile_column_names,
        moving_window_length,
        feature_aggregation_functions
    )

    rolling_agg_columns_devices = create_device_rolling_window_columns_config(
        start_time,
        end_time,
        joined_queries,
        moving_window_length
    )

    queries_with_basic_window_columns = bq_session.query(
        joined_queries,
        *rolling_agg_columns_base,
        *rolling_agg_columns_devices,
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
                    joined_queries.c['user_id'],
                    joined_queries.c['date']),
                (start_time - timedelta(days=2)).date()
            ),
            text('day')).label('days_since_last_active'),
    ).subquery('queries_with_basic_window_columns')

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


def create_device_rolling_window_columns_config(
    start_time,
    end_time,
    joined_queries,
    moving_window_length
):
    features = FeatureColumns(
        [''],
        start_time,
        end_time
    )

    device_rolling_agg_columns = [
        create_rolling_agg_function(
            moving_window_length,
            False,
            func.sum,
            joined_queries.c[device_column],
            joined_queries.c['date'],
            joined_queries.c['user_id']
        ).label(f'{device_column}_sum')
        for device_column in features.device_based_features
        if device_column in [
            column.name for column in joined_queries.columns
        ]
    ]

    return device_rolling_agg_columns


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
            joined_queries.c['user_id'],
            joined_queries.c['date']
        ).cast(Float).label(f'{column_name}_{suffix}')
        for column_name, column_source in column_source_to_name_mapping.items()
        for aggregation_function_alias, aggregation_function in feature_aggregation_functions.items()
        for suffix, is_half_window in get_rolling_agg_window_variants(aggregation_function_alias).items()
        if f'{column_name}_{suffix}' != 'days_active_count'
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
            joined_queries.c['user_id'],
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
    joined_partial_queries.c[f'pageview_{first_aggregation_function_alias}'] > 0
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
    ).filter(and_(*finalizing_filter)).subquery('filtered_w_derived_metrics')

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
    ).subquery('filtered_w_derived_metrics_w_all_time_delta_columns')

    return filtered_w_derived_metrics_w_all_time_delta_columns
