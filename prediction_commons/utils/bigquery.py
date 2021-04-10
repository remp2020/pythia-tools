import re
import pandas as pd
from sqlalchemy import select, column
from sqlalchemy.types import Float, DATE, String, Integer, TIMESTAMP
from sqlalchemy import and_, func, case, text
from sqlalchemy.sql.expression import cast, literal
from datetime import timedelta, datetime
from .config import PROFILE_COLUMNS, generate_4_hour_interval_column_names, FeatureColumns, sanitize_column_name
from typing import List, Dict, Any
import os
from google.oauth2 import service_account

from prediction_commons.utils.enums import WindowHalfDirection, OutcomeLabelCategory
from prediction_commons.utils.db_utils import create_connection, get_sqlalchemy_tables_w_session, get_sqla_table


def get_profiles_table(
    model_record_level: str
):
    profile_table_name = f'rolling_daily_{model_record_level}_profile'
    bq_mappings, bq_engine = get_sqlalchemy_tables_w_session(
        table_names=[profile_table_name]
    )
    rolling_daily_profile = get_sqla_table(
        bq_mappings[profile_table_name], bq_engine
    )

    return rolling_daily_profile


class DataDownloader:
    LABELS = {}

    def __init__(
            self,
            start_time: datetime,
            end_time: datetime,
            moving_window_length,
            model_record_level: str,
            historically_oversampled_outcome_type: OutcomeLabelCategory = None
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.moving_window_length = moving_window_length
        self.model_record_level = model_record_level
        self.historically_oversampled_outcome_type = historically_oversampled_outcome_type

    def get_minority_label_filter(self):
        minority_label_filter = ''
        if self.historically_oversampled_outcome_type is not None:
            minority_labels = [f"'{outcome_label}'" for outcome_label, outcome_label_category in self.LABELS.items() if
                               outcome_label_category == self.historically_oversampled_outcome_type.value]
            minority_labels = ','.join(minority_labels)
            minority_label_filter = f' OR (outcome IN ({minority_labels}))'

        return minority_label_filter

    def get_retrieval_query(self, table):
        minority_label_filter = self.get_minority_label_filter()

        query = f'''
            SELECT
                {self.model_record_level}_id,
                date,
                outcome_date,
                outcome,
                feature_aggregation_functions,
                {','.join([column.name for column in table.columns if 'features' in column.name])}
            FROM
                {os.getenv('BIGQUERY_DATASET')}.rolling_daily_{self.model_record_level}_profile
            WHERE
                window_days = @window_days
                AND outcome_date <= @end_time
                AND ((outcome_date >= @start_time){minority_label_filter})
        '''

        return query

    def get_feature_frame_via_sqlalchemy(
            self,
    ):
        rolling_daily_profile_table = get_profiles_table(self.model_record_level)

        query = self.get_retrieval_query(rolling_daily_profile_table)

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
                            'parameterValue': {'value': str(self.start_time.date())}
                        },
                        {
                            'name': 'end_time',
                            'parameterType': {'type': 'DATE'},
                            'parameterValue': {'value': str(self.end_time.date())}
                        },
                        {
                            'name': 'window_days',
                            'parameterType': {'type': 'INT64'},
                            'parameterValue': {'value': self.moving_window_length}
                        },
                    ]
                }
            }
        )

        return feature_frame


def create_rolling_agg_function(
        moving_window_length: int,
        agg_function,
        column,
        partitioning_column,
        ordering_column,
        window_half_direction: WindowHalfDirection = WindowHalfDirection.FULL
):
    lower_bound = moving_window_length
    # If we're aggregating for a half window, halve the reach of the lower bound
    if window_half_direction in [WindowHalfDirection.FIRST_HALF, WindowHalfDirection.LAST_HALF]:
        lower_bound = round(lower_bound / 2, 0)
    # If there is an odd number of days in the moving window, one the half windows needs to have a shortened time
    # we've decided for this to be the second window part as for comparisons of change between first and second half
    # we prefer to have more recent data stand out
    if moving_window_length % 2 > 0 and window_half_direction == WindowHalfDirection.LAST_HALF:
        lower_bound = lower_bound - 1
    # For last window half, we need to order the window in a descending order for which the window_half_direction is
    # used
    if window_half_direction != WindowHalfDirection.LAST_HALF:
        # since the lower bound is defined as a negative offset, we multiply by -1
        rows = (int(-1 * lower_bound), 0)
    else:
        rows = (-1 * moving_window_length, int(-1 * lower_bound))

    result_function = agg_function(column).over(
        partition_by=partitioning_column,
        order_by=ordering_column,
        rows=rows
    )

    return result_function


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
            'time': '0.0'
        }

        self.feature_type_to_column_mapping = {
            'numeric_columns': self.features_expected.numeric_columns,
            'profile_numeric_columns_from_json_fields':  self.features_expected.profile_numeric_columns_from_json_fields,
            'time_based_columns': self.features_expected.time_based_columns,
            'categorical_columns': [
                column.name for column in full_query.columns
                for category in self.features_expected.CATEGORICAL_COLUMNS if column.name == category
            ],
            'bool_columns': self.features_expected.BOOL_COLUMNS,
            'numeric_columns_with_window_variants': self.features_expected.numeric_columns_window_variants
        }
        
    def get_feature_set_dumps(self):
        return self.feature_set_dumps

    def dump_feature_sets(self):
        for feature_set_name, feature_set in self.feature_type_to_column_mapping.items():
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
                        ).label(f'features__{feature_set_name}__{key}')
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


class FeatureBuilder:
    labels = {}
    feature_columns = FeatureColumns
    tables_to_map = (
            ['browsers'] +
            [f'aggregated_user_days_{profile_feature_set_name}' for profile_feature_set_name in PROFILE_COLUMNS
             if profile_feature_set_name != 'hour_interval_pageviews']
    )

    bq_mappings, bq_engine = get_sqlalchemy_tables_w_session(
        table_names=tables_to_map
    )

    bq_session = bq_mappings['session']
    browsers = bq_mappings['browsers']
    column_dumper = ColumnJsonDumper

    def __init__(
            self,
            aggregation_time: datetime,
            moving_window_length: int,
            feature_aggregation_functions: Dict[str, func],
            model_record_level: str
    ):
        self.aggregation_time = aggregation_time
        self.moving_window_length = moving_window_length
        if feature_aggregation_functions is None:
            feature_aggregation_functions = {'avg': func.avg}
        self.feature_aggregation_functions = feature_aggregation_functions
        # This should currently resolve to browser_id / user_id
        self.model_record_level = model_record_level

    def insert_daily_feature_frame(
            self,
            meta_columns_w_values: Dict[str, Any] = {}
    ):
        full_query = self.get_full_features_query()
        meta_columns = [
            'date', f'{self.model_record_level}_id', 'outcome', 'outcome_date', 'pipeline_version',
            'created_at', 'window_days', 'feature_aggregation_functions'
        ]

        features_in_data = [column.name for column in full_query.columns if column.name not in meta_columns]
        features_expected = self.feature_columns(
            self.feature_aggregation_functions,
            self.aggregation_time
        )
        
        dumper = self.column_dumper(
            full_query,
            features_in_data,
            features_expected
        )

        dumper.dump_feature_sets()
        feature_set_dumps = dumper.get_feature_set_dumps()

        full_query = self.bq_session.query(
            full_query.c['date'].cast(DATE).label('date'),
            full_query.c[f'{self.model_record_level}_id'],
            full_query.c['outcome'],
            full_query.c['outcome_date'],
            literal(meta_columns_w_values['pipeline_version']).cast(String).label('pipeline_version'),
            literal(meta_columns_w_values['created_at']).cast(TIMESTAMP).label('created_at'),
            literal(meta_columns_w_values['window_days']).cast(Integer).label('window_days'),
            literal(meta_columns_w_values['feature_aggregation_functions']).cast(String).label(
                'feature_aggregation_functions'
            ),
            *feature_set_dumps
        )
        
        rolling_daily_user_profile = get_profiles_table(self.model_record_level)
        date_data_sample = self.bq_session.query(
            *[rolling_daily_user_profile.c[column.name].label(column.name) for column in
              rolling_daily_user_profile.columns]
        ).filter(
            and_(
                rolling_daily_user_profile.c['date'] == cast(self.aggregation_time, DATE),
                *[rolling_daily_user_profile.c[meta_column] == meta_columns_w_values[meta_column]
                  for meta_column in meta_columns_w_values.keys() if meta_column != 'created_at'],
            )
        ).limit(1)

        if len(date_data_sample.all()) == 1:
            delete = rolling_daily_user_profile.delete().where(
                and_(
                    rolling_daily_user_profile.c['date'] == cast(self.aggregation_time, DATE),
                    *[rolling_daily_user_profile.c[meta_column] == meta_columns_w_values[meta_column]
                      for meta_column in meta_columns_w_values.keys() if meta_column != 'created_at']
                )
            )

            delete.execute()

        insert = rolling_daily_user_profile.insert().from_select(
            meta_columns + [feature_dump.name for feature_dump in feature_set_dumps],
            full_query
        )

        insert.execute()

    def get_full_features_query(self):
        filtered_data = self.filter_by_date()

        all_date_user_combinations = self.get_subqueries_for_non_gapped_time_series(
            filtered_data
        )

        device_information = self.get_device_information_subquery()

        filtered_data_with_profile_fields, profile_column_names = self.add_profile_based_features(filtered_data)

        joined_partial_queries = self.join_all_partial_queries(
            filtered_data_with_profile_fields,
            all_date_user_combinations,
            device_information,
            profile_column_names
        )

        data_with_rolling_windows = self.calculate_rolling_windows_features(
            joined_partial_queries,
            profile_column_names
        )

        filtered_w_derived_metrics = self.filter_joined_queries_adding_derived_metrics(
            data_with_rolling_windows
        )

        filtered_w_derived_metrics_w_all_time_delta_columns = self.add_all_time_delta_columns(
            filtered_w_derived_metrics,
        )

        feature_query = self.remove_helper_lookback_rows(
            filtered_w_derived_metrics_w_all_time_delta_columns,
        )

        full_query = self.add_outcomes(
            feature_query
        )

        return full_query

    def add_outcomes(
            self,
            feature_query,
    ):
        return self.bq_session.query()

    def filter_by_date(
        self
    ):

        return self.bq_session.query()

    def remove_helper_lookback_rows(
            self,
            filtered_w_derived_metrics_w_all_time_delta_columns,
    ):
        label_lookback_cause = filtered_w_derived_metrics_w_all_time_delta_columns.c['date'] >= self.aggregation_time.date()

        features_query = self.bq_session.query(
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
            self,
            filtered_data
    ):
        start_time, end_time = self.bq_session.query(
            func.min(filtered_data.c['date']),
            func.max(filtered_data.c['date']),
        ).all()[0]

        generated_time_series = self.bq_session.query(select([column('dates').label('date_gap_filler')]).select_from(
            func.unnest(
                func.generate_date_array(start_time, end_time)
            ).alias('dates')
        )).subquery()

        ids = self.bq_session.query(
            filtered_data.c[f'{self.model_record_level}_id'],
        ).group_by(filtered_data.c[f'{self.model_record_level}_id']).subquery(name=f'{self.model_record_level}_ids')

        all_date_user_combinations = self.bq_session.query(
            select([ids, generated_time_series.c['date_gap_filler']]).alias(
                'all_date_user_combinations')).subquery()

        return all_date_user_combinations

    def get_prominent_device_list(
        self,
    ):
        prominent_device_brands_past_90_days = self.bq_session.query(
            func.lower(self.browsers.c['device_brand'])
        ).filter(
            self.browsers.c['date'] >= cast(self.aggregation_time - timedelta(days=90), DATE),
            self.browsers.c['date'] <= cast(self.aggregation_time, DATE)
        ).group_by(self.browsers.c['device_brand']).all()

        # This handles cases such as Toshiba and TOSHIBA (occurs on 2020-02-17) since resulting
        # column names are not case sensitive
        prominent_device_brands_past_90_days = set(
            [device_brand[0] for device_brand in prominent_device_brands_past_90_days]
        )

        return prominent_device_brands_past_90_days

    def get_device_information_subquery(
            self
    ):
        return self.bq_session.query()

    def get_profile_columns(
            self,
            filtered_data_w_profile_columns,
            profile_feature_set_name,
            start_time,
            end_time
    ):
        table = self.bq_mappings[f'aggregated_{self.model_record_level}_days_{profile_feature_set_name}']

        pivoted_profile_table = self.bq_session.query(
            table.c[f'{self.model_record_level}_id'].label(f'{self.model_record_level}_id'),
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
                for profile_column in self.feature_columns.SUPPORTED_JSON_FIELDS_KEYS[profile_feature_set_name]
            ]
        ).filter(
            table.c['date'] >= start_time,
            table.c['date'] <= end_time
        ).group_by(
            table.c[f'{self.model_record_level}_id'],
            table.c['date']
        ).subquery()

        added_profile_columns = [
            # This normalization deals with the fact, that some names might contain dashes which are not allowed in SQL
            # this causes errors when sqlalchemy code gets translated into pure SQL
            sanitize_column_name(f'{profile_feature_set_name}_{profile_column}')
            for profile_column in self.feature_columns.SUPPORTED_JSON_FIELDS_KEYS[profile_feature_set_name]
        ]

        filtered_data_w_profile_columns = self.bq_session.query(
            filtered_data_w_profile_columns,
            *[pivoted_profile_table.c[profile_column].label(profile_column)
              for profile_column in added_profile_columns]
        ).outerjoin(
            pivoted_profile_table,
            and_(
                pivoted_profile_table.c['date'] == filtered_data_w_profile_columns.c['date'],
                pivoted_profile_table.c[f'{self.model_record_level}_id'] == filtered_data_w_profile_columns.c[
                    f'{self.model_record_level}_id'
                ]
            )
        ).subquery('filtered_data_w_profile_columns')

        return filtered_data_w_profile_columns, added_profile_columns

    def add_profile_based_features(
            self,
            filtered_data):
        profile_based_columns = dict()
        profile_based_columns['hour_interval_pageviews'] = self.add_4_hour_intervals(
            filtered_data
        )

        profile_columns = list(profile_based_columns['hour_interval_pageviews'].keys())

        start_time, end_time = self.bq_session.query(
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
                filtered_data_w_profile_columns, new_profile_columns = self.get_profile_columns(
                    filtered_data_w_profile_columns,
                    json_column,
                    start_time,
                    end_time
                )
                profile_columns.extend(new_profile_columns)

        return filtered_data_w_profile_columns, profile_columns

    @staticmethod
    def add_4_hour_intervals(filtered_data):
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
            self,
            filtered_data_with_profile_fields,
            all_date_user_combinations,
            device_information,
            profile_column_names
    ):

        return self.bq_session.query()

    def calculate_rolling_windows_features(
            self,
            joined_queries,
            profile_column_names: List[str],
    ):
        rolling_agg_columns_base = self.create_rolling_window_columns_config(
            joined_queries,
            profile_column_names
        )

        rolling_agg_columns_devices = self.create_device_rolling_window_columns_config(
            joined_queries
        )

        queries_with_basic_window_columns = self.bq_session.query(
            *[column.label(column.name) for column in joined_queries.columns],
            *rolling_agg_columns_base,
            *rolling_agg_columns_devices,
            func.date_diff(
                # last day in the current window
                joined_queries.c['date'],
                # last day active in current window
                # func.coalesce(
                create_rolling_agg_function(
                    self.moving_window_length,
                    func.max,
                    joined_queries.c['date_w_gaps'],
                    joined_queries.c[f'{self.model_record_level}_id'],
                    joined_queries.c['date']
                ),
                # (self.aggregation_time - timedelta(days=2)).date()),
                text('day')).label('days_since_last_active'),
        ).subquery('queries_with_basic_window_columns')

        return queries_with_basic_window_columns

    @staticmethod
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
            ).label(f'dow_{i}')
            for i in range(0, 7)
        }

        # 4-hour intervals
        combinations.update(
            {time_key_column_name: joined_queries.c[time_key_column_name]
             for time_key_column_name in interval_names}
        )

        return combinations

    def create_device_rolling_window_columns_config(
        self,
        joined_queries,
    ):
        features = self.feature_columns(
            [''],
            self.aggregation_time
        )

        device_rolling_agg_columns = [
            create_rolling_agg_function(
                self.moving_window_length,
                func.sum,
                joined_queries.c[device_column],
                joined_queries.c['date'],
                joined_queries.c[f'{self.model_record_level}_id']
            ).label(f'{device_column}_sum')
            for device_column in features.device_based_features
            if device_column in [
                column.name for column in joined_queries.columns
            ]
        ]

        return device_rolling_agg_columns

    def create_rolling_window_columns_config(
            self,
            joined_queries,
            profile_column_names,
    ):
        column_source_to_name_mapping = self.get_base_numeric_features_window_config(
            joined_queries,
            profile_column_names
        )

        time_column_config = self.create_time_window_vs_day_of_week_combinations(
            joined_queries
        )

        column_source_to_name_mapping.update(time_column_config)

        def get_rolling_agg_window_variants(aggregation_function_alias):
            # {naming suffix : related parameter for determining part of full window}
            return {
                f'{aggregation_function_alias}_first_window_half': WindowHalfDirection.FIRST_HALF,
                f'{aggregation_function_alias}_last_window_half': WindowHalfDirection.LAST_HALF,
                f'{aggregation_function_alias}': WindowHalfDirection.FULL
            }

        rolling_agg_columns = []
        # this generates all basic rolling sum columns for both full and second half of the window
        rolling_agg_columns = rolling_agg_columns + [
            create_rolling_agg_function(
                self.moving_window_length,
                aggregation_function,
                column_source,
                joined_queries.c[f'{self.model_record_level}_id'],
                joined_queries.c['date'],
                half_window_direction
            ).cast(Float).label(f'{column_name}_{suffix}')
            for column_name, column_source in column_source_to_name_mapping.items()
            for aggregation_function_alias, aggregation_function in self.feature_aggregation_functions.items()
            for suffix, half_window_direction in get_rolling_agg_window_variants(aggregation_function_alias).items()
            if f'{column_name}_{suffix}' != 'days_active_count'
        ]

        # It only makes sense to aggregate active days by summing, all other aggregations would end up with a value
        # of 1 after we eventually filter out windows with no active days in them, this is why we separate this feature
        # out from the loop with all remaining features
        rolling_agg_columns = rolling_agg_columns + [
            create_rolling_agg_function(
                self.moving_window_length,
                func.sum,
                case(
                    [
                        (
                            joined_queries.c['date_w_gaps'] == None,
                            0
                        )
                    ],
                    else_=1),
                joined_queries.c[f'{self.model_record_level}_id'],
                joined_queries.c['date'],
                half_window_direction
            ).cast(Float).label(f'days_active_{suffix}')
            for suffix, half_window_direction in get_rolling_agg_window_variants('count').items()
        ]

        return rolling_agg_columns

    @staticmethod
    def get_base_numeric_features_window_config(
            joined_queries,
            profile_column_names
    ):
        # {name of the resulting column : source / calculation},
        column_source_to_name_mapping = {
            # All json key columns have their own rolling sums
            **{
                column: joined_queries.c[column] for column in profile_column_names
            }
        }

        return column_source_to_name_mapping

    def filter_joined_queries_adding_derived_metrics(
            self,
            joined_partial_queries
    ):
        derived_metrics_config = {}

        feature_columns = self.feature_columns(
            self.feature_aggregation_functions.keys(),
            self.aggregation_time
        )
        for feature_aggregation_function_alias in self.feature_aggregation_functions.keys():
            derived_metrics_config.update(
                feature_columns.build_derived_metrics_config(feature_aggregation_function_alias)
            )

        filtered_w_derived_metrics = self.bq_session.query(
            *[column.label(column.name)
              for column in joined_partial_queries.columns
              if column.name != 'row_number'],
            *[
                func.coalesce((
                        joined_partial_queries.c[derived_metrics_config[key]['nominator'] + suffix] /
                        joined_partial_queries.c[derived_metrics_config[key]['denominator'] + suffix]
                ), 0.0).label(key + suffix)
                for key in derived_metrics_config.keys()
                for suffix in ['_first_window_half', '_last_window_half']
            ]
        )

        return filtered_w_derived_metrics

    def add_all_time_delta_columns(
        self,
        filtered_w_derived_metrics,
    ):
        filtered_w_derived_metrics_w_all_time_delta_columns = self.bq_session.query(
            filtered_w_derived_metrics,
            *[
                case(
                    [
                        # If the 2nd half of period has 0 for its value, we assign 100 % decline
                        (filtered_w_derived_metrics.c[column.name] == 0, -1),
                        # If the 1st half of period has 0 for its value, we assign 100 % growth
                        (
                            filtered_w_derived_metrics.c[
                                column.name.replace('_last_window_half', '_first_window_half')
                            ] == 0, 1)
                    ],
                    else_=(
                            filtered_w_derived_metrics.c[column.name] /
                            filtered_w_derived_metrics.c[column.name.replace('_last_window_half', '_first_window_half')]
                    ).label(f'relative_{column.name.replace("_last_window_half", "")}_change_first_and_second_half')
                )
                for column in filtered_w_derived_metrics.columns
                if '_last_window_half' in column.name
            ]
        ).subquery('filtered_w_derived_metrics_w_all_time_delta_columns')

        return filtered_w_derived_metrics_w_all_time_delta_columns


def get_model_meta(
        prediction_min_date: datetime,
        model_type: str,
        model_version: str,
        window_days: int
):
    model_name = f'models'
    bq_mappings, bq_engine = get_sqlalchemy_tables_w_session(
        table_names=[model_name]
    )
    models = bq_mappings[model_name]
    bq_session = bq_mappings['session']

    model_meta_query = bq_session.query(
        *[models.c[column.name].label(column.name) for column in models.columns]
    ).filter(
        and_(
            cast(models.c['max_date'], DATE) < cast(prediction_min_date.date(), DATE),
            models.c['model_type'] == model_type,
            models.c['model_version'] == model_version,
            models.c['window_days'] == window_days,
            func.timestamp_diff(
                cast(datetime.utcnow(), TIMESTAMP),
                models.c['train_date'],
                text('minute')
            ) == bq_session.query(
                    func.min(
                        func.timestamp_diff(
                            cast(datetime.utcnow(), TIMESTAMP),
                            models.c['train_date'],
                            text('minute')
                        )
                    )
                ).filter(
                    and_(
                        cast(models.c['max_date'], DATE) < cast(prediction_min_date.date(), DATE),
                        models.c['model_type'] == model_type,
                        models.c['model_version'] == model_version,
                        models.c['window_days'] == window_days,
                    )
            )
        )
    )

    model_meta = pd.read_sql(model_meta_query.statement, model_meta_query.session.bind)

    return model_meta
