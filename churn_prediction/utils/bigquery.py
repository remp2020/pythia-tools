from sqlalchemy.types import DATE, String
from sqlalchemy import and_, func, case, Float, text
from sqlalchemy.sql.expression import cast, literal
from datetime import timedelta, datetime

from prediction_commons.utils.enums import WindowHalfDirection, OutcomeLabelCategory
from .config import LABELS, ChurnFeatureColumns
from typing import Dict
import os
from .db_utils import UserIdHandler
from prediction_commons.utils.db_utils import get_sqla_table, get_sqlalchemy_tables_w_session
from prediction_commons.utils.bigquery import FeatureBuilder, create_rolling_agg_function, ColumnJsonDumper, \
    DataDownloader
from prediction_commons.utils.config import PROFILE_COLUMNS, sanitize_column_name, EXPECTED_DEVICE_TYPES


class ChurnDataDownloader(DataDownloader):
    LABELS = LABELS

    def __init__(self, start_time: datetime, end_time: datetime, moving_window_length, model_record_level: str,
                 historically_oversampled_outcome_type: OutcomeLabelCategory = None):
        super().__init__(start_time, end_time, moving_window_length, model_record_level,
                         historically_oversampled_outcome_type)

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


class ChurnColumnJsonDumper(ColumnJsonDumper):
    def __init__(self, full_query, features_in_data, features_expected):
        super().__init__(full_query, features_in_data, features_expected)
        self.feature_type_to_column_mapping.update(
            {'device_based_columns': self.features_expected.device_based_features}
        )
        self.column_defaults.update(
            {'device': '0.0'}
        )


class ChurnFeatureBuilder(FeatureBuilder):
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
    feature_columns = ChurnFeatureColumns
    labels = LABELS
    column_dumper = ChurnColumnJsonDumper

    def __init__(self, aggregation_time: datetime, moving_window_length: int,
                 feature_aggregation_functions: Dict[str, func]):
        super().__init__(
            aggregation_time,
            moving_window_length,
            feature_aggregation_functions,
            model_record_level='user'
        )

    def positive_label(self):
        return next(iter([label for label, label_type in self.labels.items() if label_type == 'positive']))

    def negative_label(self):
        return next(iter([label for label, label_type in self.labels.items() if label_type == 'negative']))

    def add_outcomes(
            self,
            feature_query,
    ):
        # The events table holds all the events, not just conversion ones
        relevant_events = self.bq_session.query(
            self.events.c['time'].cast(DATE).label('date'),
            self.events.c['type'].label('outcome'),
            self.events.c['user_id'].label('user_id')
        ).filter(
            self.events.c['type'].in_(
                list(LABELS.keys())
            ),
            cast(self.events.c['time'], DATE) == cast(self.aggregation_time, DATE)
        ).subquery()

        # TODO: Remove deduplication, once the event table doesn't contain any
        relevant_events_deduplicated = self.bq_session.query(
            relevant_events.c['date'],
            relevant_events.c['user_id'],
            # This case when provides logic for dealing with multiple outcomes during the same time period
            # an example is user_id 195379 during the 4/2020 where the user renews, but then cancels and gets
            # a refund (the current pipeline provides both labels)
            case(
                [
                    # If there is at least one churn event, we identify the user as churned
                    (
                        literal(self.negative_label()).in_(
                            func.unnest(
                                func.array_agg(relevant_events.c['outcome']
                                               )
                            )
                        ), self.negative_label())
                ],
                # In case of any number of any positive only events we consider the event as a renewal
                else_=self.positive_label()
            ).label('outcome')
        ).group_by(
            relevant_events.c['date'].label('date'),
            relevant_events.c['user_id'].label('user_id')
        ).subquery()

        feature_query_w_outcome = self.bq_session.query(
            feature_query,
            relevant_events_deduplicated.c['outcome'].label('outcome'),
            relevant_events_deduplicated.c['date'].label('outcome_date')
        ).outerjoin(
            relevant_events_deduplicated,
            and_(
                feature_query.c['user_id'] == relevant_events_deduplicated.c['user_id'],
                feature_query.c['date'] == relevant_events_deduplicated.c['date']
            )
        ).subquery('feature_query_w_outcome')

        return feature_query_w_outcome

    def join_all_partial_queries(
            self,
            filtered_data_with_profile_fields,
            all_date_user_combinations,
            device_information,
            profile_column_names
    ):
        joined_queries = self.bq_session.query(
            all_date_user_combinations.c[f'{self.model_record_level}_id'].label(f'{self.model_record_level}_id'),
            all_date_user_combinations.c['date_gap_filler'].label('date'),
            func.extract(text('DAYOFWEEK'), all_date_user_combinations.c['date_gap_filler']).cast(String).label(
                'day_of_week'),
            filtered_data_with_profile_fields.c['date'].label('date_w_gaps'),
            (filtered_data_with_profile_fields.c['pageviews'] > 0.0).label('is_active_on_date'),
            filtered_data_with_profile_fields.c['pageviews'].label('pageviews'),
            filtered_data_with_profile_fields.c['timespent'].label('timespent'),
            filtered_data_with_profile_fields.c['sessions_without_ref'].label('sessions_without_ref'),
            filtered_data_with_profile_fields.c['sessions'].label('sessions'),
            # Add all columns created from json_fields
            *[filtered_data_with_profile_fields.c[profile_column].label(profile_column) for profile_column in
              profile_column_names],
            # Unpack all device information columns except ones already present in other queries
            *[device_information.c[column.name] for column in device_information.columns if
              column.name not in [f'{self.model_record_level}_id', 'date']]
        ).outerjoin(
            filtered_data_with_profile_fields,
            and_(
                all_date_user_combinations.c[f'{self.model_record_level}_id'] == filtered_data_with_profile_fields.c[
                    f'{self.model_record_level}_id'
                ],
                all_date_user_combinations.c['date_gap_filler'] == filtered_data_with_profile_fields.c['date'])
        ).outerjoin(
            device_information,
            and_(
                device_information.c[f'{self.model_record_level}_id'] == all_date_user_combinations.c[
                    f'{self.model_record_level}_id'
                ],
                device_information.c['date'] == all_date_user_combinations.c['date_gap_filler']
            )
        ).subquery('joined_data')

        return joined_queries

    def filter_by_date(
            self
    ):
        filtered_data = self.bq_session.query(
            self.aggregated_user_days.c['date'].label('date'),
            self.aggregated_user_days.c['user_id'].label('user_id'),
            self.aggregated_user_days.c['pageviews'].label('pageviews'),
            self.aggregated_user_days.c['timespent'].label('timespent'),
            self.aggregated_user_days.c['sessions'].label('sessions'),
            self.aggregated_user_days.c['sessions_without_ref'].label('sessions_without_ref'),
            self.aggregated_user_days.c['pageviews_0h_4h'].label('pvs_0h_4h'),
            self.aggregated_user_days.c['pageviews_4h_8h'].label('pvs_4h_8h'),
            self.aggregated_user_days.c['pageviews_8h_12h'].label('pvs_8h_12h'),
            self.aggregated_user_days.c['pageviews_12h_16h'].label('pvs_12h_16h'),
            self.aggregated_user_days.c['pageviews_16h_20h'].label('pvs_16h_20h'),
            self.aggregated_user_days.c['pageviews_20h_24h'].label('pvs_20h_24h')
        ).subquery()

        current_data = self.bq_session.query(
            *[filtered_data.c[column.name].label(column.name) for column in
              filtered_data.columns]
        ).filter(
            filtered_data.c['date'] >= cast(self.aggregation_time - timedelta(days=self.moving_window_length), DATE),
            filtered_data.c['date'] <= cast(self.aggregation_time, DATE)
        ).subquery()

        user_id_handler = UserIdHandler(
            self.aggregation_time
        )
        user_id_handler.upload_user_ids()

        database = os.getenv('BIGQUERY_PROJECT_ID')
        schema = os.getenv('BIGQUERY_DATASET')

        user_id_table = get_sqla_table(
            table_name=f'{database}.{schema}.user_ids_filter', engine=self.bq_engine,
        )

        filtered_data = self.bq_session.query(current_data).join(
            user_id_table,
            current_data.c['user_id'] == user_id_table.c['user_id'].cast(String)
        ).subquery('filtered_data')

        return filtered_data

    def get_device_information_subquery(
            self,
    ):
        prominent_device_brands_past_90_days = self.get_prominent_device_list()

        database = os.getenv('BIGQUERY_PROJECT_ID')
        schema = os.getenv('BIGQUERY_DATASET')

        user_id_table = get_sqla_table(
            table_name=f'{database}.{schema}.user_ids_filter', engine=self.bq_engine,
        )

        device_features = self.bq_session.query(
            *[
                func.sum(
                    case(
                        [(self.browsers.c[f'is_{device}'] == 't', 1.0)],
                        else_=0.0
                    )
                ).label(f'{device}_device')
                for device in EXPECTED_DEVICE_TYPES
            ],
            *[
                func.sum(
                    case(
                        [(func.lower(self.browsers.c['device_brand']) == device_brand, 1.0)],
                        else_=0.0
                    )
                ).label(f'{sanitize_column_name(device_brand)}_device_brand')
                for device_brand in prominent_device_brands_past_90_days
            ],
            self.browser_users.c['user_id'].label('user_id'),
            self.browser_users.c['date'].label('date')
        ).join(
            self.browser_users,
            self.browsers.c['browser_id'] == self.browser_users.c['browser_id'],
        ).join(
            user_id_table,
            self.browser_users.c['user_id'] == user_id_table.c['user_id'].cast(String),
        ).filter(
            self.browsers.c['date'] >= cast(self.aggregation_time - timedelta(days=self.moving_window_length), DATE),
            self.browsers.c['date'] <= cast(self.aggregation_time, DATE),
        ).group_by(
            self.browser_users.c['user_id'].label('user_id'),
            self.browser_users.c['date'].label('date')
        ).subquery('device_information')

        return device_features

    def create_rolling_window_columns_config(
            self,
            joined_queries,
            profile_column_names,
    ):
        # {name of the resulting column : source / calculation},
        column_source_to_name_mapping = {
            'pageviews': joined_queries.c['pageviews'],
            'timespent': joined_queries.c['timespent'],
            'direct_visit': joined_queries.c['sessions_without_ref'],
            'visit': joined_queries.c['sessions'],
            # All json key columns have their own rolling sums
            **{
                column: joined_queries.c[column] for column in profile_column_names
            }
        }

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
                joined_queries.c['user_id'],
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
                joined_queries.c['user_id'],
                joined_queries.c['date'],
                half_window_direction
            ).cast(Float).label(f'days_active_{suffix}')
            for suffix, half_window_direction in get_rolling_agg_window_variants('count').items()
        ]

        return rolling_agg_columns
