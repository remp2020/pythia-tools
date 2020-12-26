import os

from sqlalchemy.types import DATE
from sqlalchemy import and_, func, case, Float, String, text
from sqlalchemy.sql.expression import cast
from datetime import timedelta, datetime

from prediction_commons.utils.enums import WindowHalfDirection, OutcomeLabelCategory
from .config import LABELS, ConversionFeatureColumns
from typing import Dict
from prediction_commons.utils.db_utils import get_sqlalchemy_tables_w_session
from prediction_commons.utils.bigquery import FeatureBuilder, create_rolling_agg_function, DataDownloader
from prediction_commons.utils.config import PROFILE_COLUMNS


class ConversionDataDownloader(DataDownloader):
    LABELS = LABELS

    def __init__(self, start_time: datetime, end_time: datetime, moving_window_length, model_record_level: str,
                 historically_oversampled_outcome_type: OutcomeLabelCategory = None):
        super().__init__(start_time, end_time, moving_window_length, model_record_level,
                         historically_oversampled_outcome_type)

    def get_retrieval_query(self, table):
        minority_label_filter = self.get_minority_label_filter()

        query = f'''
            SELECT
                profiles.{self.model_record_level}_id AS {self.model_record_level}_id,
                user_ids,
                date,
                outcome_date,
                outcome,
                feature_aggregation_functions,
                {','.join([column.name for column in table.columns if 'features' in column.name])}
            FROM
                {os.getenv('BIGQUERY_DATASET')}.rolling_daily_{self.model_record_level}_profile profiles
            LEFT JOIN (
                SELECT 
                    browser_id, 
                    ARRAY_AGG(DISTINCT(user_id)) AS user_ids 
                FROM 
                    {os.getenv('BIGQUERY_DATASET')}.browser_users 
                WHERE 
                    date >= @start_time 
                    AND date <= @end_time 
                    GROUP BY browser_id
            ) user_ids_aggregated
            ON (profiles.browser_id = user_ids_aggregated.browser_id)
            WHERE
                window_days = @window_days
                AND outcome_date <= @end_time
                AND ((outcome_date >= @start_time){minority_label_filter})
        '''

        return query


class ConversionFeatureBuilder(FeatureBuilder):
    tables_to_map = (
            ['aggregated_browser_days', 'events', 'browsers'] +
            [f'aggregated_browser_days_{profile_feature_set_name}' for profile_feature_set_name in PROFILE_COLUMNS
             if profile_feature_set_name != 'hour_interval_pageviews']
    )

    bq_mappings, bq_engine = get_sqlalchemy_tables_w_session(
        table_names=tables_to_map
    )

    bq_session = bq_mappings['session']
    aggregated_browser_days = bq_mappings['aggregated_browser_days']
    events = bq_mappings['events']
    browsers = bq_mappings['browsers']
    feature_columns = ConversionFeatureColumns
    labels = LABELS

    def __init__(self, aggregation_time: datetime, moving_window_length: int,
                 feature_aggregation_functions: Dict[str, func]):
        super().__init__(
            aggregation_time,
            moving_window_length,
            feature_aggregation_functions,
            model_record_level='browser'
        )

    def add_outcomes(
            self,
            feature_query
    ):
        # The events table holds all the events, not just conversion ones
        relevant_events = self.bq_session.query(
            self.events.c['time'].cast(DATE).label('date'),
            self.events.c['type'].label('outcome'),
            self.events.c['browser_id'].label('browser_id')
        ).filter(
            self.events.c['type'].in_(
                list(LABELS.keys())
            )
        ).subquery()

        feature_query_w_outcome = self.bq_session.query(
            feature_query,
            case(
                [
                    (
                        relevant_events.c['outcome'].in_(self.positive_labels()),
                        relevant_events.c['date']
                    )
                ],
                else_=(self.aggregation_time + timedelta(days=1)).date()
            ).label('outcome_date'),
            case(
                [
                    (
                        relevant_events.c['outcome'].in_(self.positive_labels()),
                        relevant_events.c['outcome']
                    )
                ],
                else_=self.negative_label()
            ).label('outcome')
        ).outerjoin(
            relevant_events,
            and_(
                feature_query.c['browser_id'] == relevant_events.c['browser_id'],
                feature_query.c['date'] >= func.date_sub(
                    relevant_events.c['date'],
                    text(f'interval {1} day')
                )
            )
        ).subquery()

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
            filtered_data_with_profile_fields.c['commerce_checkouts'].label('commerce_checkouts'),
            filtered_data_with_profile_fields.c['commerce_payments'].label('commerce_payments'),
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

    def positive_labels(self):
        return [label for label, label_type in self.labels.items() if label_type == 'positive']

    def negative_label(self):
        return [label for label, label_type in self.labels.items() if label_type == 'negative'][0]

    def filter_by_date(
            self
    ):
        filtered_data = self.bq_session.query(
            self.aggregated_browser_days.c['date'].label('date'),
            self.aggregated_browser_days.c['browser_id'].label('browser_id'),
            self.aggregated_browser_days.c['user_ids'].label('user_ids'),
            self.aggregated_browser_days.c['pageviews'].label('pageviews'),
            self.aggregated_browser_days.c['timespent'].label('timespent'),
            self.aggregated_browser_days.c['sessions'].label('sessions'),
            self.aggregated_browser_days.c['sessions_without_ref'].label('sessions_without_ref'),
            self.aggregated_browser_days.c['pageviews_0h_4h'].label('pvs_0h_4h'),
            self.aggregated_browser_days.c['pageviews_4h_8h'].label('pvs_4h_8h'),
            self.aggregated_browser_days.c['pageviews_8h_12h'].label('pvs_8h_12h'),
            self.aggregated_browser_days.c['pageviews_12h_16h'].label('pvs_12h_16h'),
            self.aggregated_browser_days.c['pageviews_16h_20h'].label('pvs_16h_20h'),
            self.aggregated_browser_days.c['pageviews_20h_24h'].label('pvs_20h_24h'),
            self.aggregated_browser_days.c['commerce_checkouts'].label('commerce_checkouts'),
            self.aggregated_browser_days.c['commerce_payments'].label('commerce_payments'),
        ).filter(
            self.aggregated_browser_days.c['date'] >= cast(
                self.aggregation_time - timedelta(days=self.moving_window_length), DATE
            ),
            self.aggregated_browser_days.c['date'] <= cast(self.aggregation_time, DATE)
        ).subquery()

        return filtered_data

    def get_device_information_subquery(
            self
    ):
        device_information = self.bq_session.query(
            case(
                [
                    (self.browsers.c['device_brand'] == None,
                     'Desktop')
                ],
                else_=self.browsers.c['device_brand']
            ).label('device_brand').label('device'),
            self.browsers.c['browser_family'].label('browser'),
            self.browsers.c['is_desktop'],
            self.browsers.c['is_mobile'],
            self.browsers.c['is_tablet'],
            self.browsers.c['os_family'].label('os'),
            self.browsers.c['browser_id'],
            self.browsers.c['date']
        ).order_by(
            self.browsers.c['browser_id'],
            self.browsers.c['date'].desc()
        ).distinct(
            self.browsers.c['browser_id'],
        ).subquery('device_information')

        return device_information

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
            'commerce_checkouts': joined_queries.c['commerce_checkouts'],
            'commerce_payments': joined_queries.c['commerce_payments'],
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
                joined_queries.c['browser_id'],
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
                joined_queries.c['browser_id'],
                joined_queries.c['date'],
                half_window_direction
            ).cast(Float).label(f'days_active_{suffix}')
            for suffix, half_window_direction in get_rolling_agg_window_variants('count').items()
        ]

        return rolling_agg_columns
