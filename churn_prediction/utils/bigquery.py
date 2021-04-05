from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import DATE, String, INTEGER
from sqlalchemy import and_, func, case, Float, text, Table
from sqlalchemy.sql.expression import cast, literal
from pybigquery.sqlalchemy_bigquery import INTEGER
from google.oauth2 import service_account
from datetime import timedelta, datetime

from dotenv import load_dotenv
load_dotenv('../.env')

from prediction_commons.utils.enums import WindowHalfDirection, OutcomeLabelCategory
from .config import LABELS, ChurnFeatureColumns, EVENT_LOOKAHEAD
from typing import Dict
import os
from .db_utils import UserIdHandler
from prediction_commons.utils.db_utils import get_sqla_table, get_sqlalchemy_tables_w_session, create_connection
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
                        {','.join([column.name for column in table.columns if 'features' in column.name])},
                        RANK() OVER (PARTITION  BY user_id ORDER BY date DESC) AS date_rank
                    FROM
                        {os.getenv('BIGQUERY_DATASET')}.rolling_daily_{self.model_record_level}_profile
                    WHERE
                        window_days = @window_days
                        AND outcome_date <= @end_time
                        AND ((outcome_date >= @start_time){minority_label_filter})
                '''

        if not self.historically_oversampled_outcome_type:
            query = self.deduplicate_records(query, table)

        return query


class ChurnColumnJsonDumper(ColumnJsonDumper):
    def __init__(self, full_query, features_in_data, features_expected):
        super().__init__(full_query, features_in_data, features_expected)
        self.feature_type_to_column_mapping.update(
            {
                'device_based_columns': self.features_expected.device_based_features,
                'numeric_columns_subscriptions_base': self.features_expected.numeric_columns_subscriptions_base,
                'numeric_columns_subscriptions_derived': self.features_expected.numeric_columns_subscriptions_derived,
                'categorical_columns_subscriptions': self.features_expected.categorical_columns_subscriptions
            }
        )
        self.column_defaults.update(
            {'device': '0.0'}
        )


class SubscriptionFeatureBuilder:
    def __init__(
            self,
            max_date: datetime = datetime.utcnow(),
            min_date: datetime = None,
            engine: Engine = None,
            connection=None,
            session=None
    ):
        self.max_date = max_date
        self.min_date = min_date
        client_secrets_path = os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')
        self.credentials = service_account.Credentials.from_service_account_file(
            client_secrets_path,
        )

        self.engine = engine
        self.bq_session = session
        self.connection = connection

        # We can't initialize the table yet in case it doesn't exist
        self.sub_data = None

        self.sub_desc_column_names = ["length", "is_paid", "sub_print_access", "sub_print_friday_access"]

    def upload_sub_data(self):
        from churn_prediction.utils.mysql import get_subscription_data

        if not self.engine.dialect.has_table(self.engine, 'sub_data', os.getenv('BIGQUERY_DATASET')):
            self.min_date = None
        else:
            result = self.engine.execute(
                f"SELECT MAX(created_at) FROM {os.getenv('BIGQUERY_DATASET')}.sub_data"
            )

            self.min_date = result.fetchall()[0][0]

        if self.min_date is None or self.min_date.date() < self.max_date.date():
            data = get_subscription_data(self.max_date, self.min_date)

            data.to_gbq(
                destination_table=f'{os.getenv("BIGQUERY_DATASET")}.sub_data',
                project_id=os.getenv('BIGQUERY_PROJECT_ID'),
                credentials=self.credentials,
                if_exists='append'
            )

        database = os.getenv('BIGQUERY_PROJECT_ID')
        schema = os.getenv('BIGQUERY_DATASET')
        self.sub_data = get_sqla_table(
            table_name=f'{database}.{schema}.sub_data', engine=self.engine,

        )

    def sub_web_access(self):

        return case(
            [
                (self.sub_data.c['sub_web_access'] != 1, -1),
            ],
            else_=(
                    self.sub_data.c['sub_web_access'].cast(INTEGER) +
                    self.sub_data.c['sub_standard_access'].cast(INTEGER) +
                    self.sub_data.c['sub_club_access'].cast(INTEGER)
            )
        ).label('web_access_level')

    def sub_prices_query(self):
        sub_desc_columns = [
            self.sub_data.c[column].label(column)
            for column in self.sub_desc_column_names
        ]

        sub_web_access = self.sub_web_access()

        sub_prices = self.bq_session.query(
            func.avg(
                func.safe_divide(
                    (self.sub_data.c["amount"] - self.sub_data.c["additional_amount"]),
                    self.sub_data.c['length']
                )
            ).label('average_price'),
            sub_web_access,
            *sub_desc_columns
        ).group_by(
            sub_web_access,
            *sub_desc_columns
        ).subquery()

        return sub_prices

    def get_user_level_subscription_features(self):
        sub_data = self.bq_session.query(
            *[column.label(column.name) for column in self.sub_data.columns],
            self.sub_web_access(),
            func.lag(self.sub_data.c['end_time']).over(
                partition_by=self.sub_data.c['user_id'],
                order_by=self.sub_data.c['start_time']
            ).label('previous_sub_end_time'),
            (func.safe_divide(
                (self.sub_data.c['amount'] - self.sub_data.c['additional_amount']),
                self.sub_data.c['length']
            )).label('daily_price')
        ).subquery()

        sub_prices = self.sub_prices_query()

        user_stats_query = self.bq_session.query(
            sub_data.c['user_id'].label('user_id'),
            func.sum(sub_data.c['amount']).label('total_amount'),
            func.avg(sub_data.c['amount']).label('average_amount'),
            func.count(func.distinct(sub_data.c['subscription_id'])).label('count_subs'),
            func.sum(
                case(
                    [
                        (sub_data.c['is_paid'] == 1, 1.0)
                    ],
                    else_=0.0
                )
            ).label('paid_subs'),
            func.sum(
                case(
                    [
                        (sub_data.c['is_paid'] == 0, 1.0)
                    ],
                    else_=0.0
                )
            ).label('free_subs'),
            func.date_diff(
                func.max(sub_data.c['created_at']).cast(DATE),
                func.min(sub_data.c['created_at']).cast(DATE),
                text('day')
            ).label('number_of_days_since_the_first_paid_sub'),
            func.avg(
                func.date_diff(
                    sub_data.c['start_time'].cast(DATE),
                    sub_data.c['previous_sub_end_time'].cast(DATE),
                    text('day')
                )
            ).label('average_gap'),
            func.max(
                func.date_diff(
                    sub_data.c['start_time'].cast(DATE),
                    sub_data.c['previous_sub_end_time'].cast(DATE),
                    text('day')
                )
            ).label('max_gap'),
            func.sum(
                case(
                    [
                        (
                            sub_data.c['is_paid'] == 1, sub_data.c['length'])
                    ],
                    else_=0.0
                )
            ).label('total_days_paid_sub'),
            func.sum(
                case(
                    [
                        (sub_data.c['is_paid'] == 0, sub_data.c['length'])
                    ],
                    else_=0.0
                )
            ).label('total_days_free_sub'),
            func.sum(
                case(
                    [
                        (sub_data.c['is_upgraded'] == 1, 1.0)
                    ],
                    else_=0.0
                )
            ).label('upgraded_subs'),
            func.sum(
                case(
                    [
                        (sub_prices.c['average_price'] == 0, 0.0),
                        (
                            1 - sub_data.c['daily_price'] / sub_prices.c['average_price'] >= 0.2,
                            1.0
                        )
                    ],
                    else_=0.0
                )
            ).label('discount_subs')
        ).filter(
            sub_data.c['start_time'].cast(DATE) <= self.max_date.date()
        ).join(
            sub_prices,
            and_(
                sub_data.c['web_access_level'] == sub_prices.c['web_access_level'],
                *[
                    sub_data.c[column] == sub_prices.c[column] for column in self.sub_desc_column_names
                ]
            )
        ).group_by(
            sub_data.c['user_id']
        ).subquery()

        return user_stats_query

    def get_k_to_last_subscriptions(
            self,
            # Which set of subscriptions we want, first to last (= the last one), second to last (penultimate),
            # third to last ...
            k: int = 1
    ):
        k_to_Last_subscriptions_query = self.bq_session.query(
            *[column.label(column.name) for column in self.sub_data.columns],
            func.rank().over(
                partition_by=self.sub_data.c['user_id'],
                order_by=self.sub_data.c['start_time']
            ).label('reverse_order_rank'),
            self.sub_web_access(),
            func.safe_divide(
                (self.sub_data.c["amount"] - self.sub_data.c["additional_amount"]),
                self.sub_data.c['length']
            ).label('daily_price')
        ).filter(
            self.sub_data.c['start_time'].cast(DATE) <= self.max_date.date()
        ).subquery()

        k_to_Last_subscriptions_query = self.bq_session.query(
            *[column.label(column.name) for column in k_to_Last_subscriptions_query.columns]
        ).filter(
            k_to_Last_subscriptions_query.c['reverse_order_rank'] == k
        ).subquery()

        sub_prices_query = self.sub_prices_query()
        k_to_Last_subscriptions_query = self.bq_session.query(
            *[column.label(column.name) for column in k_to_Last_subscriptions_query.columns],
            case(
                [
                    (sub_prices_query.c['average_price'] == 0, 0.0),
                    (
                        1 - k_to_Last_subscriptions_query.c['daily_price'] /
                        sub_prices_query.c['average_price'] >= 0.2,
                        1.0
                    )
                ],
                else_=0.0
            ).label('is_discount')
        ).join(
            sub_prices_query,
            and_(
                k_to_Last_subscriptions_query.c['web_access_level'] == sub_prices_query.c[
                    'web_access_level'],
                *[
                    k_to_Last_subscriptions_query.c[column] == sub_prices_query.c[column] for column in
                    self.sub_desc_column_names
                ]
            )
        ).subquery()

        return k_to_Last_subscriptions_query

    def get_subscription_based_features(self):
        last_subscriptions_query = self.get_k_to_last_subscriptions(1)
        sub_prices = self.sub_prices_query()
        last_subscriptions_query = self.bq_session.query(
            *[column.label(column.name) for column in last_subscriptions_query.columns]
        ).join(
            sub_prices,
            and_(
                last_subscriptions_query.c['web_access_level'] == sub_prices.c['web_access_level'],
                *[
                    last_subscriptions_query.c[column] == sub_prices.c[column] for column in self.sub_desc_column_names
                ]
            )

        ).subquery()

        second_to_last_subscriptions_query = self.get_k_to_last_subscriptions(2)
        second_to_last_subscriptions_query = self.bq_session.query(
            *[column.label(column.name) for column in second_to_last_subscriptions_query.columns]
        ).join(
            sub_prices,
            and_(
                second_to_last_subscriptions_query.c['web_access_level'] == sub_prices.c['web_access_level'],
                *[
                    second_to_last_subscriptions_query.c[column] == sub_prices.c[column]
                    for column in self.sub_desc_column_names
                ]
            )

        ).subquery()

        last_and_previous_subscription = self.bq_session.query(
            *[
                column.label(f'{column.name}_last') for column in last_subscriptions_query.columns
                if column.name != 'user_id'
            ],
            *[
                column.label(f'{column.name}_previous') for column in second_to_last_subscriptions_query.columns
                if column.name != 'user_id'
            ],
            last_subscriptions_query.c['user_id'].label('user_id')
        ).join(
            second_to_last_subscriptions_query,
            second_to_last_subscriptions_query.c['user_id'] == last_subscriptions_query.c['user_id']
        ).subquery()

        numeric_columns = ['length', 'amount']

        categorical_columns = ['is_recurrent', 'is_recurrent_charge', 'web_access_level', 'sub_print_access',
                               'sub_print_friday_access', 'is_discount']

        last_and_previous_subscription = self.bq_session.query(
            *[column.label(column.name) for column in last_and_previous_subscription.columns],
            # Diff for numeric columns
            *[
                case(
                    [
                        # If the 2nd half of period has 0 for its value, we assign 100 % decline
                        (last_and_previous_subscription.c[f'{column}_last'] == 0, -1),
                        # If the 1st half of period has 0 for its value, we assign 100 % growth
                        (
                            last_and_previous_subscription.c[
                                f'{column}_previous'] == 0, 1)
                    ],
                    else_=(
                        last_and_previous_subscription.c[f'{column}_last'] /
                        last_and_previous_subscription.c[f'{column}_previous']
                    )
                ).label(f'{column}_diff')
                for column in numeric_columns
            ],
            # Diff for categorical columns
            *[
                (
                    last_and_previous_subscription.c[f'{column}_last'] -
                    last_and_previous_subscription.c[f'{column}_previous']
                ).label(f'{column}_diff')
                for column in categorical_columns
             ]
        ).subquery()

        return last_and_previous_subscription

    def merge_all_subscription_related_features(self):
        user_level_features_query = self.get_user_level_subscription_features()
        subscription_level_features_query = self.get_subscription_based_features()

        merged_features_query = self.bq_session.query(
            *[column.label(column.name) for column in user_level_features_query.columns if column.name != 'user_id'],
            *[column.label(column.name) for column in subscription_level_features_query.columns
              if column.name != 'user_id'],
            user_level_features_query.c['user_id'].cast(String).label('user_id')
        ).join(
            subscription_level_features_query,
            subscription_level_features_query.c['user_id'] == user_level_features_query.c['user_id']
        ).subquery()

        database = os.getenv('BIGQUERY_PROJECT_ID')
        schema = os.getenv('BIGQUERY_DATASET')

        user_id_table = get_sqla_table(
            table_name=f'{database}.{schema}.user_ids_filter', engine=self.engine,
        )

        merged_features_filtered_query = self.bq_session.query(
            merged_features_query
        ).join(
            user_id_table,
            merged_features_query.c['user_id'] == user_id_table.c['user_id'].cast(String)
        ).subquery('filtered_subscription_data')

        return merged_features_filtered_query


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
            and_(
                self.events.c['type'].in_(
                    list(LABELS.keys())
                ),
                cast(self.events.c['time'], DATE) > cast(self.aggregation_time, DATE),
                cast(self.events.c['time'], DATE) <= cast(self.aggregation_time + timedelta(days=EVENT_LOOKAHEAD), DATE)
            )
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
            *[column.label(column.name) for column in feature_query.columns if column.name not in ['user_id', 'date']],
            func.coalesce(feature_query.c['user_id'], relevant_events_deduplicated.c['user_id']).label('user_id'),
            func.coalesce(feature_query.c['date'], self.aggregation_time.date()).label('date'),
            relevant_events_deduplicated.c['outcome'].label('outcome'),
            relevant_events_deduplicated.c['date'].label('outcome_date')
        ).join(
            relevant_events_deduplicated,
            and_(
                feature_query.c['user_id'] == relevant_events_deduplicated.c['user_id'],
                func.date_diff(
                    relevant_events_deduplicated.c['date'],
                    feature_query.c['date'],
                    text('day')
                ) <= EVENT_LOOKAHEAD,
                func.date_diff(
                    relevant_events_deduplicated.c['date'],
                    feature_query.c['date'],
                    text('day')
                ) > 0,
            ),
            full=True
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

        filtered_data = self.bq_session.query(
            *[column.label(column.name) for column in current_data.columns]
        ).join(
            user_id_table,
            current_data.c['user_id'] == user_id_table.c['user_id'].cast(String),
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

    def get_full_features_query(self):
        full_query_behavioural = super().get_full_features_query()

        subscription_feature_builder = SubscriptionFeatureBuilder(
            max_date=self.aggregation_time,
            min_date=None,
            engine=self.bq_engine,
            connection=self.bq_engine.connect(),
            session=self.bq_session
        )

        subscription_feature_builder.upload_sub_data()

        full_query_transactional = subscription_feature_builder.merge_all_subscription_related_features()

        full_query = self.bq_session.query(
            *[column.label(column.name) for column in full_query_transactional.columns if column.name != 'date'],
            *[column.label(column.name) for column in full_query_behavioural.columns
              if column.name not in ('user_id', 'date')],
            func.coalesce(full_query_behavioural.c['date'], self.aggregation_time.date()).label('date')
        ).outerjoin(
            full_query_behavioural,
            full_query_behavioural.c['user_id'] == full_query_transactional.c['user_id']
        ).subquery()

        return full_query
