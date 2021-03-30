from sqlalchemy.types import DATE, String
from sqlalchemy import and_, func, case, Float, text
from sqlalchemy.sql.expression import cast, literal
from pybigquery.sqlalchemy_bigquery import INTEGER
from datetime import timedelta, datetime

from prediction_commons.utils.enums import WindowHalfDirection, OutcomeLabelCategory
from .config import LABELS, ChurnFeatureColumns, EVENT_LOOKAHEAD
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


def upload_sub_data(
        max_date: datetime = datetime.utcnow(),
        min_date: datetime = None,
):
    from churn_prediction.utils.mysql import get_subscription_data
    data = get_subscription_data(max_date, min_date)
    from google.oauth2 import service_account
    client_secrets_path = f"../../{os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')}"
    credentials = service_account.Credentials.from_service_account_file(
        client_secrets_path,
    )

    data.to_gbq(
        destination_table=f'{os.getenv("BIGQUERY_DATASET")}.sub_data',
        project_id=os.getenv('BIGQUERY_PROJECT_ID'),
        credentials=credentials,
        if_exists='append'
    )


def determine_sub_table_min_date():
    client_secrets_path = os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')
    database = os.getenv('BIGQUERY_PROJECT_ID')
    from prediction_commons.utils.db_utils import create_connection
    engine, connection = create_connection(
        f'bigquery://{database}',
        engine_kwargs={'credentials_path': f'../../{client_secrets_path}'}
    )

    if not engine.has_table('sub_data', os.getenv('BIGQUERY_DATASET')):
        return None
    else:
        result = engine.execute(
            f"SELECT MAX(created_at) FROM {os.getenv('BIGQUERY_DATASET')}.sub_data"
        )

    return result.fetchall()[0][0]


def get_user_level_subscription_features(
        max_date: datetime = datetime.utcnow()
):
    bq_mappings, bq_engine = get_sqlalchemy_tables_w_session(
        table_names=['sub_data']
    )

    sub_data = bq_mappings['sub_data']
    bq_session = bq_mappings['session']

    sub_desc_column_names = ["length", "is_paid", "sub_print_access", "sub_print_friday_access"]
    sub_desc_columns = [
        sub_data.c[column].label(column)
        for column in sub_desc_column_names
    ]
    sub_web_access = case(
        [
            (sub_data.c['sub_web_access'] != 1, -1),
        ],
        else_= sub_data.c['sub_web_access'].cast(INTEGER) + sub_data.c['sub_standard_access'].cast(INTEGER) + sub_data.c['sub_club_access'].cast(INTEGER)
    ).label('web_access_level')

    sub_prices = bq_session.query(
        func.avg(
            (sub_data.c["amount"] - sub_data.c["additional_amount"]) / sub_data.c['length']
        ).label('average_price'),
        sub_web_access,
        *sub_desc_columns
    ).group_by(
        sub_web_access,
        *sub_desc_columns
    ).subquery()

    sub_data = bq_session.query(
        *[column.label(column.name) for column in sub_data.columns],
        sub_web_access,
        func.lag(sub_data.c['end_time']).over(
            partition_by=sub_data.c['user_id'],
            order_by=sub_data.c['start_time']
        ).label('previous_sub_end_time'),
        ((sub_data.c['amount'] - sub_data.c['additional_amount']) / sub_data.c['length']).label('daily_price')
    ).subquery()

    user_stats_query = bq_session.query(
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
        sub_data.c['start_time'].cast(DATE) <= max_date.date()
    ).join(
        sub_prices,
        and_(
            sub_data.c['web_access_level'] == sub_prices.c['web_access_level'],
            *[
                sub_data.c[column] == sub_prices.c[column] for column in sub_desc_column_names
            ]
        )
    ).group_by(
        sub_data.c['user_id']
    )
    result = pd.read_sql(user_stats_query.statement, user_stats_query.session.bind)
    print(result.head())
    # result = pd.read_sql(query.statement, query.session.bind)

# def get_subscription_based_features():



# import time
from dotenv import load_dotenv
load_dotenv('../.env')
# max_date_table = determine_sub_table_min_date()
# start_time = time.time()
# upload_sub_data(
#     min_date=max_date_table,
#     max_date=datetime.utcnow()
# )
# print((time.time() - start_time) / 60)
test = get_user_level_subscription_features()
