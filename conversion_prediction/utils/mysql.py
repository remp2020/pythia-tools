import pandas as pd
from sqlalchemy.types import DATE
from sqlalchemy import and_, func
from datetime import datetime
from sqlalchemy import MetaData, Table
from .db_utils import create_connection
from sqlalchemy.orm import sessionmaker
from typing import List, Dict
import os


def get_sqla_table(table_name, engine, schema='public'):
    meta = MetaData()
    table = Table(table_name, meta, schema=schema, autoload=True,
                  autoload_with=engine)
    return table


def get_sqlalchemy_tables_w_session(db_connection_string_name: str, schema: str, table_names: List[str]) -> Dict:
    table_mapping = {}
    _, db_connection = create_connection(os.getenv(db_connection_string_name))

    for table in table_names:
        table_mapping[table] = get_sqla_table(table_name=table, engine=db_connection, schema=schema)

    table_mapping['session'] = sessionmaker(bind=db_connection)()

    return table_mapping


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
        'MYSQL_BEAM_CONNECTION_STRING',
        'MYSQL_BEAM_DB',
        ['article_pageviews']
    )
    mysql_beam_session = beam_mysql_mappings['session']
    article_pageviews = beam_mysql_mappings['article_pageviews']

    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CRM_CONNECTION_STRING',
        'MYSQL_CRM_DB',
        ['payments']
    )

    mysql_predplatne_session = predplatne_mysql_mappings['session']
    payments = predplatne_mysql_mappings['payments']

    # We create two subqueries using the same data to merge twice in order to get rolling sum in mysql

    payments_query = mysql_predplatne_session.query(
        payments.c['created_at'].cast(DATE).label('date'),
        func.count(payments.c['id']).label('payment_count'),
        func.sum(payments.c['amount']).label('sum_paid')
    ).filter(
        payments.c['created_at'].cast(DATE) >= start_time,
        payments.c['created_at'].cast(DATE) <= end_time,
        payments.c['status'] == 'paid'
    ).group_by(
        'date'
    )

    article_pageviews_query = mysql_beam_session.query(
        article_pageviews.c['time_from'].cast(DATE).label('date'),
        func.sum(article_pageviews.c['sum']).label('article_pageviews'),
    ).filter(
        article_pageviews.c['time_from'].cast(DATE) >= start_time,
        article_pageviews.c['time_from'].cast(DATE) <= end_time
    ).group_by(
        'date'
    )

    payments = pd.read_sql(
        payments_query.statement,
        payments_query.session.bind
    )

    article_pageviews = pd.read_sql(
        article_pageviews_query.statement,
        article_pageviews_query.session.bind
    )

    mysql_predplatne_session.close()
    mysql_beam_session.close()

    context = pd.merge(
        left=payments,
        right=article_pageviews,
        on=['date'],
        how='inner'
    )

    return context


