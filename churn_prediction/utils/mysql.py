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
    )

    user_payment_history = pd.read_sql(
        clv.statement,
        clv.session.bind
    )

    user_payment_history['clv'] = user_payment_history['clv'].astype(float)
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


def get_users_with_expirations(
        aggregation_date: datetime.date = datetime.utcnow().date(),
        expiration_lookahead: int = 30
) -> List[str]:
    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CONNECTION_STRING',
        'predplatne',
        ['payments', 'subscriptions']
    )

    mysql_predplatne_session = predplatne_mysql_mappings['session']
    payments = predplatne_mysql_mappings['payments']
    subscriptions = predplatne_mysql_mappings['subscriptions']

    relevant_users = mysql_predplatne_session.query(
        subscriptions.c['user_id']
    ).join(
        payments,
        payments.c['subscription_id'] == subscriptions.c['id']
    ).filter(
        and_(
            payments.c['status'] == 'paid',
            func.datediff(subscriptions.c['end_time'], aggregation_date) <= expiration_lookahead,
            func.datediff(subscriptions.c['end_time'], aggregation_date) > 0,
        )
    ).group_by(
        subscriptions.c['user_id']
    )

    relevant_users = relevant_users.all()
    relevant_users = [user_id[0] for user_id in relevant_users]
    mysql_predplatne_session.close()

    return relevant_users
