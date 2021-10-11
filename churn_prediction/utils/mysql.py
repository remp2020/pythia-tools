import pandas as pd
from sqlalchemy.types import DATE
from sqlalchemy import and_, func
from datetime import datetime
from sqlalchemy import MetaData, Table

from churn_prediction.utils.config import EVENT_LOOKAHEAD
from prediction_commons.utils.db_utils import create_connection
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
    engine, db_connection = create_connection(os.getenv(db_connection_string_name))

    for table in table_names:
        table_mapping[table] = get_sqla_table(table_name=table, engine=db_connection, schema=os.getenv(schema))

    table_mapping['session'] = sessionmaker(bind=db_connection)()

    return table_mapping


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
        payments.c['status'].in_(['paid', 'prepaid', 'family', 'upgrade'])
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


def get_users_with_expirations(
        aggregation_date: datetime.date = datetime.utcnow().date()
) -> pd.DataFrame:
    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CRM_CONNECTION_STRING',
        'MYSQL_CRM_DB',
        ['payments', 'subscriptions']
    )

    mysql_predplatne_session = predplatne_mysql_mappings['session']
    payments = predplatne_mysql_mappings['payments']
    subscriptions = predplatne_mysql_mappings['subscriptions']
    relevant_subscriptions = mysql_predplatne_session.query(
        subscriptions.c['user_id'],
        subscriptions.c['end_time'],
        subscriptions.c['start_time'],
        subscriptions.c['user_id']
    ).join(
        payments,
        payments.c['subscription_id'] == subscriptions.c['id']
    ).filter(
        and_(
            payments.c['status'].in_(['paid', 'prepaid', 'family', 'upgrade']),
            func.datediff(subscriptions.c['end_time'], aggregation_date) <= EVENT_LOOKAHEAD,
            func.datediff(subscriptions.c['end_time'], aggregation_date) > 0,
        )
    ).subquery('relevant_users')

    # We will be filtering out users that renewed before
    relevant_users = mysql_predplatne_session.query(
        relevant_subscriptions.c['user_id'],
        func.max(relevant_subscriptions.c['end_time']).cast(DATE).label('outcome_date')
    ).outerjoin(
        subscriptions,
        and_(
            subscriptions.c['user_id'] == relevant_subscriptions.c['user_id'],
            subscriptions.c['start_time'] > relevant_subscriptions.c['start_time'],
            subscriptions.c['start_time'] <= relevant_subscriptions.c['end_time'],
            subscriptions.c['start_time'] <= aggregation_date
        )
    ).filter(
        subscriptions.c['user_id'] == None
    ).group_by(
        relevant_subscriptions.c['user_id']
    )

    relevant_users = pd.read_sql(
        relevant_users.statement,
        relevant_users.session.bind
    )

    mysql_predplatne_session.close()

    return relevant_users


def get_table_created_at_filters(
    table,
    max_time: datetime,
    min_time: datetime = None
) -> List:
    filters = [table.c['created_at'].cast(DATE) <= max_time.date()]
    if min_time:
        filters.append(table.c['created_at'].cast(DATE) > min_time.date())
    return filters


def get_subscription_data(
    max_time: datetime,
    min_time: datetime = None
) -> pd.DataFrame:
    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CRM_CONNECTION_STRING',
        'MYSQL_CRM_DB',
        ['payments', 'subscriptions', 'subscription_types', 'subscription_upgrades']
    )

    mysql_predplatne_session = predplatne_mysql_mappings['session']
    payments = predplatne_mysql_mappings['payments']
    subscriptions = predplatne_mysql_mappings['subscriptions']
    subscription_types = predplatne_mysql_mappings['subscription_types']
    subscription_upgrades = predplatne_mysql_mappings['subscription_upgrades']

    subscription_id_access_level = mysql_predplatne_session.query(
        subscription_types.c['id'].label("subscription_type_id"),
        subscription_types.c['name'].label("name"),
        subscription_types.c['code'].label("code"),
        subscription_types.c['length'].label("length"),
        subscription_types.c['price'].label("price"),
        subscription_types.c['web'].label("is_web"),
        subscription_types.c['mobile'].label("is_mobile"),
        subscription_types.c['club'].label("is_club"),
        subscription_types.c['print'].label("is_print"),
        subscription_types.c['print_friday'].label("is_print_friday")
    ).filter(
        subscription_types.c['no_subscription'] != True
    ).subquery()

    payments_grouped_filtered = mysql_predplatne_session.query(
        payments.c['subscription_id'].label("subscription_id"),
        func.count(payments.c['id']).label("payment_count"),
        func.sum(payments.c['amount']).label("amount"),
        func.sum(payments.c['additional_amount']).label("additional_amount"),
        func.if_(func.sum(payments.c['recurrent_charge']) > 0, True, False).label("is_recurrent_charge"),
        func.group_concat(payments.c['status']).label("payment_status")
    ).filter(
        and_(
            payments.c['subscription_id'].isnot(None),
            *get_table_created_at_filters(payments, max_time, min_time)
        )
    ).group_by(
        payments.c['subscription_id']
    ).subquery()

    subscriptions_upgraded_subs = mysql_predplatne_session.query(
        subscription_upgrades.c['base_subscription_id'].label("base_subscription_id"),
        func.if_(func.count(subscription_upgrades.c['id']) > 0, True, False).label("is_upgraded")
    ).group_by(
        subscription_upgrades.c['base_subscription_id']
    ).filter(
        *get_table_created_at_filters(subscription_upgrades, max_time, min_time)
    ).subquery()

    subscriptions_upgrades = mysql_predplatne_session.query(
        subscription_upgrades.c['base_subscription_id'].label("base_subscription_id"),
        subscription_upgrades.c['upgraded_subscription_id'].label("upgraded_subscription_id"),
        subscription_upgrades.c['type'].label("upgrade_type"),
        func.if_(func.count(subscription_upgrades.c['id']) > 0, True, False).label("is_upgrade")
    ).filter(
        and_(
            *get_table_created_at_filters(subscription_upgrades, max_time, min_time)
        )
    ).group_by(
        subscription_upgrades.c['base_subscription_id'],
        subscription_upgrades.c['upgraded_subscription_id'],
        subscription_upgrades.c['type']
    ).subquery()

    subscriptions_data = mysql_predplatne_session.query(
        subscriptions.c['created_at'].label('created_at'),
        subscriptions.c['user_id'].label("user_id"),
        subscriptions.c['id'].label("subscription_id"),
        subscriptions.c['start_time'].label("start_time"),
        subscriptions.c['end_time'].label("end_time"),
        subscriptions.c['length'].label("length"),
        subscriptions.c['is_recurrent'].label("is_recurrent"),
        subscriptions.c['is_paid'].label("is_paid"),
        subscriptions.c['type'].label("subscription_type"),
        subscriptions.c['subscription_type_id'].label("subscription_type_id"),
        payments_grouped_filtered.c['payment_count'].label("payment_count"),
        payments_grouped_filtered.c['amount'].label("amount"),
        payments_grouped_filtered.c['additional_amount'].label("additional_amount"),
        payments_grouped_filtered.c['is_recurrent_charge'].label("is_recurrent_charge"),
        payments_grouped_filtered.c['payment_status'].label("payment_status"),
        subscription_id_access_level.c['name'].label("sub_type_name"),
        subscription_id_access_level.c['code'].label("sub_type_code"),
        subscription_id_access_level.c['length'].label("sub_type_length"),
        subscription_id_access_level.c['price'].label("sub_type_price"),
        subscription_id_access_level.c['is_web'].label("sub_web_access"),
        subscription_id_access_level.c['is_mobile'].label("sub_standard_access"),
        subscription_id_access_level.c['is_club'].label("sub_club_access"),
        subscription_id_access_level.c['is_print'].label("sub_print_access"),
        subscription_id_access_level.c['is_print_friday'].label("sub_print_friday_access"),
        subscriptions_upgraded_subs.c['is_upgraded'].label("is_upgraded"),
        subscriptions_upgrades.c['base_subscription_id'].label("base_subscription_id"),
        subscriptions_upgrades.c['upgrade_type'].label("upgrade_type"),
        subscriptions_upgrades.c['is_upgrade'].label("is_upgrade"),
    ).join(
        payments_grouped_filtered,
        subscriptions.c['id'] == payments_grouped_filtered.c['subscription_id'],
        isouter=True
    ).join(
        subscription_id_access_level,
        subscriptions.c['subscription_type_id'] == subscription_id_access_level.c['subscription_type_id'],
        isouter=True
    ).join(
        subscriptions_upgraded_subs,
        subscriptions.c['id'] == subscriptions_upgraded_subs.c['base_subscription_id'],
        isouter=True
    ).join(
        subscriptions_upgrades,
        subscriptions.c['id'] == subscriptions_upgrades.c['upgraded_subscription_id'],
        isouter=True
    ).filter(
        and_(
            *get_table_created_at_filters(subscriptions, max_time, min_time),
            payments_grouped_filtered.c['payment_status'].in_(['paid', 'prepaid', 'family', 'upgrade'])
        )
    ).order_by(
        subscriptions.c['user_id'],
        subscriptions.c['start_time']
    )

    subscriptions_data_merged = pd.read_sql(subscriptions_data.statement, subscriptions_data.session.bind)

    subscriptions_data_merged = subscriptions_data_merged.loc[subscriptions_data_merged["start_time"] != subscriptions_data_merged["end_time"]].copy()
    subscriptions_data_merged = subscriptions_data_merged.loc[~subscriptions_data_merged["payment_status"].str.contains("form|refund", na=False)].copy()
    subscriptions_data_merged = subscriptions_data_merged.loc[subscriptions_data_merged["start_time"] >= datetime(2015,1,1,0,0,0)].copy()

    column_fillna = {'payment_count': 0,
                     'amount': 0.0,
                     'additional_amount': 0.0,
                     'is_recurrent_charge': False,
                     'payment_status': 'no_payment',
                     'is_upgraded': False,
                     'base_subscription_id': subscriptions_data_merged['subscription_id'],
                     'upgrade_type': 'none',
                     'is_upgrade': False,
                     }

    subscriptions_data_merged.fillna(column_fillna, inplace=True)

    column_types = {'user_id': int,
                    'subscription_id': int,
                    'length': int,
                    'is_recurrent': float,
                    'is_paid': float,
                    'subscription_type': str,
                    'subscription_type_id': int,
                    'payment_count': int,
                    'amount': float,
                    'additional_amount': float,
                    'is_recurrent_charge': float,
                    'payment_status': str,
                    'sub_type_name': str,
                    'sub_type_code': str,
                    'sub_type_length': int,
                    'sub_type_price': float,
                    'sub_web_access': float,
                    'sub_standard_access': float,
                    'sub_club_access': float,
                    'sub_print_access': float,
                    'sub_print_friday_access': float,
                    'is_upgraded': float,
                    'base_subscription_id': int,
                    'upgrade_type': str,
                    'is_upgrade': float
                    }

    subscriptions_data_merged = subscriptions_data_merged.astype(column_types)

    mysql_predplatne_session.close()

    return subscriptions_data_merged
