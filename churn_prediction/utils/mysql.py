import pandas as pd
from sqlalchemy.types import DATE
from sqlalchemy import and_, func, cast, or_, case
from datetime import datetime
from sqlalchemy import MetaData, Table

from churn_prediction.utils.config import EVENT_LOOKAHEAD
from prediction_commons.utils.db_utils import create_connection
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Dict
import os
import numpy as np


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


def get_payment_history_features(end_time: datetime):
    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CRM_CONNECTION_STRING',
        'MYSQL_CRM_DB',
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


def get_users_with_expirations(
        aggregation_date: datetime.date = datetime.utcnow().date()
) -> List[str]:
    predplatne_mysql_mappings = get_sqlalchemy_tables_w_session(
        'MYSQL_CRM_CONNECTION_STRING',
        'MYSQL_CRM_DB',
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
            func.datediff(subscriptions.c['end_time'], aggregation_date) <= EVENT_LOOKAHEAD,
            func.datediff(subscriptions.c['end_time'], aggregation_date) > 0,
        )
    ).group_by(
        subscriptions.c['user_id']
    )

    relevant_users = relevant_users.all()
    relevant_users = [user_id[0] for user_id in relevant_users]
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

    relevant_users = get_users_with_expirations(max_time.date())

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
            payments.c['user_id'].in_(relevant_users),
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
        func.encode(subscription_id_access_level.c['name'], 'utf-8').label("sub_type_name"),
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
            subscriptions.columns.user_id.in_(relevant_users),
            *get_table_created_at_filters(subscriptions, max_time, min_time)
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


def get_subscription_stats(stats_end_time: datetime):
    subscription_data = get_subscription_data(stats_end_time)

    # calculate time intervals between subscriptions
    subscription_data['gap'] = np.nan
    subscription_data["start_time_shifted"] = subscription_data[["start_time"]].shift(-1)
    subscription_data["user_id_shifted"] = subscription_data[["user_id"]].shift(-1)
    subscription_data['gap'] = (
            subscription_data["start_time_shifted"] - subscription_data['end_time']
    ).dt.days
    subscription_data.loc[
        subscription_data["user_id_shifted"] != subscription_data['user_id'],
        'gap'
    ] = 0

    subscription_data = subscription_data.drop("start_time_shifted", axis=1)
    subscription_data = subscription_data.drop("user_id_shifted", axis=1)

    subscription_data['web_access_level'] = 0
    subscription_data['web_access_level'] = (
            subscription_data['sub_web_access'] +
            subscription_data['sub_standard_access'] +
            subscription_data['sub_club_access']
    )

    subscription_data.loc[
        (subscription_data['web_access_level'] > 1) &
        (subscription_data['sub_web_access'] != 1),
        'web_access_level'
    ] = -1

    subscription_data.drop(["sub_web_access", "sub_standard_access", "sub_club_access"], axis=1, inplace=True)
    subscription_data["net_amount"] = subscription_data["amount"] - subscription_data["additional_amount"]
    subscription_data['real_length'] = (subscription_data['end_time'] - subscription_data['start_time']).dt.days

    # paid subscription without payment - adding net_amount and amount
    paid_subs_without_payment = np.logical_and(
        subscription_data["is_paid"] == True,
        subscription_data["payment_count"] == 0
    )

    subscription_data.loc[paid_subs_without_payment, "net_amount"] = subscription_data["sub_type_price"]

    subscription_data.loc[paid_subs_without_payment, "amount"] = subscription_data["sub_type_price"]

    # free subscription - daily_price
    subscription_data.loc[subscription_data["is_paid"] == False, "daily_price"] = 0

    # upgraded subscription - daily_price
    is_upgrade = subscription_data["is_upgraded"] == True
    subscription_data.loc[
        (is_upgrade) &
        (subscription_data["is_upgrade"] == False),
        "daily_price"] = subscription_data["net_amount"] / subscription_data["length"]

    # upgrade subscription - daily_price
    subscription_data.loc[
        is_upgrade, "daily_price"
    ] = subscription_data["sub_type_price"] / subscription_data["length"]

    # other - daily_price
    subscription_data.loc[
        subscription_data["daily_price"].isna(),
        "daily_price"
    ] = subscription_data["net_amount"] / subscription_data["length"]

    sub_desc_columns = ["length", "is_paid", "sub_print_access", "sub_print_friday_access", "web_access_level"]
    subscription_data['sub_comb'] = subscription_data[sub_desc_columns].to_dict(orient='records')
    subscription_data['sub_comb'] = subscription_data['sub_comb'].astype(str)

    sub_prices = subscription_data.groupby(sub_desc_columns, as_index=False).agg({"daily_price": "mean"}).dropna()
    sub_prices = sub_prices.rename(columns={"daily_price": "average_price"})
    sub_prices['sub_comb'] = sub_prices[sub_desc_columns].to_dict(orient='records')
    sub_prices['sub_comb'] = sub_prices['sub_comb'].astype(str)
    sub_prices = sub_prices.drop(columns=sub_desc_columns)

    subscription_data = pd.merge(
        subscription_data,
        sub_prices,
        how="left",
        on="sub_comb"
    )

    subscription_data = pd.merge(
        subscription_data,
        subscription_data[["subscription_id", "net_amount"]],
        how="left",
        left_on="base_subscription_id",
        right_on="subscription_id",
        suffixes=("", "_base_sub")
    )

    subscription_data["daily_price_diff_relative"] = (
                1 - (subscription_data["average_price"] / subscription_data["daily_price"])
    )
    subscription_data["daily_price_diff_relative"] = subscription_data["daily_price_diff_relative"].replace(
        [np.inf, -np.inf],
        np.nan
    ).fillna(0.0)

    subscription_data["is_discount_sub"] = 0.0
    subscription_data.loc[subscription_data["daily_price_diff_relative"] <= -0.2, "is_discount_sub"] = 1.0

    subscription_data = subscription_data.drop(
        columns=["base_subscription_id", "upgrade_type", "sub_comb", "average_price", "subscription_id_base_sub",
                 "net_amount_base_sub", "daily_price_diff_relative", "real_length", "daily_price"])

    subscription_data["subscription_id"] = subscription_data["subscription_id"].astype("int")

    def basic_stats_agg(x):
        names = {
            'total_amount': x['amount'].sum(),
            'subscription_count': x['subscription_id'].count(),
            'average_amount': x['amount'].mean(),
            'is_donor': x[x['additional_amount'] > 0]['additional_amount'].any(),
            'paid_subs': x[x['is_paid'] == True]['subscription_id'].count(),
            'free_subs': x[x['is_paid'] == False]['subscription_id'].count(),
            'number_of_days_since_the_first_paid_sub': (datetime.now() - x['start_time'].min()).days,
            'average_gap_paid_subs': x[x['is_paid'] == True]['gap'].mean(),
            'maximal_gap_paid_subs': x[x['is_paid'] == True]['gap'].max(),
            'total_days_paid_sub': x[x['is_paid'] == True]['length'].sum(),
            'total_days_free_sub': x[x['is_paid'] == False]['length'].sum(),
            'discount_subs': x[x['is_discount_sub'] == True]['subscription_id'].count(),
            'upgraded_subs': x[x['is_upgraded'] == True]['subscription_id'].count()

        }
        return pd.Series(names)

    import time
    start_time = time.time()
    users_sub_stats = subscription_data.groupby("user_id").apply(basic_stats_agg).reset_index()
    print((time.time() - start_time) / 60)
    raise ValueError('aa')

    subscription_data.sort_values('start_time', inplace=True)
    last_subs = subscription_data[subscription_data["is_paid"] == True].drop_duplicates('user_id', keep='last')
    previous_subs = subscription_data[
        (~subscription_data['subscription_id'].isin(last_subs['subscription_id'])) &
        (subscription_data["is_paid"] == True)
    ].drop_duplicates('user_id', keep='last')

    last_subs = pd.merge(last_subs, previous_subs, how="left", on="user_id", suffixes=("_last", "_previous"))
    last_subs = last_subs.fillna(0.0)

    computable_columns = {
        "length": "per_change",
        "is_recurrent": "val_change",
        "amount": "per_change",
        "is_recurrent_charge": "val_change",
        "web_access_level": "val_change",
        "sub_print_access": "val_change",
        "sub_print_friday_access": "val_change",
        "is_discount_sub": "val_change"
    }

    for key_column in computable_columns:
        if computable_columns[key_column] == 'per_change':
            last_subs[key_column + '_diff'] = last_subs[[key_column + '_previous', key_column + '_last']].pct_change(axis=1)[
                key_column + '_last'].tolist()
            last_subs[key_column + '_diff'].replace(np.float("inf"), 1, inplace=True)
        elif computable_columns[key_column] == 'val_change':
            last_subs[key_column + '_diff'] = last_subs[[key_column + '_previous', key_column + '_last']].diff(axis=1)[
                key_column + '_last'].tolist()
        last_subs = last_subs.drop(columns=[key_column + '_last', key_column + '_previous'])

    users_sub_stats = pd.merge(users_sub_stats, last_subs, how="left", on="user_id", validate="one_to_one")

    return users_sub_stats


def get_sub_gaps_query(
        max_date: datetime,
        mysql_predplatne_session: Session,
        subscriptions,
        relevant_users,
):
    relevant_subs = mysql_predplatne_session.query(subscriptions).filter(
        and_(
            subscriptions.c['user_id'].in_(relevant_users),
            subscriptions.c['start_time'].cast(DATE) < max_date.date(),
            subscriptions.c['is_paid'] == True
        )
    ).subquery()

    relevant_subs_2 = mysql_predplatne_session.query(relevant_subs).subquery()

    subs_joined = mysql_predplatne_session.query(
        relevant_subs.c['end_time'],
        relevant_subs_2.c['start_time'],
        func.datediff(relevant_subs.c['end_time'], relevant_subs_2.c['start_time']).label('gap'),
        relevant_subs_2.c['id']
    ).join(
        relevant_subs_2,
        and_(
            relevant_subs_2.c['user_id'] == relevant_subs.c['user_id'],
            relevant_subs_2.c['start_time'] > relevant_subs.c['end_time'],
        )
    ).subquery()

    min_diff_subs = mysql_predplatne_session.query(
        func.min(
            subs_joined.c['gap']
        ).label('min_gap'),
        subs_joined.c['id']
    ).group_by(
        subs_joined.c['id']
    ).subquery()

    sub_gaps = mysql_predplatne_session.query(
        subs_joined
    ).join(
        min_diff_subs,
        and_(
            subs_joined.c['gap'] == min_diff_subs.c['min_gap'],
            subs_joined.c['id'] == min_diff_subs.c['id']
        )
    ).subquery()

    return sub_gaps


def get_subscription_upgrades_subquery(
    max_date: datetime,
    mysql_predplatne_session: Session,
    subscription_upgrades
):
    subscriptions_upgraded_subs = mysql_predplatne_session.query(
        subscription_upgrades.c['base_subscription_id'].label("base_subscription_id"),
        func.if_(func.count(subscription_upgrades.c['id']) > 0, True, False).label("is_upgraded")
    ).group_by(
        subscription_upgrades.c['base_subscription_id']
    ).filter(
        subscription_upgrades.c['created_at'].cast(DATE) < max_date.date(),
    ).subquery()

    return subscriptions_upgraded_subs
