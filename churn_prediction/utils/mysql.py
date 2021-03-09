import pandas as pd
from sqlalchemy.sql.elements import Cast
from sqlalchemy.types import DATE
from sqlalchemy import and_, func, case
from datetime import datetime
from sqlalchemy import MetaData, Table
from prediction_commons.utils.db_utils import create_connection
from sqlalchemy.orm import sessionmaker
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
    _, db_connection = create_connection(os.getenv(db_connection_string_name))

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
            Cast(subscriptions.c['end_time'], DATE) == aggregation_date
        )
    ).group_by(
        subscriptions.c['user_id']
    )

    relevant_users = relevant_users.all()
    relevant_users = [user_id[0] for user_id in relevant_users]
    mysql_predplatne_session.close()

    return relevant_users

def get_subscription_data(end_time: datetime):
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

    relevant_users = get_users_with_expirations()

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
    ).filter(subscription_types.c['no_subscription'] != True).subquery()

    payments_grouped_filtered = mysql_predplatne_session.query(
        payments.c['subscription_id'].label("subscription_id"),
        func.count(payments.c['id']).label("payment_count"),
        func.sum(payments.c['amount']).label("amount"),
        func.sum(payments.c['additional_amount']).label("additional_amount"),
        func.if_(func.sum(payments.c['recurrent_charge']) > 0, True, False).label("is_recurrent_charge"),
        func.group_concat(payments.c['status']).label("payment_status")
    ).filter(
        payments.c['subscription_id'].isnot(None)
    ).group_by(
        payments.c['subscription_id']
    ).subquery()

    subscriptions_upgraded_subs = mysql_predplatne_session.query(
        subscription_upgrades.c['base_subscription_id'].label("base_subscription_id"),
        func.if_(func.count(subscription_upgrades.c['id']) > 0, True, False).label("is_upgraded")
    ).group_by(
        subscription_upgrades.c['base_subscription_id']
    ).subquery()

    subscriptions_upgrades = mysql_predplatne_session.query(
        subscription_upgrades.c['base_subscription_id'].label("base_subscription_id"),
        subscription_upgrades.c['upgraded_subscription_id'].label("upgraded_subscription_id"),
        subscription_upgrades.c['type'].label("upgrade_type"),
        func.if_(func.count(subscription_upgrades.c['id']) > 0, True, False).label("is_upgrade")
    ).group_by(
        subscription_upgrades.c['base_subscription_id'],
        subscription_upgrades.c['upgraded_subscription_id'],
        subscription_upgrades.c['type']
    ).subquery()

    subscriptions_data = mysql_predplatne_session.query(
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
            subscriptions.columns.start_time < end_time)
    )

    subscriptions_data_merged = pd.read_sql(subscriptions_data.statement, subscriptions_data.session.bind) 
    
    subscriptions_data_merged=subscriptions_data_merged.loc[subscriptions_data_merged["start_time"]!=subscriptions_data_merged["end_time"]].copy()
    subscriptions_data_merged=subscriptions_data_merged.loc[~subscriptions_data_merged["payment_status"].str.contains("form|refund", na=False)].copy()
    subscriptions_data_merged=subscriptions_data_merged.loc[subscriptions_data_merged["start_time"]>=datetime(2015,1,1,0,0,0)].copy()
    
    column_fillna={'payment_count':0,
                  'amount':0.0,
                  'additional_amount':0.0,
                  'is_recurrent_charge':False,
                  'payment_status':'no_payment',
                  'is_upgraded':False,
                  'base_subscription_id':subscriptions_data_merged['subscription_id'],
                  'upgrade_type':'none',
                  'is_upgrade':False,
                  }
    
    subscriptions_data_merged.fillna(column_fillna, inplace=True)    

    column_types={'user_id':int,
                 'subscription_id':int,
                 'length':int,
                 'is_recurrent':float,
                 'is_paid':float,
                 'subscription_type':str,
                 'subscription_type_id':int,
                 'payment_count':int,
                 'amount':float,
                 'additional_amount':float,
                 'is_recurrent_charge':float,
                 'payment_status':str,
                 'sub_type_name':str,
                 'sub_type_code':str,
                 'sub_type_length':int,
                 'sub_type_price':float,
                 'sub_web_access':float,
                 'sub_standard_access':float,
                 'sub_club_access':float,
                 'sub_print_access':float,
                 'sub_print_friday_access':float,
                 'is_upgraded':float,
                 'base_subscription_id':int,
                 'upgrade_type':str,
                 'is_upgrade':float
                 }
    
    subscriptions_data_merged = subscriptions_data_merged.astype(column_types) 
    
    mysql_predplatne_session.close()

    return subscriptions_data_merged

def get_subscription_stats(stats_end_time: datetime):
    subscription_data = get_subscription_data(stats_end_time)

    def web_access_features_aggregation(sub_web_access, sub_standard_access, sub_club_access):
        access_level = 0
        if sub_web_access:
            access_level += 1
            if sub_standard_access:
                access_level += 1
                if sub_club_access:
                    access_level += 1
        elif sub_standard_access or sub_club_access:
            access_level -= 1
        return access_level

    subscription_data['web_access_level'] = subscription_data.apply(
        lambda x: web_access_features_aggregation(x['sub_web_access'], x['sub_standard_access'], x['sub_club_access']),
        axis=1)
    subscription_data['web_access_level'] = subscription_data['web_access_level'].astype(float)

    subscription_data.drop(["sub_web_access", "sub_standard_access", "sub_club_access"], axis=1, inplace=True)
    subscription_data["net_amount"] = subscription_data["amount"] - subscription_data["additional_amount"]
    subscription_data['real_length'] = (
                (subscription_data['end_time'] - subscription_data['start_time']).dt.total_seconds() / (
                    24 * 60 * 60)).astype(float)

    # paid subscription without payment - adding net_amount and amount
    subscription_data.loc[
        (subscription_data["is_paid"] == True) & (subscription_data["payment_count"] == 0), "net_amount"] = \
    subscription_data["sub_type_price"]
    subscription_data.loc[
        (subscription_data["is_paid"] == True) & (subscription_data["payment_count"] == 0), "amount"] = \
    subscription_data["sub_type_price"]

    # free subscription - daily_price
    subscription_data.loc[subscription_data["is_paid"] == False, "daily_price"] = 0

    # upgraded subscription - daily_price
    subscription_data.loc[
        (subscription_data["is_upgraded"] == True) & (subscription_data["is_upgrade"] == False), "daily_price"] = \
    subscription_data["net_amount"] / subscription_data["length"]

    # upgrade subscription - daily_price
    subscription_data.loc[(subscription_data["is_upgrade"] == True), "daily_price"] = subscription_data[
                                                                                          "sub_type_price"] / \
                                                                                      subscription_data["length"]

    # other - daily_price
    subscription_data.loc[(pd.isna(subscription_data["daily_price"])), "daily_price"] = subscription_data[
                                                                                            "net_amount"] / \
                                                                                        subscription_data["length"]

    sub_desc_columns = ["length", "is_paid", "sub_print_access", "sub_print_friday_access", "web_access_level"]
    subscription_data['sub_comb'] = subscription_data[sub_desc_columns].to_dict(orient='records')
    subscription_data['sub_comb'] = subscription_data['sub_comb'].astype(str)

    sub_prices = subscription_data.groupby(sub_desc_columns, as_index=False).agg({"daily_price": "mean"}).dropna()
    sub_prices = sub_prices.rename(columns={"daily_price": "average_price"})
    sub_prices['sub_comb'] = sub_prices[sub_desc_columns].to_dict(orient='records')
    sub_prices['sub_comb'] = sub_prices['sub_comb'].astype(str)
    sub_prices = sub_prices.drop(columns=sub_desc_columns)

    subscription_data = pd.merge(subscription_data, sub_prices, how="left", on="sub_comb")

    subscription_data = pd.merge(subscription_data, subscription_data[["subscription_id", "net_amount"]], how="left",
                                 left_on="base_subscription_id", right_on="subscription_id", suffixes=("", "_base_sub"))
    subscription_data.loc[subscription_data["daily_price"] > 0, "daily_price_diff_relative"] = (
                1 - (subscription_data["average_price"] / subscription_data["daily_price"]))

    subscription_data["is_discount_sub"] = 0.0
    subscription_data.loc[subscription_data["daily_price_diff_relative"] <= -0.2, "is_discount_sub"] = 1.0

    subscription_data = subscription_data.drop(
        columns=["base_subscription_id", "upgrade_type", "sub_comb", "average_price", "subscription_id_base_sub",
                 "net_amount_base_sub", "daily_price_diff_relative", "real_length", "daily_price"])

    subscription_data["subscription_id"] = subscription_data["subscription_id"].astype("int")

    # splitiing data to 2 groups - paid and free subs
    subscription_data_free = subscription_data.loc[subscription_data["is_paid"] == False].copy()
    subscription_data_paid = subscription_data.loc[subscription_data["is_paid"] == True].copy()
    del subscription_data

    sub_without_overlap = []

    # original dataframe with paid subs
    sub_data = subscription_data_paid[["user_id", "subscription_id", "start_time", "end_time"]].copy()
    # copy of the original dataframe
    sub_data_copied = sub_data.copy()

    # merging dataframes on "user_id" - get all combinations of subcription_ids
    sub_data_merged = pd.merge(sub_data, sub_data_copied, how="left", on="user_id", suffixes=("_original", "_copied"))
    sub_data_merged = sub_data_merged.loc[
        sub_data_merged["subscription_id_original"] != sub_data_merged["subscription_id_copied"]].copy()

    # testing whether the identifiers have a time overlap - True or False
    sub_data_merged["overlap"] = sub_data_merged[['start_time_original', 'start_time_copied']].max(axis=1) < \
                                 sub_data_merged[['end_time_original', 'end_time_copied']].min(axis=1)

    # selects rows with different subscription_ids
    sub_overlap_is_true = sub_data_merged.loc[sub_data_merged["overlap"] == True].copy()
    sub_overlap_is_true_list = list(set(
        sub_overlap_is_true["subscription_id_original"].unique().tolist() + sub_overlap_is_true[
            "subscription_id_copied"].unique().tolist()))

    sub_overlap_is_true = sub_overlap_is_true.loc[
        ~sub_overlap_is_true.apply(lambda x: frozenset([x.subscription_id_original, x.subscription_id_copied]),
                                   axis=1).duplicated()].copy()

    # calculate features after merging dataframes - if overlap==True
    sub_overlap_is_true["subscription_id"] = sub_overlap_is_true.apply(
        lambda x: (x.subscription_id_original, x.subscription_id_copied), axis=1)

    # remove redundant columns
    sub_overlap_is_true = sub_overlap_is_true.drop("overlap", axis=1)
    sub_overlap_is_true = sub_overlap_is_true.drop([col for col in sub_overlap_is_true.columns if 'original' in col],
                                                   axis=1)
    sub_overlap_is_true = sub_overlap_is_true.drop([col for col in sub_overlap_is_true.columns if 'copied' in col],
                                                   axis=1)

    # grouping of all overlapping subscriptions - subscription group_id assignment
    sub_id_set = set(
        [frozenset(s) for s in sub_overlap_is_true["subscription_id"].tolist()])  # Convert to a set of sets
    sub_with_overlap = []
    while (sub_id_set):
        sub_id_set_current = set(sub_id_set.pop())
        check = len(sub_id_set)
        while check:
            check = False
            for s in sub_id_set.copy():
                if sub_id_set_current.intersection(s):
                    check = True
                    sub_id_set.remove(s)
                    sub_id_set_current.update(s)
        sub_with_overlap.append(sorted(list(sub_id_set_current)))

        # subscription_group_id assignment
    sub_matching_list = [[l_value, l_index + 1] for l_index, l_values in enumerate(sub_with_overlap) for l_value in
                         l_values]
    sub_matching_table = pd.DataFrame(sub_matching_list, columns=["subscription_id", "subscription_period_id"])
    sub_overlap_is_true = pd.merge(subscription_data_paid, sub_matching_table, how="right", on="subscription_id",
                                   validate="one_to_one")

    # subscription_group features aggregation to one row
    def subscriptions_with_overlap_agg(x):
        names = {
            'user_id': x['user_id'].max(),
            'subscription_id': sorted(x['subscription_id'].unique().tolist()),
            'start_time': x['start_time'].min(),
            'end_time': x['end_time'].max(),
            'length': (x['end_time'].max() - x['start_time'].min()).days,
            'is_recurrent': x['is_recurrent'].any(),
            'is_paid': x['is_paid'].any(),
            'subscription_type': sorted(x['subscription_type'].unique().tolist()),
            'subscription_type_id': sorted(x['subscription_type_id'].unique().tolist()),
            'payment_count': x['payment_count'].sum(),
            'amount': x['amount'].sum(),
            'additional_amount': x['additional_amount'].sum(),
            'is_recurrent_charge': x['is_recurrent_charge'].any(),
            'payment_status': x['payment_status'].tolist(),
            'sub_type_name': x['sub_type_name'].tolist(),
            'sub_type_code': x['sub_type_code'].tolist(),
            'sub_type_length': x['sub_type_length'].tolist(),
            'sub_type_price': x['sub_type_price'].tolist(),
            'sub_print_access': x['sub_print_access'].any(),
            'sub_print_friday_access': x['sub_print_friday_access'].any(),
            'is_upgraded': x['is_upgraded'].any(),
            'is_upgrade': x['is_upgrade'].any(),
            'web_access_level': x['web_access_level'].max(),
            'net_amount': x['net_amount'].sum(),
            'is_discount_sub': x['is_discount_sub'].any(),
        }

        return pd.Series(names)

    sub_overlap_is_true = sub_overlap_is_true.groupby('subscription_period_id').apply(
        subscriptions_with_overlap_agg).reset_index()

    sub_overlap_is_true = sub_overlap_is_true.drop(columns=['subscription_period_id'])

    # remove subscription with overlap from original dataframe and append grouped subscription data with overlap
    subscription_data_paid = subscription_data_paid[
        ~subscription_data_paid["subscription_id"].isin(sub_overlap_is_true_list)]
    subscription_data_paid = pd.concat([subscription_data_paid, sub_overlap_is_true], sort=False)

    # calculate time intervals between subscriptions
    subscription_data_paid = subscription_data_paid.sort_values(["user_id", "start_time"], ascending=["True", "True"])
    subscription_data_paid["start_time_shifted"] = subscription_data_paid[["start_time"]].shift(-1)
    subscription_data_paid["user_id_shifted"] = subscription_data_paid[["user_id"]].shift(-1)

    subscription_data_paid['gap'] = (subscription_data_paid['start_time_shifted'] - subscription_data_paid[
        'end_time']).dt.total_seconds() / (60 * 60 * 24)
    subscription_data_paid.loc[
        subscription_data_paid['user_id_shifted'] != subscription_data_paid['user_id'], 'gap'] = 0

    subscription_data_paid = subscription_data_paid.drop("start_time_shifted", axis=1)
    subscription_data_paid = subscription_data_paid.drop("user_id_shifted", axis=1)

    subscription_data_free["gap"] = np.nan

    # re-merging data to one dataframe - paid+free subs
    subscription_data = pd.concat([subscription_data_paid, subscription_data_free], sort=False)
    del subscription_data_paid
    del subscription_data_free

    # calculation of the number of subscriptions in one record
    subscription_data['subcription_count'] = subscription_data['subscription_id'].apply(
        lambda x: len(x) if isinstance(x, list) else 1)

    def basic_stats_agg(x):
        names = {
            'total_amount': x['amount'].sum(),
            'average_amount': x['amount'].mean(),
            'is_donor': x[x['additional_amount'] > 0]['additional_amount'].any(),
            'paid_subs': x[x['is_paid'] == True]['subscription_id'].count(),
            'free_subs': x[x['is_paid'] == False]['subscription_id'].count(),
            'number_of_days_since_the_first_paid_sub': (datetime.now() - x['start_time'].min()).total_seconds() / (
                        60 * 60 * 24),
            'average_gap_paid_subs': x[x['is_paid'] == True]['gap'].mean(),
            'maximal_gap_paid_subs': x[x['is_paid'] == True]['gap'].max(),
            'total_days_paid_sub': x[x['is_paid'] == True]['length'].sum(),
            'total_days_free_sub': x[x['is_paid'] == False]['length'].sum(),
            'discount_subs': x[x['is_discount_sub'] == True]['subscription_id'].count(),
            'upgraded_subs': x[x['is_upgraded'] == True]['subscription_id'].count()

        }
        return pd.Series(names)

    users_sub_stats = subscription_data.groupby("user_id").apply(basic_stats_agg).reset_index()

    def try_join(l):
        if isinstance(l, list):
            return ','.join(map(str, l))
        else:
            return l

    subscription_data['subscription_id_converted'] = [try_join(l) for l in subscription_data['subscription_id']]

    feature_columns = ["user_id", "subscription_id_converted", "start_time", "length", "is_recurrent", "amount",
                       "is_recurrent_charge", "web_access_level", "sub_print_access", "sub_print_friday_access",
                       "is_discount_sub"]

    last_subs = subscription_data.loc[subscription_data["is_paid"] == True][feature_columns].copy()
    last_subs = last_subs.sort_values('start_time').groupby('user_id').tail(1)
    last_subs_ids = last_subs['subscription_id_converted'].tolist()
    last_subs = last_subs.drop(columns=['subscription_id_converted', 'start_time'])

    previous_subs = subscription_data.loc[subscription_data["is_paid"] == True][feature_columns].copy()
    previous_subs = previous_subs[~previous_subs['subscription_id_converted'].isin(last_subs_ids)]
    previous_subs = previous_subs.sort_values('start_time').groupby('user_id').tail(1)
    previous_subs = previous_subs.drop(columns=['subscription_id_converted', 'start_time'])

    last_subs = pd.merge(last_subs, previous_subs, how="left", on="user_id", suffixes=("_last", "_previous"))
    last_subs = last_subs.fillna(0)

    computable_columns = {"length": "per_change",
                          "is_recurrent": "val_change",
                          "amount": "per_change",
                          "is_recurrent_charge": "val_change",
                          "web_access_level": "val_change",
                          "sub_print_access": "val_change",
                          "sub_print_friday_access": "val_change",
                          "is_discount_sub": "val_change"
                          }

    for key_column in computable_columns:
        if (computable_columns[key_column] == 'per_change'):
            last_subs[key_column + '_diff'] = \
            last_subs[[key_column + '_previous', key_column + '_last']].pct_change(axis=1)[
                key_column + '_last'].tolist()
            last_subs[key_column + '_diff'].replace(np.float("inf"), 1, inplace=True)
        elif (computable_columns[key_column] == 'val_change'):
            last_subs[key_column + '_diff'] = last_subs[[key_column + '_previous', key_column + '_last']].diff(axis=1)[
                key_column + '_last'].tolist()
        last_subs = last_subs.drop(columns=[key_column + '_last', key_column + '_previous'])

    users_sub_stats = pd.merge(users_sub_stats, last_subs, how="left", on="user_id", validate="one_to_one")
    
    return users_sub_stats

