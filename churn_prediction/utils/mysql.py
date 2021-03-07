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
        ['payments', 'subscriptions', 'content_access', 'subscription_type_content_access', 'subscription_types']
    )

    mysql_predplatne_session = predplatne_mysql_mappings['session']
    payments = predplatne_mysql_mappings['payments']
    subscriptions = predplatne_mysql_mappings['subscriptions']
    content_access = predplatne_mysql_mappings['content_access']
    subscription_type_content_access = predplatne_mysql_mappings['subscription_type_content_access']
    subscription_types = predplatne_mysql_mappings['subscription_types']

    relevant_users = get_users_with_expirations(end_time)

    subscription_id_access_level = mysql_predplatne_session.query(
        subscription_type_content_access.c['subscription_type_id'].label("subscription_type_id"),
        subscription_types.c['name'].label("name"),
        subscription_types.c['code'].label("code"),
        subscription_types.c['length'].label("length"),
        subscription_types.c['price'].label("price"),
        func.sum(
            case(
                [
                    (
                        content_access.c['name'] == "web"
                        , True)
                ],
                else_=False)
        ).label("is_web"),
        func.sum(
            case(
                [
                    (
                        content_access.c['name'] == "standard"
                        , True)
                ],
                else_=False)
        ).label("is_standard"),
        func.sum(
            case(
                [
                    (
                        content_access.c['name'] == "club"
                        , True)
                ],
                else_=False)
        ).label("is_club"),
        func.sum(
            case(
                [
                    (
                        content_access.c['name'] == "print"
                        , True)
                ],
                else_=False)
        ).label("is_print"),
        func.sum(
            case(
                [
                    (
                        content_access.c['name'] == "print_friday"
                        , True)
                ],
                else_=False)
        ).label("is_print_friday")
    ).join(
        content_access,
        content_access.c['id'] == subscription_type_content_access.c['content_access_id']
    ).join(
        subscription_types,
        subscription_types.c['id'] == subscription_type_content_access.c['subscription_type_id']
    ).group_by("subscription_type_id", "name", "code", "length", "price").subquery()

    payments_grouped_filtered = mysql_predplatne_session.query(
        payments.c['subscription_id'].label("subscription_id"),
        func.count(payments.c['id']).label("payment_count"),
        func.sum(payments.c['amount']).label("amount"),
        func.sum(payments.c['additional_amount']).label("additional_amount"),
        func.if_(func.sum(payments.c['recurrent_charge']) > 0, True, False).label("is_recurrent_charge"),
    ).filter(
        payments.c['status'].in_(['paid', 'prepaid', 'imported']),
        payments.c['subscription_id'].isnot(None)
    ).group_by(
        "subscription_id"
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
        func.encode(subscription_id_access_level.c['name'], 'utf-8').label("sub_type_name"),
        subscription_id_access_level.c['code'].label("sub_type_code"),
        subscription_id_access_level.c['length'].label("sub_type_length"),
        subscription_id_access_level.c['price'].label("sub_type_price"),
        subscription_id_access_level.c['is_web'].label("sub_web_access"),
        subscription_id_access_level.c['is_standard'].label("sub_standard_access"),
        subscription_id_access_level.c['is_club'].label("sub_club_access"),
        subscription_id_access_level.c['is_print'].label("sub_print_access"),
        subscription_id_access_level.c['is_print_friday'].label("sub_print_friday_access")
    ).join(
        payments_grouped_filtered,
        subscriptions.c['id'] == payments_grouped_filtered.c['subscription_id'],
        isouter=True
    ).join(
        subscription_id_access_level,
        subscriptions.c['subscription_type_id'] == subscription_id_access_level.c['subscription_type_id'],
        isouter=True
    ).filter(
        subscriptions.columns.user_id.in_(relevant_users),
        subscriptions.columns.start_time < end_time
    )

    subscriptions_data_merged = pd.read_sql(subscriptions_data.statement, subscriptions_data.session.bind)

    mysql_predplatne_session.close()

    subscriptions_data_merged = subscriptions_data_merged.fillna(0)

    return subscriptions_data_merged


def subscription_stats(stats_end_time: datetime):

    subscription_data = get_subscription_data(stats_end_time)

    def web_access_features_aggregation(sub_web_access, sub_standard_access, sub_club_access):
        if sub_web_access == False and sub_standard_access == False and sub_club_access == False:
            access_level = 0
        elif sub_web_access == True and sub_standard_access == False and sub_club_access == False:
            access_level = 1
        elif sub_web_access == True and sub_standard_access == True and sub_club_access == False:
            access_level = 2
        elif sub_web_access == True and sub_club_access == True:
            access_level = 3
        else:
            access_level = -1
        return access_level

    subscription_data['web_access_level'] = subscription_data.apply(
        lambda x: web_access_features_aggregation(x['sub_web_access'], x['sub_standard_access'], x['sub_club_access']),
        axis=1)
    subscription_data = subscription_data.drop(["sub_web_access", "sub_standard_access", "sub_club_access"], axis=1)

    subscription_data["net_amount"] = subscription_data["amount"] - subscription_data["additional_amount"]

    sub_desc_columns = ["length", "is_paid", "is_recurrent", "sub_print_access", "sub_print_friday_access",
                        "web_access_level"]
    subscription_data['sub_comb'] = subscription_data[sub_desc_columns].to_dict(orient='records')
    subscription_data['sub_comb'] = subscription_data['sub_comb'].astype(str)

    sub_prices = subscription_data.groupby(sub_desc_columns).mean()[["net_amount"]].dropna().reset_index()
    sub_prices = sub_prices.rename(columns={"net_amount": "average_price"})
    sub_prices['sub_comb'] = sub_prices[sub_desc_columns].to_dict(orient='records')
    sub_prices['sub_comb'] = sub_prices['sub_comb'].astype(str)
    sub_prices = sub_prices.drop(columns=sub_desc_columns)

    subscription_data = pd.merge(subscription_data, sub_prices, how="left", on="sub_comb")

    mask = (subscription_data[['net_amount', 'average_price']].values == [0, 0]).all(1)
    subscription_data["price_diff"] = (subscription_data['net_amount'] - subscription_data['average_price']).div(
        (subscription_data['net_amount'] + subscription_data['average_price']) / 2, fill_value=0).where(~mask, 0)

    subscription_data["is_discount_sub"] = False
    subscription_data.loc[subscription_data["price_diff"] <= -0.33, "is_discount_sub"] = True

    subscription_data = subscription_data.drop(columns=["sub_comb", "average_price", "price_diff"])

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
    overlap_list = []
    for row in sub_data_merged.itertuples():
        overlap_list.extend(
            [max(row.start_time_original, row.start_time_copied) < min(row.end_time_original, row.end_time_copied)])

    sub_data_merged.loc[:, "overlap"] = overlap_list

    # selects rows with different subscription_ids
    sub_overlap_is_true = sub_data_merged.loc[sub_data_merged["overlap"] == True].copy()
    sub_overlap_is_true_list = list(set(
        sub_overlap_is_true["subscription_id_original"].unique().tolist() + sub_overlap_is_true[
            "subscription_id_copied"].unique().tolist()))

    # selects rows with a unique combination of subscription_ids
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
    while sub_id_set:
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

    # subscription_group features aggregation
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
            'sub_type_name': x['sub_type_name'].tolist(),
            'sub_type_code': x['sub_type_code'].tolist(),
            'sub_type_length': x['sub_type_length'].tolist(),
            'sub_type_price': x['sub_type_price'].tolist(),
            'sub_print_access': x['sub_print_access'].any(),
            'sub_print_friday_access': x['sub_print_friday_access'].any(),
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
    subscription_data_paid["gap"] = subscription_data_paid.apply(
        lambda x: (x.start_time_shifted - x.end_time).total_seconds() / (
                    60 * 60 * 24) if x.user_id == x.user_id_shifted else 0, axis=1)

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
        }
        return pd.Series(names)

    users_sub_stats = subscription_data.groupby("user_id").apply(basic_stats_agg).reset_index()

    def try_join(l):
        if isinstance(l, list):
            return ','.join(map(str, l))
        else:
            return l

    def relative_change(col1, col2):
        if pd.isnull(col1) or col1 == 0:
            return 1
        else:
            return (1 - (col2 / col1)).abs()

    def val_change(col1 , col2):
        return col2 - col1

    subscription_data['subscription_id_converted'] = [try_join(l) for l in subscription_data['subscription_id']]

    feature_columns=["user_id", "subscription_id_converted","start_time","length","is_recurrent","amount","additional_amount","is_recurrent_charge","web_access_level","sub_print_access","sub_print_friday_access"]

    last_subs=subscription_data.loc[subscription_data["is_paid"]==True][feature_columns].copy()
    last_subs=last_subs.sort_values('start_time').groupby('user_id').tail(1)
    last_subs_ids=last_subs['subscription_id_converted'].tolist()
    last_subs=last_subs.drop(columns=['subscription_id_converted','start_time'])

    previous_subs=subscription_data.loc[subscription_data["is_paid"]==True][feature_columns].copy()
    previous_subs=previous_subs[~previous_subs['subscription_id_converted'].isin(last_subs_ids)]
    previous_subs=previous_subs.sort_values('start_time').groupby('user_id').tail(1)
    previous_subs=previous_subs.drop(columns=['subscription_id_converted','start_time'])

    last_subs=pd.merge(last_subs,previous_subs,how="left",on="user_id",suffixes=("_last","_previous"))
    last_subs=last_subs.fillna(0)

    computable_columns={"length":"per_change",
                        "is_recurrent":"value_change",
                        "amount":"per_change",
                        "additional_amount":"per_change",
                        "is_recurrent_charge":"value_change",
                        "web_access_level":"value_change",
                        "sub_print_access":"value_change",
                        "sub_print_friday_access":"value_change"
                    }

    for key_column in computable_columns:
        if(computable_columns[key_column] == 'per_change'):
            last_subs[key_column + '_diff']=relative_change(last_subs[key_column + '_previous'],last_subs[key_column + '_last'])
        elif(computable_columns[key_column] == 'value_change'):
            last_subs[key_column + '_diff']=val_change(last_subs[key_column + '_previous'],last_subs[key_column + '_last'])
        last_subs=last_subs.drop(columns=[key_column + '_last',key_column + '_previous'])

    users_sub_stats=pd.merge(users_sub_stats,last_subs,how="left",on="user_id",validate="one_to_one")
        
    return users_sub_stats


from dotenv import load_dotenv
from datetime import timedelta
load_dotenv('../.env')
subscription_stats(datetime.utcnow().date())



