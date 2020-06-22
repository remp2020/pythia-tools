from __future__ import print_function
import os.path
import argparse
import arrow
import mysql.connector
import psycopg2
import psycopg2.extras
from datetime import date, timedelta
from utils import load_env, create_con

BASE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')


class Event:
    def __init__(self, user_id, time):
        self.user_id = user_id
        self.time = time


def create_mysql_connection(username, password, db, host):
    cnx = mysql.connector.connect(user=username, password=password, host=host, database=db)
    cursor = cnx.cursor()
    return cnx, cursor


def churn_or_renewed_users(cursor, subscriptions_stop_date, churn_days_threshold):
    print("Loading churn/renewal users data for date " + str(subscriptions_stop_date) + ", churn threshold " + str(
        churn_days_threshold) + " days")
    day = subscriptions_stop_date.strftime("%Y-%m-%d")

    day_start = day + ' 00:00:00'
    day_end = day + ' 23:59:59'

    sql = '''
SELECT s1.user_id AS user_id, s1.end_time AS sub_end, s2.start_time AS next_sub_start 
FROM subscriptions s1
LEFT JOIN subscriptions s2 ON 
    s1.user_id = s2.user_id AND 
    s1.end_time <= s2.start_time AND
    DATE_ADD(s1.end_time, INTERVAL {} DAY) >= s2.start_time AND
    DATE_ADD(s1.end_time, INTERVAL {} DAY) <= s2.end_time
    AND s2.is_paid=true
WHERE s1.end_time >= '{}' AND s1.end_time <= '{}' AND s1.is_paid=true
    '''
    sql = sql.format(churn_days_threshold, churn_days_threshold, day_start, day_end)
    cursor.execute(sql)

    churn_events_list = []
    renewal_events_list = []

    for user_id, sub_end, next_sub_start in cursor:
        if next_sub_start is None:
            churn_events_list.append(Event(str(user_id), sub_end))
        else:
            renewal_events_list.append(Event(str(user_id), next_sub_start))
    return churn_events_list, renewal_events_list


def save_events_to_aggregated_user_days(cursor, subscriptions_stop_date, churn_events, event_name):
    end = arrow.get(subscriptions_stop_date).shift(days=-1)
    start = end.shift(days=-29)

    print("Marking days " + start.strftime("%Y-%m-%d") + " - " + end.strftime("%Y-%m-%d"))

    sql = '''
    UPDATE aggregated_user_days 
    SET next_30_days = %s, next_event_time = %s
    WHERE date = %s AND user_id = %s AND next_30_days = 'ongoing'
    '''
    psycopg2.extras.execute_batch(cursor, sql, [
        (event_name, event.time.isoformat(), day[0].date(), event.user_id)
        for event in churn_events
        for day in arrow.Arrow.span_range('day', start, end)
    ])
    cursor.connection.commit()


def save_events(cursor, computed_for, churn_events, event_name):
    sql = '''
        INSERT INTO events (user_id, time, type, computed_for)
        VALUES (%s, %s, %s, %s)
    '''
    psycopg2.extras.execute_batch(cursor, sql, [
        (event.user_id, event.time.isoformat(), event_name, computed_for)
        for event in churn_events
    ])
    cursor.connection.commit()


def run(churn_date):
    load_env()

    if os.getenv("CRM_DB_HOST") is None:
        print('CRM database connection settings not set in .env file, skipping churn/renewal data aggregation')
        exit()

    year = int(churn_date[0:4])
    month = int(churn_date[4:6])
    day = int(churn_date[6:8])
    churn_date = date(year, month, day)

    crm_db_conn, crm_db_cur = create_mysql_connection(
        os.getenv("CRM_DB_USER"),
        os.getenv("CRM_DB_PASS"),
        os.getenv("CRM_DB_DB"),
        os.getenv("CRM_DB_HOST")
    )

    # churn date threshold says how long is the interval for user to renew subscription without being counted as a churn
    # by default 2 days
    churn_days_threshold = 2
    subscriptions_stop_date = churn_date - timedelta(days=churn_days_threshold)

    churn_events_list, renewal_events_list = churn_or_renewed_users(crm_db_cur, subscriptions_stop_date,
                                                                    churn_days_threshold)

    crm_db_cur.close()
    crm_db_conn.close()

    postgre_conn, postgre_cur = create_con(
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASS"),
        os.getenv("POSTGRES_DB"),
        os.getenv("POSTGRES_HOST")
    )

    # Delete events for particular day (so command can be safely run multiple times)
    event_types = ['churn', 'renewal']
    postgre_cur.execute('''
            DELETE FROM events WHERE computed_for = %s AND type = ANY(%s)
        ''', (churn_date, event_types))
    postgre_conn.commit()

    print("Saving churn events, count=" + str(len(churn_events_list)))
    if churn_events_list:
        save_events_to_aggregated_user_days(postgre_cur, subscriptions_stop_date, churn_events_list, "churn")
        save_events(postgre_cur, churn_date, churn_events_list, "churn")

    print("Saving renewal events, count=" + str(len(renewal_events_list)))
    if renewal_events_list:
        save_events_to_aggregated_user_days(postgre_cur, subscriptions_stop_date, renewal_events_list, "renewal")
        save_events(postgre_cur, churn_date, renewal_events_list, "renewal")

    postgre_cur.close()
    postgre_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Script to mark churn/renewal subscriptions')
    parser.add_argument('date', metavar='date', help='date, format YYYYMMDD')

    args = parser.parse_args()
    run(args.date)


if __name__ == '__main__':
    main()
