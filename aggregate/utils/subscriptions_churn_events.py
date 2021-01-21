from __future__ import print_function
from datetime import timedelta
import pandas


class Event:
    def __init__(self, user_id, time, type):
        self.user_id = user_id
        self.time = time
        self.type = type


class ChurnEventsParser:
    def __init__(self, cur_date, crm_mysql_cursor):
        # churn threshold defines interval for user to renew subscription
        # if there is a pause between subscriptions A and B < churn_threshold_in_days
        # we count it as 'renewal', otherwise as 'churn' event
        self.churn_threshold_in_days = 2
        self.cur_date = cur_date
        self.crm_mysql_cursor = crm_mysql_cursor

        # computed events list
        self.events_list = []

    def load_data(self):
        # one can tell if user churned/renewed for subscriptions ending on the 'subscriptions_churn_threshold_date' day
        subscriptions_churn_threshold_date = self.cur_date - timedelta(days=self.churn_threshold_in_days)
        threshold_day = subscriptions_churn_threshold_date.strftime("%Y-%m-%d")
        threshold_day_start = threshold_day + ' 00:00:00'
        threshold_day_end = threshold_day + ' 23:59:59'

        print("ChurnEventsParser - loading churn/renewal users data for date "
              + str(self.cur_date)
              + ", churn threshold " + str(self.churn_threshold_in_days) + " day(s)")

        # Look for subscriptions(s1) that ended on 'threshold_day' (having length > 5)
        # and join them with the same user|type future subscriptions (s2) (having length > 5)
        # that start within 'self.churn_threshold_in_days'
        sql = '''
        SELECT s1.user_id AS user_id, s1.end_time AS sub_end, s2.start_time AS next_sub_start
        FROM subscriptions s1
            JOIN subscription_types st ON s1.subscription_type_id = st.id AND st.length > 5
            LEFT JOIN
            (SELECT subscriptions.* FROM subscriptions JOIN subscription_types ON subscriptions.subscription_type_id = subscription_types.id AND subscription_types.length > 5) s2
            ON
                s1.user_id = s2.user_id
                AND s1.end_time <= s2.start_time
                AND DATE_ADD(s1.end_time, INTERVAL {} DAY) >= s2.start_time
                AND DATE_ADD(s1.end_time, INTERVAL {} DAY) <= s2.end_time
        WHERE s1.end_time >= '{}' AND s1.end_time <= '{}'
        '''
        sql = sql.format(self.churn_threshold_in_days, self.churn_threshold_in_days, threshold_day_start, threshold_day_end)
        self.crm_mysql_cursor.execute(sql)
        events_list = []
        churn_events_count = 0
        renewal_events_count = 0
        for user_id, sub_end, next_sub_start in self.crm_mysql_cursor:
            if next_sub_start is None:
                events_list.append(Event(str(user_id), sub_end, "churn"))
                churn_events_count += 1
            else:
                events_list.append(Event(str(user_id), next_sub_start, "renewal"))
                renewal_events_count += 1
        self.events_list = events_list
        print("Loaded " + str(churn_events_count) + " churn event(s) and " + str(renewal_events_count) + " renewal event(s)")

    def upload_to_bq(self, bq_uploader):
        print("ChurnEventsParser - uploading data to BigQuery")
        # TODO delete data first?
        records = []
        for event in self.events_list:
            records.append({
                "user_id": event.user_id,
                "time": str(event.time),
                "type": event.type,
                "computed_for_date": str(self.cur_date),
            })
        df = pandas.DataFrame(
            records,
            columns=["user_id", "time", "type", "computed_for_date"]
        )
        bq_uploader.upload_to_table('events', data_source=df)
