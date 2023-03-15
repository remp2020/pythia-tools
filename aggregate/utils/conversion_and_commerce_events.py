from __future__ import print_function
import csv
import arrow
import pandas


class Commerce:
    def __init__(self, row):
        self.browser_id = row['browser_id']
        self.time = row['time']
        self.user_id = row['user_id']
        self.step = row['step']

    def __str__(self):
        return "[" + self.step + "|" + self.browser_id + "|" + self.time + "|" + self.user_id + "]"

    def __repr__(self):
        return self.__str__()


class CommerceParser:
    def __init__(self):
        self.user_id_payment_time = {}
        self.user_id_browser_id = {}
        self.data = []
        self.events_to_save = []
        pass

    def __load_data(self, commerce_file, csv_delimiter):
        with open(commerce_file) as csv_file:
            r = csv.DictReader(csv_file, delimiter=csv_delimiter)
            for row in r:
                self.data.append(Commerce(row))

    def process_file(self, commerce_file, csv_delimiter):
        # TODO: rely on `commerce_session_id` instead of `browser_id` to identify commerce session
        print("Processing file: " + commerce_file)
        self.__load_data(commerce_file, csv_delimiter)
        self.data.sort(key=lambda x: x.time)

        for c in self.data:
            if c.step == 'payment' and c.browser_id and c.user_id:
                self.user_id_payment_time[c.user_id] = arrow.get(c.time)
                self.user_id_browser_id[c.user_id] = c.browser_id
            elif c.step == 'purchase' and c.user_id:
                purchase_time = arrow.get(c.time)
                purchase_time_minus_5 = purchase_time.shift(minutes=-5)

                if c.user_id not in self.user_id_payment_time:
                    continue

                payment_time = self.user_id_payment_time[c.user_id]

                # purchase event within 5 minutes of payment
                if purchase_time_minus_5 <= payment_time:
                    browser_id = self.user_id_browser_id[c.user_id]
                    self.events_to_save.append({
                        "user_id": c.user_id,
                        "browser_id": browser_id,
                        "time": c.time,
                        "type": "conversion",
                    })

    def upload_to_bq(self, bq_uploader, processed_date):
        print("CommerceParser - uploading data to BigQuery")
        # TODO delete data?
        records = []
        for event in self.events_to_save:
            records.append({
                "user_id": event["user_id"],
                "browser_id": event["browser_id"],
                "time": str(event["time"]),
                "type": event["type"],
                "computed_for_date": str(processed_date),
            })
        df = pandas.DataFrame(
            records,
            columns=["user_id", "browser_id", "time", "type", "computed_for_date"]
        )
        bq_uploader.upload_to_table('events', data_source=df)


class PageView:
    def __init__(self, row):
        self.browser_id = row['browser_id']
        self.user_id = row['user_id']
        self.time = row['time']
        self.subscriber = row['subscriber'].lower() == 'true'

    def __str__(self):
        return "[" + self.time + "|" + self.browser_id + "|" + self.user_id + "|" + str(self.subscriber) + "]"

    def __repr__(self):
        return self.__str__()


class SharedLoginParser:
    def __init__(self):
        self.data = []
        self.not_logged_in_browsers = set()
        self.logged_in_browsers = set()
        self.logged_in_browsers_time = {}
        self.browser_user_id = {}

    def __load_data(self, f, csv_delimiter):
        with open(f) as csv_file:
            r = csv.DictReader(csv_file, delimiter=csv_delimiter)
            for row in r:
                self.data.append(PageView(row))

    def __find_login_events(self):
        for p in self.data:
            if not p.subscriber and not p.user_id:
                self.not_logged_in_browsers.add(p.browser_id)
            elif p.subscriber and p.user_id:
                # this represents an event where user has logged in that particular day
                logged_in_time = arrow.get(p.time)
                if p.browser_id in self.not_logged_in_browsers:
                    self.logged_in_browsers.add(p.browser_id)
                    self.logged_in_browsers_time[p.browser_id] = logged_in_time
                else:
                    # correct earlier timestamp event
                    if (p.browser_id in self.logged_in_browsers_time and logged_in_time < self.logged_in_browsers_time[p.browser_id]) or p.browser_id not in self.logged_in_browsers_time:
                        self.logged_in_browsers_time[p.browser_id] = logged_in_time
                self.browser_user_id[p.browser_id] = p.user_id

    def upload_to_bq(self, bq_uploader, processed_date):
        print("SharedLoginParser - uploading data to BigQuery")
        # TODO delete data?
        records = []
        for browser_id in self.logged_in_browsers:
            records.append({
                "user_id": self.browser_user_id[browser_id],
                "browser_id": browser_id,
                "time": self.logged_in_browsers_time[browser_id].isoformat(),
                "type": "shared_account_login",
                "computed_for_date": str(processed_date),
            })
        df = pandas.DataFrame(
            records,
            columns=["user_id", "browser_id", "time", "type", "computed_for_date"]
        )
        bq_uploader.upload_to_table('events', data_source=df)

    def process_file(self, pageviews_file, csv_delimiter):
        print("Processing file: " + pageviews_file)
        self.__load_data(pageviews_file, csv_delimiter)
        self.data.sort(key=lambda x: x.time)
        self.__find_login_events()
