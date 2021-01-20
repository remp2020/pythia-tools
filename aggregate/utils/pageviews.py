from __future__ import print_function
import csv
import os
import os.path
import arrow
import json
import math
import pandas
from collections import OrderedDict
from user_agents import parse as ua_parse


def add_one(arr, key):
    if key not in arr:
        arr[key] = 0
    arr[key] += 1


def empty_data_entry(data_to_merge=None):
    data = {
        'pageviews': 0,
        'timespent': 0,
        'sessions': set(),
        'sessions_without_ref': set(),
        "referer_mediums_pageviews": {},
        "article_categories_pageviews": {},
        "article_tags_pageviews": {},
        "hour_interval_pageviews": {},
    }

    # each hour has a separate column
    for i in range(24):
        data['pageviews_' + str(i) + 'h'] = 0

    # each 4 hours have a separate column
    for i in range(6):
        h = (int) (i * 4)
        data['pageviews_' + str(h) + 'h_' + str(h + 4) + 'h'] = 0

    if data_to_merge:
        data.update(data_to_merge)

    return data


def update_record_from_pageviews_row(record, row):
    record['pageviews'] += 1
    record['sessions'].add(row['remp_session_id'])

    add_one(record['referer_mediums_pageviews'], row['derived_referer_medium'])
    add_one(record['article_categories_pageviews'], row['category'])

    for tag in row['tags'].split(','):
        if tag:
            add_one(record['article_tags_pageviews'], tag)

    # Hour aggregations
    hour = arrow.get(row['time']).to('utc').hour
    hour_string = str(hour).zfill(2)
    add_one(record['hour_interval_pageviews'], hour_string + ':00-' + hour_string + ':59')
    record['pageviews_' + str(hour) + 'h'] += 1

    # 4-hours aggregations
    interval4h = math.floor(hour/4) * 4 # round to 4h interval start
    dict_key = 'pageviews_' + str(interval4h) + 'h_' + str(interval4h + 4) + 'h'
    record[dict_key] += 1

    if row['derived_referer_medium'] == 'direct':
        record['sessions_without_ref'].add(row['remp_session_id'])


def aggregated_pageviews_row_accessors(accessors_to_merge=None):
    accessors = {
        'sessions': lambda d: len(d['sessions']),
        'sessions_without_ref': lambda d: len(d['sessions_without_ref']),
        'referer_mediums_pageviews': lambda d: json.dumps(d['referer_mediums_pageviews']),
        'article_categories_pageviews': lambda d: json.dumps(d['article_categories_pageviews']),
        'article_tags_pageviews': lambda d: json.dumps(d['article_tags_pageviews']),
        'hour_interval_pageviews': lambda d: json.dumps(d['hour_interval_pageviews']),
    }

    # This needs to be a function, see:
    # https://docs.python.org/3/faq/programming.html#why-do-lambdas-defined-in-a-loop-with-different-values-all-return-the-same-result
    def lambda_accessor(name):
        return lambda d: d[name]

    # Parameters directly referenced using lambda
    simple_accessors = ['pageviews', 'timespent'] + \
                       ['pageviews_' + str(i) + 'h' for i in range(24)] + \
                       ['pageviews_' + str(i * 4) + 'h_' + str(i * 4 + 4) + 'h' for i in range(6)]

    for name in simple_accessors:
        accessors.update({name: lambda_accessor(name)})

    if accessors_to_merge:
        accessors.update(accessors_to_merge)

    return accessors


class UserParser:
    def __init__(self):
        self.data = {}

    def __process_pageviews(self, f):
        print("Processing file: " + f)

        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')
            for row in r:
                if row['user_id'] == '' or row['subscriber'] != 'True':
                    continue

                if row['derived_referer_medium'] == '':
                    continue

                user_id = row['user_id']
                if user_id not in self.data:
                    self.data[user_id] = empty_data_entry({
                        'browser_ids': set()
                    })

                # Retrieve record
                record = self.data[user_id]
                update_record_from_pageviews_row(record, row)

                if row['browser_id']:
                    record['browser_ids'].add(row['browser_id'])

                # Save record
                self.data[user_id] = record

    def __process_timespent(self, f):
        print("Processing file: " + f)
        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')
            for row in r:
                user_id = row['user_id']
                if user_id != '' and user_id in self.data:
                    self.data[user_id]['timespent'] += int(row['timespent'])

    def process_files(self, pageviews_file, pageviews_timespent_file):
        self.__process_pageviews(pageviews_file)
        if os.path.isfile(pageviews_timespent_file):
            self.__process_timespent(pageviews_timespent_file)
        else:
            print("Missing pageviews timespent data, skipping (file: " + str(pageviews_timespent_file) + ")")

    def upload_to_bq(self, bq_uploader, processed_date):
        print("UserParser - uploading data to BigQuery")
        # TODO: delete data first?
        self.__save_to_aggregated_user_days(bq_uploader, processed_date)
        self.__save_to_aggregated_user_days_tags(bq_uploader, processed_date)
        self.__save_to_aggregated_user_days_categories(bq_uploader, processed_date)
        self.__save_to_aggregated_user_days_referer_mediums(bq_uploader, processed_date)

    def __save_to_aggregated_user_days(self, bq_uploader, processed_date):
        accessors = aggregated_pageviews_row_accessors({
            'browser_ids': lambda d: list(d['browser_ids'])
        })
        ordered_accessors = OrderedDict([(key, accessors[key]) for key in accessors])
        records = []
        for user_id, user_data in self.data.items():
            row = {
                "date": str(processed_date),
                "user_id": user_id,
            }
            for key, func in ordered_accessors.items():
                row[key] = func(user_data)
            records.append(row)

        df = pandas.DataFrame(
            records,
            columns=["date", "user_id"] + list(ordered_accessors.keys())
        )
        bq_uploader.upload_to_table('aggregated_user_days', data_source=df)

    def __save_to_aggregated_user_days_tags(self, bq_uploader, processed_date):
        records = []
        for user_id, user_data in self.data.items():
            for key in user_data['article_tags_pageviews']:
                records.append({
                    "date": str(processed_date),
                    "user_id": user_id,
                    "tags": key,
                    "pageviews": user_data['article_tags_pageviews'][key]
                })
        df = pandas.DataFrame(
            records,
            columns=["date", "user_id", "tags", "pageviews"]
        )
        bq_uploader.upload_to_table('aggregated_user_days_tags', data_source=df)

    def __save_to_aggregated_user_days_categories(self, bq_uploader, processed_date):
        records = []
        for user_id, user_data in self.data.items():
            for key in user_data['article_categories_pageviews']:
                records.append({
                    "date": str(processed_date),
                    "user_id": user_id,
                    "categories": key,
                    "pageviews": user_data['article_categories_pageviews'][key]
                })
        df = pandas.DataFrame(
            records,
            columns=["date", "user_id", "categories", "pageviews"]
        )
        bq_uploader.upload_to_table('aggregated_user_days_categories', data_source=df)

    def __save_to_aggregated_user_days_referer_mediums(self, bq_uploader, processed_date):
        records = []
        for user_id, user_data in self.data.items():
            for key in user_data['referer_mediums_pageviews']:
                records.append({
                    "date": str(processed_date),
                    "user_id": user_id,
                    "referer_mediums": key,
                    "pageviews": user_data['referer_mediums_pageviews'][key]
                })
        df = pandas.DataFrame(
            records,
            columns=["date", "user_id", "referer_mediums", "pageviews"]
        )
        bq_uploader.upload_to_table('aggregated_user_days_referer_mediums', data_source=df)


class BrowserParser:
    def __init__(self):
        self.browsers_with_users = {}
        self.browser_commerce_steps = {}
        self.data = {}

    def __process_commerce(self, commerce_file):
        print("BrowserParser - processing commerce data from: " + commerce_file)
        with open(commerce_file) as csv_file:
            r = csv.DictReader(csv_file, delimiter=',')
            for row in r:
                if row['browser_id']:
                    if row['browser_id'] not in self.browser_commerce_steps:
                        self.browser_commerce_steps[row['browser_id']] = {
                            'checkout': 0,
                            'payment': 0,
                            'purchase': 0,
                            'refund': 0
                        }

                    if row['step'] not in ['checkout', 'payment', 'purchase', 'refund']:
                        raise Exception(
                            "unknown commerce step: " + row['step'] + ' for browser_id: ' + row['browser_id'])
                    else:
                        self.browser_commerce_steps[row['browser_id']][row['step']] += 1

    def __process_pageviews(self, f):
        print("BrowserParser - processing pageviews from: " + f)

        ua_cache = {}
        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')

            for row in r:
                # save UA to cache
                user_agent = row['user_agent']
                if user_agent not in ua_cache:
                    ua_cache[user_agent] = ua_parse(user_agent)

                # save all user devices for (user <-> device mapping)
                browser_id = row['browser_id']
                if browser_id not in self.browsers_with_users:
                    self.browsers_with_users[browser_id] = {'user_ids': set()}
                if row['user_id']:
                    self.browsers_with_users[browser_id]['user_ids'].add(row['user_id'])
                self.browsers_with_users[browser_id]['ua'] = ua_cache[user_agent]

                # continue with pageviews only for subscribers
                if row['subscriber'] == 'True':
                    continue

                if row['derived_referer_medium'] == '':
                    continue

                if browser_id not in self.data:
                    self.data[browser_id] = empty_data_entry({
                        'user_ids': set()
                    })

                record = self.data[browser_id]
                update_record_from_pageviews_row(record, row)

                if row['user_id']:
                    record['user_ids'].add(row['user_id'])
                record['ua'] = ua_cache[user_agent]

                self.data[browser_id] = record


    def __process_timespent(self, f):
        print("BrowserParser - processing timespent data from: " + f)
        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')
            for row in r:
                browser_id = row['browser_id']
                if browser_id in self.data:
                    self.data[browser_id]['timespent'] += int(row['timespent'])

    def process_files(self, pageviews_file, pageviews_timespent_file, commerce_file):
        self.__process_pageviews(pageviews_file)
        self.__process_commerce(commerce_file)
        if os.path.isfile(pageviews_timespent_file):
            self.__process_timespent(pageviews_timespent_file)
        else:
            print("Missing pageviews timespent data, skipping (file: " + str(pageviews_timespent_file) + ")")

    def __save_to_browsers_and_browser_users(self, bq_uploader, processed_date):
        accessors = {
            'browser_family': lambda d: d['ua'].browser.family,
            'browser_version': lambda d: d['ua'].browser.version_string,
            'os_family': lambda d: d['ua'].os.family,
            'os_version': lambda d: d['ua'].os.version_string,
            'device_family': lambda d: d['ua'].device.family,
            'device_brand': lambda d: d['ua'].device.brand,
            'device_model': lambda d: d['ua'].device.model,
            'is_desktop': lambda d: str(d['ua'].is_pc), # Uploader expects bools as strings
            'is_tablet': lambda d: str(d['ua'].is_tablet),
            'is_mobile': lambda d: str(d['ua'].is_mobile),
        }
        ordered_accessors = OrderedDict(accessors.items())

        browsers_records = []
        browser_users_records = []
        for browser_id, browser_data in self.browsers_with_users.items():
            row = {
                "date": str(processed_date),
                "browser_id": browser_id,
            }
            for key, func in ordered_accessors.items():
                row[key] = func(browser_data)
            browsers_records.append(row)

            for user_id in list(browser_data['user_ids']):
                browser_users_records.append({
                    "date": str(processed_date),
                    "browser_id": browser_id,
                    "user_id": user_id
                })

        # 'browser' table
        df = pandas.DataFrame(
            browsers_records,
            columns=["date", "browser_id"] + list(ordered_accessors.keys())
        )
        bq_uploader.upload_to_table('browsers', data_source=df)

        # 'browser_users' table
        df = pandas.DataFrame(
            browser_users_records,
            columns=["date", "browser_id", "user_id"]
        )
        bq_uploader.upload_to_table('browser_users', data_source=df)

    def __save_to_aggregated_browser_days(self, bq_uploader, processed_date):
        accessors = aggregated_pageviews_row_accessors({
            'browser_family': lambda d: d['ua'].browser.family,
            'browser_version': lambda d: d['ua'].browser.version_string,
            'os_family': lambda d: d['ua'].os.family,
            'os_version': lambda d: d['ua'].os.version_string,
            'device_family': lambda d: d['ua'].device.family,
            'device_brand': lambda d: d['ua'].device.brand,
            'device_model': lambda d: d['ua'].device.model,
            'is_desktop': lambda d: str(d['ua'].is_pc),
            'is_tablet': lambda d: str(d['ua'].is_tablet),
            'is_mobile': lambda d: str(d['ua'].is_mobile),
            'user_ids': lambda d: list(d['user_ids']),
        })

        ordered_accessors = OrderedDict(accessors.items())

        records = []
        for browser_id, browser_data in self.data.items():
            row = {
                "date": str(processed_date),
                "browser_id": browser_id,
            }
            for key, func in ordered_accessors.items():
                row[key] = func(browser_data)

            # commerce data
            if browser_id in self.browser_commerce_steps:
                row["commerce_checkouts"] = self.browser_commerce_steps[browser_id]['checkout']
                row["commerce_payments"] = self.browser_commerce_steps[browser_id]['payment']
                row["commerce_purchases"] = self.browser_commerce_steps[browser_id]['purchase']
                row["commerce_refunds"] = self.browser_commerce_steps[browser_id]['refund']
            else:
                row["commerce_checkouts"] = 0
                row["commerce_payments"] = 0
                row["commerce_purchases"] = 0
                row["commerce_refunds"] = 0

            records.append(row)

        df = pandas.DataFrame(
            records,
            columns=["date", "browser_id"] + list(ordered_accessors.keys()) + ["commerce_checkouts", "commerce_payments", "commerce_purchases", "commerce_refunds"]
        )
        bq_uploader.upload_to_table('aggregated_browser_days', data_source=df)

    def __save_to_aggregated_browser_days_tags(self, bq_uploader, processed_date):
        records = []
        for browser_id, browser_data in self.data.items():
            for key in browser_data['article_tags_pageviews']:
                records.append({
                    "date": str(processed_date),
                    "browser_id": browser_id,
                    "tags": key,
                    "pageviews": browser_data['article_tags_pageviews'][key]
                })

        df = pandas.DataFrame(
            records,
            columns=["date", "browser_id", "tags", "pageviews"]
        )
        bq_uploader.upload_to_table('aggregated_browser_days_tags', data_source=df)

    def __save_to_aggregated_browser_days_categories(self, bq_uploader, processed_date):
        records = []
        for browser_id, browser_data in self.data.items():
            for key in browser_data['article_categories_pageviews']:
                records.append({
                    "date": str(processed_date),
                    "browser_id": browser_id,
                    "categories": key,
                    "pageviews": browser_data['article_categories_pageviews'][key],
                })

        df = pandas.DataFrame(
            records,
            columns=["date", "browser_id", "categories", "pageviews"]
        )
        bq_uploader.upload_to_table('aggregated_browser_days_categories', data_source=df)

    def __save_to_aggregated_browser_days_referer_mediums(self, bq_uploader, processed_date):
        records = []
        for browser_id, browser_data in self.data.items():
            for key in browser_data['referer_mediums_pageviews']:
                records.append(
                    {
                        "date": str(processed_date),
                        "browser_id": browser_id,
                        "referer_mediums": key,
                        "pageviews": browser_data['referer_mediums_pageviews'][key],
                    }
                )
        df = pandas.DataFrame(
            records,
            columns=["date", "browser_id", "referer_mediums", "pageviews"]
        )
        bq_uploader.upload_to_table('aggregated_browser_days_referer_mediums', data_source=df)

    def upload_to_bq(self, bq_uploader, processed_date):
        print("BrowserParser - uploading data to BigQuery")

        # TODO: delete data first?

        self.__save_to_browsers_and_browser_users(bq_uploader, processed_date)
        self.__save_to_aggregated_browser_days(bq_uploader, processed_date)
        self.__save_to_aggregated_browser_days_tags(bq_uploader, processed_date)
        self.__save_to_aggregated_browser_days_categories(bq_uploader, processed_date)
        self.__save_to_aggregated_browser_days_referer_mediums(bq_uploader, processed_date)
