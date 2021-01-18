from __future__ import print_function
import csv
import os
import os.path
import arrow
import json
import math
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


# def make_insert_update_sql(table, primary_keys, keys):
#     concatenated_primary_keys = string.join(primary_keys, ', ')
#     all_keys = string.join(primary_keys + keys, ', ')
#     all_key_placeholders = string.join(['%s'] * (len(primary_keys) + len(keys)), ', ')
#     update_keys = string.join(["{} = EXCLUDED.{}".format(key, key) for key in keys], ', ')
#
#     if len(keys) > 0:
#         return '''INSERT INTO {} ({})
#                         VALUES ({})
#                         ON CONFLICT ({}) DO UPDATE SET {}
#                     '''.format(table, all_keys, all_key_placeholders, concatenated_primary_keys, update_keys)
#     else:
#         return '''INSERT INTO {} ({})
#                         VALUES ({})
#                         ON CONFLICT ({}) DO NOTHING
#                     '''.format(table, all_keys, all_key_placeholders, concatenated_primary_keys)


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

    # def store_in_db(self, conn, cur, processed_date):
    #     print("Deleting data for date " + str(processed_date))
    #
    #     tables_to_del = [
    #         'aggregated_user_days',
    #         'aggregated_user_days_tags',
    #         'aggregated_user_days_categories',
    #         'aggregated_user_days_referer_mediums'
    #     ]
    #
    #     for t in tables_to_del:
    #         cur.execute('DELETE FROM ' + t + ' WHERE date = %s', (processed_date,))
    #         conn.commit()
    #
    #     print("Storing data for date " + str(processed_date))
    #
    #     self.__save_to_aggregated_user_days(conn, cur, processed_date)
    #     self.__save_to_aggregated_user_days_tags(conn, cur, processed_date)
    #     self.__save_to_aggregated_user_days_categories(conn, cur, processed_date)
    #     self.__save_to_aggregated_user_days_referer_mediums(conn, cur, processed_date)

    # def __save_to_aggregated_user_days(self, conn, cur, processed_date):
    #     accessors = aggregated_pageviews_row_accessors({
    #         'browser_ids': lambda d: list(d['browser_ids'])
    #     })
    #
    #     ordered_accessors = OrderedDict([(key, accessors[key]) for key in accessors])
    #     sql = make_insert_update_sql('aggregated_user_days', ['date', 'user_id'], list(ordered_accessors.keys()))
    #
    #     data_to_insert = []
    #     for user_id, user_data in self.data.items():
    #         computed_values = tuple([func(user_data) for key, func in ordered_accessors.items()])
    #         data_to_insert.append((processed_date, user_id) + computed_values)
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()
    #
    # def __save_to_aggregated_user_days_tags(self, conn, cur, processed_date):
    #     sql = make_insert_update_sql('aggregated_user_days_tags', ['date', 'user_id', 'tags'], ['pageviews'])
    #
    #     data_to_insert = []
    #     for user_id, user_data in self.data.items():
    #         for key in user_data['article_tags_pageviews']:
    #             data_to_insert.append((processed_date, user_id, key, user_data['article_tags_pageviews'][key]))
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()
    #
    # def __save_to_aggregated_user_days_categories(self, conn, cur, processed_date):
    #     sql = make_insert_update_sql('aggregated_user_days_categories', ['date', 'user_id', 'categories'], ['pageviews'])
    #
    #     data_to_insert = []
    #     for user_id, user_data in self.data.items():
    #         for key in user_data['article_categories_pageviews']:
    #             data_to_insert.append((processed_date, user_id, key, user_data['article_categories_pageviews'][key]))
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()
    #
    # def __save_to_aggregated_user_days_referer_mediums(self, conn, cur, processed_date):
    #     sql = make_insert_update_sql('aggregated_user_days_referer_mediums', ['date', 'user_id', 'referer_mediums'], ['pageviews'])
    #
    #     data_to_insert = []
    #     for user_id, user_data in self.data.items():
    #         for key in user_data['referer_mediums_pageviews']:
    #             data_to_insert.append((processed_date, user_id, key, user_data['referer_mediums_pageviews'][key]))
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()


class BrowserParser:
    def __init__(self):
        self.browsers_with_users = {}
        self.data = {}
        self.ua_cache = {}

    def __process_pageviews(self, f):
        print("Processing file: " + f)

        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')
            for row in r:
                # save UA to cache
                # user_agent = unidecode(row['user_agent'].decode("utf8"))
                user_agent = row['user_agent']
                if user_agent not in self.ua_cache:
                    self.ua_cache[user_agent] = ua_parse(user_agent)

                # save all user devices for (user <-> device mapping)
                browser_id = row['browser_id']
                if browser_id not in self.browsers_with_users:
                    self.browsers_with_users[browser_id] = {'user_ids': set()}
                if row['user_id']:
                    self.browsers_with_users[browser_id]['user_ids'].add(row['user_id'])
                self.browsers_with_users[browser_id]['ua'] = self.ua_cache[user_agent]

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
                record['ua'] = self.ua_cache[user_agent]

                self.data[browser_id] = record

    def __process_timespent(self, f):
        print("Processing file: " + f)
        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')
            for row in r:
                browser_id = row['browser_id']
                if browser_id in self.data:
                    self.data[browser_id]['timespent'] += int(row['timespent'])

    def process_files(self, pageviews_file, pageviews_timespent_file):
        self.__process_pageviews(pageviews_file)
        if os.path.isfile(pageviews_timespent_file):
            self.__process_timespent(pageviews_timespent_file)
        else:
            print("Missing pageviews timespent data, skipping (file: " + str(pageviews_timespent_file) + ")")

    # def __save_to_browsers_and_browser_users(self, conn, cur, processed_date):
    #     accessors = {
    #         'browser_family': lambda d: d['ua'].browser.family,
    #         'browser_version': lambda d: d['ua'].browser.version_string,
    #         'os_family': lambda d: d['ua'].os.family,
    #         'os_version': lambda d: d['ua'].os.version_string,
    #         'device_family': lambda d: d['ua'].device.family,
    #         'device_brand': lambda d: d['ua'].device.brand,
    #         'device_model': lambda d: d['ua'].device.model,
    #         'is_desktop': lambda d: d['ua'].is_pc,
    #         'is_tablet': lambda d: d['ua'].is_tablet,
    #         'is_mobile': lambda d: d['ua'].is_mobile,
    #     }
    #
    #     ordered_accessors = OrderedDict([(key, accessors[key]) for key in accessors])
    #     sql_browsers = make_insert_update_sql('browsers', ['date', 'browser_id'], list(ordered_accessors.keys()))
    #     sql_browser_users = make_insert_update_sql('browser_users', ['date', 'browser_id', 'user_id'], [])
    #
    #     browsers_to_insert = []
    #     browser_users_to_insert = []
    #     for browser_id, browser_data in self.browsers_with_users.items():
    #         computed_values = tuple([func(browser_data) for key, func in ordered_accessors.items()])
    #         browsers_to_insert.append((processed_date, browser_id) + computed_values)
    #
    #         for user_id in list(browser_data['user_ids']):
    #             browser_users_to_insert.append((processed_date, browser_id, user_id))
    #
    #     psycopg2.extras.execute_batch(cur, sql_browsers, browsers_to_insert)
    #     conn.commit()
    #
    #     psycopg2.extras.execute_batch(cur, sql_browser_users, browser_users_to_insert)
    #     conn.commit()
    #
    # def __save_to_aggregated_browser_days(self, conn, cur, processed_date):
    #     accessors = aggregated_pageviews_row_accessors({
    #         'browser_family': lambda d: d['ua'].browser.family,
    #         'browser_version': lambda d: d['ua'].browser.version_string,
    #         'os_family': lambda d: d['ua'].os.family,
    #         'os_version': lambda d: d['ua'].os.version_string,
    #         'device_family': lambda d: d['ua'].device.family,
    #         'device_brand': lambda d: d['ua'].device.brand,
    #         'device_model': lambda d: d['ua'].device.model,
    #         'is_desktop': lambda d: d['ua'].is_pc,
    #         'is_tablet': lambda d: d['ua'].is_tablet,
    #         'is_mobile': lambda d: d['ua'].is_mobile,
    #         'user_ids': lambda d: list(d['user_ids'])
    #     })
    #
    #     ordered_accessors = OrderedDict([(key, accessors[key]) for key in accessors])
    #     sql = make_insert_update_sql('aggregated_browser_days', ['date', 'browser_id'], list(ordered_accessors.keys()))
    #
    #     data_to_insert = []
    #     for browser_id, browser_data in self.data.items():
    #         computed_values = tuple([func(browser_data) for key, func in ordered_accessors.items()])
    #         data_to_insert.append((processed_date, browser_id) + computed_values)
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()
    #
    # def __save_to_aggregated_browser_days_tags(self, conn, cur, processed_date):
    #     sql = make_insert_update_sql('aggregated_browser_days_tags', ['date', 'browser_id', 'tags'], ['pageviews'])
    #
    #     data_to_insert = []
    #     for browser_id, browser_data in self.data.items():
    #         for key in browser_data['article_tags_pageviews']:
    #             data_to_insert.append((processed_date, browser_id, key, browser_data['article_tags_pageviews'][key]))
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()
    #
    # def __save_to_aggregated_browser_days_categories(self, conn, cur, processed_date):
    #     sql = make_insert_update_sql('aggregated_browser_days_categories', ['date', 'browser_id', 'categories'], ['pageviews'])
    #
    #     data_to_insert = []
    #     for browser_id, browser_data in self.data.items():
    #         for key in browser_data['article_categories_pageviews']:
    #             data_to_insert.append((processed_date, browser_id, key, browser_data['article_categories_pageviews'][key]))
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()
    #
    # def __save_to_aggregated_browser_days_referer_mediums(self, conn, cur, processed_date):
    #     sql = make_insert_update_sql('aggregated_browser_days_referer_mediums', ['date', 'browser_id', 'referer_mediums'], ['pageviews'])
    #
    #     data_to_insert = []
    #     for browser_id, browser_data in self.data.items():
    #         for key in browser_data['referer_mediums_pageviews']:
    #             data_to_insert.append((processed_date, browser_id, key, browser_data['referer_mediums_pageviews'][key]))
    #
    #     psycopg2.extras.execute_batch(cur, sql, data_to_insert)
    #     conn.commit()
    #
    # def store_in_db(self, conn, cur, processed_date):
    #     print("Deleting data for date " + str(processed_date))
    #
    #     tables_to_del = [
    #         'browsers',
    #         'browser_users',
    #         'aggregated_browser_days',
    #         'aggregated_browser_days_tags',
    #         'aggregated_browser_days_categories',
    #         'aggregated_browser_days_referer_mediums'
    #     ]
    #
    #     for t in tables_to_del:
    #         cur.execute('DELETE FROM ' + t + ' WHERE date = %s', (processed_date,))
    #         conn.commit()
    #
    #     print("Storing data for date " + str(processed_date))
    #
    #     self.__save_to_browsers_and_browser_users(conn, cur, processed_date)
    #     self.__save_to_aggregated_browser_days(conn, cur, processed_date)
    #     self.__save_to_aggregated_browser_days_tags(conn, cur, processed_date)
    #     self.__save_to_aggregated_browser_days_categories(conn, cur, processed_date)
    #     self.__save_to_aggregated_browser_days_referer_mediums(conn, cur, processed_date)
