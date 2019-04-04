from __future__ import print_function
import csv
import os
import os.path
import re
import psycopg2
import psycopg2.extras
import arrow
import string
import json
import argparse
from unidecode import unidecode
from datetime import date
from user_agents import parse as ua_parse
from utils import load_env, create_con, migrate
from collections import OrderedDict

BASE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

pattern = re.compile("(.*)pageviews_([0-9]+).csv")
ua_cache = {}


class Parser:
    def __init__(self):
        self.data = {}
        self.browser_id_mapping = {}
        pass

    def create_record(self, browser_id):
        if browser_id not in self.data:
            self.data[browser_id] = {
                'pageviews': 0,
                'timespent': 0,
                'sessions': set(),
                'sessions_without_ref': set(),
                'user_ids': set(),

                # pageviews per:
                "referer_medium_pageviews": {},
                "article_category_pageviews": {},
                "hour_interval_pageviews": {},
            }

    def parse_user_agent(self, browser_id, user_agent):
        user_agent = unidecode(user_agent.decode("utf8"))
        if user_agent not in ua_cache:
            ua_cache[user_agent] = ua_parse(user_agent)
        self.data[browser_id]['ua'] = ua_cache[user_agent]

    def process_pageviews(self, f):
        print("Processing file: " + f)

        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')
            for row in r:
                if row['subscriber'] == 'True':
                    continue

                if row['derived_referer_medium'] == '':
                    continue

                self.browser_id_mapping[row['remp_pageview_id']] = row['browser_id']
                self.create_record(row['browser_id'])
                self.data[row['browser_id']]['pageviews'] += 1
                self.data[row['browser_id']]['sessions'].add(row['remp_session_id'])

                add_one(self.data[row['browser_id']]['referer_medium_pageviews'], row['derived_referer_medium'])
                add_one(self.data[row['browser_id']]['article_category_pageviews'], row['category'])
                hour = str(arrow.get(row['time']).to('utc').hour).zfill(2)
                add_one(self.data[row['browser_id']]['hour_interval_pageviews'], hour + ":00-" + hour + ":59")

                if row['user_id']:
                    self.data[row['browser_id']]['user_ids'].add(row['user_id'])

                if row['derived_referer_medium'] == 'direct':
                    self.data[row['browser_id']]['sessions_without_ref'].add(row['remp_session_id'])

                self.parse_user_agent(row['browser_id'], row['user_agent'])

    def process_pageviews_timespent(self, f):
        print("Processing file: " + f)
        with open(f) as csvfile:
            r = csv.DictReader(csvfile, delimiter=',')
            for row in r:
                if 'browser_id' in row:
                    browser_id = row['browser_id']
                else:
                    # older data format doesn't store browser_id in pageviews table
                    # therefore we remember mapping of pageview_id <-> browser_id and get browser_id from that
                    if row['remp_pageview_id'] not in self.browser_id_mapping:
                        continue
                    browser_id = self.browser_id_mapping[row['remp_pageview_id']]

                if browser_id not in self.data:
                    # sometimes pageview data may not be stored for timespent record, ignore these records
                    continue

                # in newer data format sum was renamed to timespent
                if 'timespent' in row:
                    self.data[browser_id]['timespent'] += int(row['timespent'])
                else:
                    self.data[browser_id]['timespent'] += int(row['sum'])

    def process_files(self, pageviews_file, pageviews_timespent_file):
        self.process_pageviews(pageviews_file)
        if os.path.isfile(pageviews_timespent_file):
            self.process_pageviews_timespent(pageviews_timespent_file)
        else:
            print("Missing pageviews timespent data, skipping (file: " + str(pageviews_timespent_file) + ")")

    def store_in_db(self, conn, cur, processed_date):
        print("Deleting data for date " + str(processed_date))

        cur.execute('DELETE FROM aggregated_browser_days WHERE date = %s', (processed_date,))
        conn.commit()

        print("Storing data for date " + str(processed_date))

        params = {
            'pageviews': lambda d: d['pageviews'],
            'timespent': lambda d: d['timespent'],
            'sessions': lambda d: len(d['sessions']),
            'sessions_without_ref': lambda d: len(d['sessions_without_ref']),

            'browser_family': lambda d: d['ua'].browser.family,
            'browser_version': lambda d: d['ua'].browser.version_string,
            'os_family': lambda d: d['ua'].os.family,
            'os_version': lambda d: d['ua'].os.version_string,
            'device_family': lambda d: d['ua'].device.family,
            'device_brand': lambda d: d['ua'].device.brand,
            'device_model': lambda d: d['ua'].device.model,
            'is_desktop': lambda d: d['ua'].is_pc,
            'is_tablet': lambda d: d['ua'].is_tablet,
            'is_mobile': lambda d: d['ua'].is_mobile,

            'user_ids': lambda d: list(d['user_ids']),

            'referer_medium_pageviews': lambda d: json.dumps(d['referer_medium_pageviews']),
            'article_category_pageviews': lambda d: json.dumps(d['article_category_pageviews']),
            'hour_interval_pageviews': lambda d: json.dumps(d['hour_interval_pageviews']),
        }

        # Create SQL
        ordered_params = OrderedDict([(key, params[key]) for key in params])
        keys = list(ordered_params.keys())

        concatenated_keys = string.join(keys, ', ')
        key_placeholders = string.join(['%s'] * len(keys), ', ')
        update_keys = string.join(["{} = EXCLUDED.{}".format(key, key) for key in keys], ', ')

        sql = '''INSERT INTO aggregated_browser_days (date, browser_id, {}) 
        VALUES (%s, %s, {}) 
        ON CONFLICT (date, browser_id) DO UPDATE SET {}
        '''.format(concatenated_keys, key_placeholders, update_keys)

        # Compute values
        data_to_insert = []
        for browser_id, browser_data in self.data.items():
            computed_values = tuple([func(browser_data) for key, func in ordered_params.items()])
            data_to_insert.append((processed_date, browser_id) + computed_values)

        # Insert in batch
        psycopg2.extras.execute_batch(cur, sql, data_to_insert)


def run(file_date, aggregate_folder):
    load_env()
    pageviews_file = os.path.join(aggregate_folder, "pageviews", "pageviews_" + file_date + ".csv")
    pageviews_time_spent_file = os.path.join(aggregate_folder, "pageviews_time_spent", "pageviews_time_spent_" + file_date + ".csv")

    if not os.path.isfile(pageviews_file):
        print("Error: file " + pageviews_file + " does not exist")
        return

    conn, cur = create_con(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASS"), os.getenv("POSTGRES_DB"), os.getenv("POSTGRES_HOST"))
    migrate(cur)
    conn.commit()

    parser = Parser()

    m = pattern.search(pageviews_file)
    date_str = m.group(2)

    parser.process_files(pageviews_file, pageviews_time_spent_file)

    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    parser.store_in_db(conn, cur, date(year, month, day))

    conn.commit()
    cur.close()
    conn.close()


def add_one(where, category):
    if category not in where:
        where[category] = 0
    where[category] += 1


def main():
    parser = argparse.ArgumentParser(description='Script to parse elastic CSV export, process it and insert into PostgreSQL')
    parser.add_argument('date', metavar='date', help='Aggregate date, format YYYYMMDD')
    parser.add_argument('--dir', metavar='AGGREGATE_DIRECTORY', dest='dir', default=BASE_PATH, help='where to look for aggregated CSV files')

    args = parser.parse_args()
    run(args.date, args.dir)

if __name__ == '__main__':
    main()

