from __future__ import print_function
import csv
import sys
import os.path
from user_agents import parse as ua_parse
import re
from datetime import date
import psycopg2
import psycopg2.extras
from utils import load_env, create_con, migrate

BASE_PATH = os.path.dirname(os.path.realpath(__file__))

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
            }

    def parse_user_agent(self, browser_id, user_agent):
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

                self.browser_id_mapping[row['remp_pageview_id']] = row['browser_id']

                self.create_record(row['browser_id'])
                self.data[row['browser_id']]['pageviews'] += 1
                self.data[row['browser_id']]['sessions'].add(row['remp_session_id'])
                if row['ref_source'] == 'direct':
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

    def process_files(self, pageviews_file, pageviews_timespent_files):
        self.process_pageviews(pageviews_file)
        if os.path.isfile(pageviews_timespent_files):
            self.process_pageviews_timespent(pageviews_timespent_files)

    def store_in_db(self, conn, cur, processed_date):
        print("Storing data for date " + str(processed_date))

        sql = '''INSERT INTO aggregated_browser_days (date, browser_id, pageviews, timespent, 
        sessions, sessions_without_ref, browser_family, browser_version, os_family, os_version,
        device_family, device_brand, device_model, is_desktop, is_tablet, is_mobile) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s) 
        ON CONFLICT (date, browser_id) DO UPDATE SET 
        pageviews = EXCLUDED.pageviews,
        timespent = EXCLUDED.timespent,
        sessions = EXCLUDED.sessions, 
        sessions_without_ref = EXCLUDED.sessions_without_ref,
        browser_family = EXCLUDED.browser_family,
        browser_version = EXCLUDED.browser_version,
        os_family = EXCLUDED.os_family,
        os_version = EXCLUDED.os_version,
        device_family = EXCLUDED.device_family,
        device_brand = EXCLUDED.device_brand,
        device_model = EXCLUDED.device_model,
        is_desktop = EXCLUDED.is_desktop,
        is_tablet = EXCLUDED.is_tablet,
        is_mobile = EXCLUDED.is_mobile
        '''
        psycopg2.extras.execute_batch(cur, sql, [(
            processed_date,
            browser_id,
            browser_data['pageviews'],
            browser_data['timespent'],
            len(browser_data['sessions']),
            len(browser_data['sessions_without_ref']),

            browser_data['ua'].browser.family,
            browser_data['ua'].browser.version_string,
            browser_data['ua'].os.family,
            browser_data['ua'].os.version_string,
            browser_data['ua'].device.family,
            browser_data['ua'].device.brand,
            browser_data['ua'].device.model,

            browser_data['ua'].is_pc,
            browser_data['ua'].is_tablet,
            browser_data['ua'].is_mobile,
        ) for browser_id, browser_data in self.data.items()
        ])


def run(file_date):
    env_vars = load_env()
    pageviews_file = BASE_PATH + "/pageviews_" + file_date + ".csv"

    if not os.path.isfile(pageviews_file):
        print("Error: file " + pageviews_file + " does not exist")
        return

    conn, cur = create_con(env_vars['POSTGRES_USER'], env_vars['POSTGRES_PASS'], env_vars['POSTGRES_DB'], env_vars['POSTGRES_HOST'])
    migrate(cur)
    conn.commit()

    parser = Parser()

    m = pattern.search(pageviews_file)
    path_str = m.group(1)
    date_str = m.group(2)
    pageviews_time_spent_file = path_str + "pageviews_time_spent_" + date_str + ".csv"

    parser.process_files(pageviews_file, pageviews_time_spent_file)

    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    parser.store_in_db(conn, cur, date(year, month, day))

    conn.commit()
    cur.close()
    conn.close()


def usage():
    print("Script to parse elastic CSV export, process it and insert into PostgreSQL")
    print("usage: ./" + sys.argv[0] + " <date>")


def main(argv):
    if len(argv) == 0:
        usage()
    else:
        run(argv[0])

if __name__ == '__main__':
    main(sys.argv[1:])



