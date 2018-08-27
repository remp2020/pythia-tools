from __future__ import print_function
import csv
import sys
import psycopg2
import psycopg2.extras
import os.path
import arrow
from datetime import date
from utils import load_env, create_con, migrate

BASE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

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
    def __init__(self, cur_date, cursor):
        self.user_id_payment_time = {}
        self.user_id_browser_id = {}
        self.data = []
        self.cur_date = cur_date
        self.cursor = cursor
        pass

    def __load_data(self, commerce_file):
        with open(commerce_file) as csv_file:
            r = csv.DictReader(csv_file, delimiter=',')
            for row in r:
                self.data.append(Commerce(row))

    def process_file(self, commerce_file):
        print("Processing file: " + commerce_file)
        self.__load_data(commerce_file)
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

                # purchase event too far from payment event
                if purchase_time_minus_5 <= payment_time:
                    browser_id = self.user_id_browser_id[c.user_id]
                    self.__mark_conversion_event(browser_id, purchase_time)

    def __mark_conversion_event(self, browser_id, purchase_time):
        # first delete that particular day
        # we don't want conversion day to be included in aggregated data
        self.cursor.execute('''
        DELETE FROM aggregated_browser_days WHERE browser_id = %s and date = %s
        ''', (browser_id, self.cur_date))

        # then mark 7_days_event to 'conversion'
        # for 7 previous days
        end = arrow.get(self.cur_date).shift(days=-1)
        start = end.shift(days=-6)
        sql = '''
        UPDATE aggregated_browser_days 
        SET next_7_days_event = 'conversion', next_event_time = %s
        WHERE date = %s AND browser_id = %s AND next_7_days_event = 'no_conversion'
        '''
        psycopg2.extras.execute_batch(self.cursor, sql, [
            (purchase_time.isoformat(), day[0].date(), browser_id) for day in arrow.Arrow.span_range('day', start, end)
        ])


class PageView:
    def __init__(self, row):
        self.browser_id = row['browser_id']
        self.user_id = row['user_id']
        self.time = row['time']
        self.subscriber = row['subscriber'] == 'True'

    def __str__(self):
        return "[" + self.time + "|" + self.browser_id + "|" + self.user_id + "|" + str(self.subscriber) + "]"

    def __repr__(self):
        return self.__str__()


class PageViewsParser:
    def __init__(self, cur_date, cursor):
        self.data = []
        self.not_logged_in_browsers = set()
        self.logged_in_browsers = set()
        self.logged_in_browsers_time = {}
        self.cur_date = cur_date
        self.cursor = cursor
        pass

    def __load_data(self, f):
        with open(f) as csv_file:
            r = csv.DictReader(csv_file, delimiter=',')
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

    def __save_in_db(self):
        # first delete that particular day
        for browser_id in self.logged_in_browsers:
            self.cursor.execute('''
            DELETE FROM aggregated_browser_days WHERE browser_id = %s and date = %s
            ''', (browser_id, self.cur_date))

        # then mark 7_days_event
        end = arrow.get(self.cur_date).shift(days=-1)
        start = end.shift(days=-6)

        sql = '''
        UPDATE aggregated_browser_days 
        SET next_7_days_event = 'shared_account_login', next_event_time = %s
        WHERE date = %s AND browser_id = %s AND next_7_days_event = 'no_conversion'
        '''
        psycopg2.extras.execute_batch(self.cursor, sql, [
            (self.logged_in_browsers_time[browser_id].isoformat(), day[0].date(), browser_id)
            for day in arrow.Arrow.span_range('day', start, end)
            for browser_id in self.logged_in_browsers
        ])

    def process_file(self, pageviews_file):
        print("Processing file: " + pageviews_file)
        self.__load_data(pageviews_file)
        self.data.sort(key=lambda x: x.time)
        self.__find_login_events()
        self.__save_in_db()


def run(file_date):
    load_env()
    commerce_file = BASE_PATH + "/commerce_" + file_date + ".csv"
    pageviews_file = BASE_PATH + "/pageviews_" + file_date + ".csv"

    if not os.path.isfile(commerce_file):
        print("Error: file " + commerce_file + " does not exist")
        return

    if not os.path.isfile(pageviews_file):
        print("Error: file " + pageviews_file + " does not exist")
        return

    year = int(file_date[0:4])
    month = int(file_date[4:6])
    day = int(file_date[6:8])
    cur_date = date(year, month, day)

    conn, cur = create_con(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASS"), os.getenv("POSTGRES_DB"),os.getenv("POSTGRES_HOST"))
    migrate(cur)
    conn.commit()

    commerce_parser = CommerceParser(cur_date, cur)
    commerce_parser.process_file(commerce_file)
    conn.commit()

    pageviews_parser= PageViewsParser(cur_date, cur)
    pageviews_parser.process_file(pageviews_file)
    conn.commit()

    cur.close()
    conn.close()


def usage():
    print("Script to process future events commerce data")
    print("usage: ./" + sys.argv[0] + " <date>")


def main(argv):
    if len(argv) == 0:
        usage()
    else:
        run(argv[0])

if __name__ == '__main__':
    main(sys.argv[1:])