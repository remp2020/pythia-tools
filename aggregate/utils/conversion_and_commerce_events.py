from __future__ import print_function
import csv
import arrow

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
    def __init__(self, cur_date):
        self.user_id_payment_time = {}
        self.user_id_browser_id = {}
        self.data = []
        self.cur_date = cur_date
        self.browser_steps = {}
        self.events_to_save = []
        pass

    def __load_data(self, commerce_file):
        with open(commerce_file) as csv_file:
            r = csv.DictReader(csv_file, delimiter=',')
            for row in r:
                self.data.append(Commerce(row))

                if row['browser_id']:
                    if row['browser_id'] not in self.browser_steps:
                        self.browser_steps[row['browser_id']] = {
                            'checkout': 0,
                            'payment': 0,
                            'purchase': 0,
                            'refund': 0
                        }

                    if row['step'] not in ['checkout', 'payment', 'purchase', 'refund']:
                        raise Exception("unknown commerce step: " + row['step'] + ' for browser_id: ' + row['browser_id'])
                    else:
                        self.browser_steps[row['browser_id']][row['step']] += 1

    # def __save_events_to_separate_table(self):
    #     sql = '''
    #         INSERT INTO events (user_id, browser_id, time, type, computed_for)
    #         VALUES (%s, %s, %s, %s, %s)
    #     '''
    #
    #     psycopg2.extras.execute_batch(self.cursor, sql, [
    #         (x["user_id"], x["browser_id"], x["time"], x["type"], self.cur_date) for x in self.events_to_save
    #     ])
    #     self.cursor.connection.commit()

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

                # purchase event within 5 minutes of payment
                if purchase_time_minus_5 <= payment_time:
                    browser_id = self.user_id_browser_id[c.user_id]
                    # self.__mark_conversion_event(browser_id, purchase_time)
                    self.events_to_save.append({
                        "user_id": c.user_id,
                        "browser_id": browser_id,
                        "time": c.time,
                        "type": "conversion",
                    })

        # self.__save_events_to_separate_table()
        # self.__save_commerce_steps_count()

    def get_browser_commerce_steps_count(self, browser_id):
        if browser_id in self.browser_steps:
            return {
                "commerce_checkouts": self.browser_steps[browser_id]['checkout'],
                "commerce_payments": self.browser_steps[browser_id]['payment'],
                "commerce_purchases": self.browser_steps[browser_id]['purchase'],
                "commerce_refunds": self.browser_steps[browser_id]['refund'],
            }
        return None

    # def __save_commerce_steps_count(self):
    #     sql = '''
    #         UPDATE aggregated_browser_days
    #         SET commerce_checkouts = %s, commerce_payments = %s, commerce_purchases = %s, commerce_refunds = %s
    #         WHERE date = %s AND browser_id = %s
    #     '''
    #
    #     psycopg2.extras.execute_batch(self.cursor, sql, [
    #         (self.browser_steps[browser_id]['checkout'],
    #          self.browser_steps[browser_id]['payment'],
    #          self.browser_steps[browser_id]['purchase'],
    #          self.browser_steps[browser_id]['refund'],
    #          self.cur_date.isoformat(),
    #          browser_id) for browser_id in self.browser_steps
    #     ])
    #     self.cursor.connection.commit()

    # def __mark_conversion_event(self, browser_id, purchase_time):
    #     # first delete that particular day
    #     # we don't want conversion day to be included in aggregated data
    #     self.cursor.execute('''
    #         DELETE FROM aggregated_browser_days WHERE browser_id = %s and date = %s
    #     ''', (browser_id, self.cur_date))
    #
    #     # then mark 7_days_event to 'conversion'
    #     # for 7 previous days
    #     end = arrow.get(self.cur_date).shift(days=-1)
    #     start = end.shift(days=-6)
    #     sql = '''
    #         UPDATE aggregated_browser_days
    #         SET next_7_days_event = 'conversion', next_event_time = %s
    #         WHERE date = %s AND browser_id = %s AND next_7_days_event = 'no_conversion'
    #     '''
    #     psycopg2.extras.execute_batch(self.cursor, sql, [
    #         (purchase_time.isoformat(), day[0].date(), browser_id) for day in arrow.Arrow.span_range('day', start, end)
    #     ])
    #     self.cursor.connection.commit()


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


class SharedLoginParser:
    def __init__(self, cur_date):
        self.data = []
        self.not_logged_in_browsers = set()
        self.logged_in_browsers = set()
        self.logged_in_browsers_time = {}
        self.browser_user_id = {}
        self.cur_date = cur_date
        # self.cursor = cursor

    # def __save_events_to_separate_table(self):
    #     sql = '''
    #         INSERT INTO events (user_id, browser_id, time, type, computed_for)
    #         VALUES (%s, %s, %s, %s, %s)
    #     '''
    #     psycopg2.extras.execute_batch(self.cursor, sql, [
    #         (
    #             self.browser_user_id[browser_id],
    #             browser_id,
    #             self.logged_in_browsers_time[browser_id].isoformat(),
    #             "shared_account_login",
    #             self.cur_date
    #         )
    #         for browser_id in self.logged_in_browsers
    #     ])
    #     self.cursor.connection.commit()

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
                self.browser_user_id[p.browser_id] = p.user_id

    # def __save_in_db(self):
    #     print("Storing login data for date " + str(self.cur_date))
    #
    #     # first delete that particular day
    #     for browser_id in self.logged_in_browsers:
    #         self.cursor.execute('''
    #         DELETE FROM aggregated_browser_days WHERE browser_id = %s and date = %s
    #         ''', (browser_id, self.cur_date))
    #
    #     # then mark 7_days_event
    #     end = arrow.get(self.cur_date).shift(days=-1)
    #     start = end.shift(days=-6)
    #
    #     sql = '''
    #     UPDATE aggregated_browser_days
    #     SET next_7_days_event = 'shared_account_login', next_event_time = %s
    #     WHERE date = %s AND browser_id = %s AND next_7_days_event = 'no_conversion'
    #     '''
    #     psycopg2.extras.execute_batch(self.cursor, sql, [
    #         (self.logged_in_browsers_time[browser_id].isoformat(), day[0].date(), browser_id)
    #         for day in arrow.Arrow.span_range('day', start, end)
    #         for browser_id in self.logged_in_browsers
    #     ])
    #     self.cursor.connection.commit()

    def process_file(self, pageviews_file):
        print("Processing file: " + pageviews_file)
        self.__load_data(pageviews_file)
        self.data.sort(key=lambda x: x.time)
        self.__find_login_events()
        # self.__save_in_db()
        # self.__save_events_to_separate_table()


# def run(file_date, aggregate_folder):
#     load_env()
#     commerce_file = os.path.join(aggregate_folder, "commerce", "commerce_" + file_date + ".csv")
#     pageviews_file = os.path.join(aggregate_folder, "pageviews", "pageviews_" + file_date + ".csv")
#
#     if not os.path.isfile(commerce_file):
#         print("Error: file " + commerce_file + " does not exist")
#         return
#
#     if not os.path.isfile(pageviews_file):
#         print("Error: file " + pageviews_file + " does not exist")
#         return
#
#     year = int(file_date[0:4])
#     month = int(file_date[4:6])
#     day = int(file_date[6:8])
#     cur_date = date(year, month, day)
#
#     conn, cur = create_con(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASS"), os.getenv("POSTGRES_DB"), os.getenv("POSTGRES_HOST"))
#     migrate(cur)
#     conn.commit()
#
#     event_types = ['conversion', 'shared_account_login']
#     # Delete events for particular day (so command can be safely run multiple times)
#     # cur.execute('''
#     #     DELETE FROM events WHERE computed_for = %s AND type = ANY(%s)
#     # ''', (cur_date, event_types))
#     # conn.commit()
#
#     commerce_parser = CommerceParser(cur_date)
#     commerce_parser.process_file(commerce_file)
#
#     pageviews_parser= SharedLoginParser(cur_date)
#     pageviews_parser.process_file(pageviews_file)
#
#     # cur.close()
#     # conn.close()


# def main():
#     parser = argparse.ArgumentParser(
#         description='Script to process future events commerce data')
#     parser.add_argument('date', metavar='date', help='Aggregate date, format YYYYMMDD')
#     parser.add_argument('--dir', metavar='AGGREGATE_DIRECTORY', dest='dir', default=BASE_PATH,
#                         help='where to look for aggregated CSV files')
#
#     args = parser.parse_args()
#     run(args.date, args.dir)
#
# if __name__ == '__main__':
#     main()
