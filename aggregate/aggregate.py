from __future__ import print_function
import os
import os.path
import re
import argparse
import mysql.connector
import psutil
from datetime import date
from dotenv import load_dotenv
from google.cloud import bigquery
import utils.bq_schema as bq_schema
from utils.pageviews import UserParser, BrowserParser
from utils.conversion_and_commerce_events import CommerceParser, SharedLoginParser
from utils.subscriptions_churn_events import ChurnEventsParser
from utils.bq_upload import BigQueryUploader


BASE_PATH = os.path.dirname(os.path.realpath(__file__))

def using(point=""):
    pid = os.getpid()
    py = psutil.Process(pid)
    memory_use = py.memory_info()[0] / 2. ** 20  # memory use in MB
    debug_info = '''%s: mem=%s MB
           '''%(point, memory_use)
    print(debug_info)


def create_mysql_connection(username, password, db, host):
    cnx = mysql.connector.connect(user=username, password=password, host=host, database=db)
    cursor = cnx.cursor()
    return cnx, cursor


def init_big_query_uploader(project_id, dataset_id):
    uploader = BigQueryUploader(project_id, dataset_id)
    bigquery_ready = uploader.is_dataset_ready()
    if not bigquery_ready:
        print("Unable to connect to dataset {}, project {}, quitting".format(dataset_id, project_id))
        return None

    # Create tables if not exist
    date_col_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
        expiration_ms=31536000000,  # 365 days
    )

    tables_and_schemas = {
        "browsers": bq_schema.browsers(),
        "browser_users": bq_schema.browser_users(),

        "aggregated_browser_days": bq_schema.aggregated_browser_days(),
        "aggregated_browser_days_tags": bq_schema.aggregated_browser_days_tags(),
        "aggregated_browser_days_categories": bq_schema.aggregated_browser_days_categories(),
        "aggregated_browser_days_referer_mediums": bq_schema.aggregated_browser_days_referer_mediums(),

        "aggregated_user_days": bq_schema.aggregated_user_days(),
        "aggregated_user_days_tags": bq_schema.aggregated_user_days_tags(),
        "aggregated_user_days_categories": bq_schema.aggregated_user_days_categories(),
        "aggregated_user_days_referer_mediums": bq_schema.aggregated_user_days_referer_mediums(),
    }
    # tables partitioned by 'date' column
    for table_name, table_schema in tables_and_schemas.items():
        if not uploader.table_exists(table_name):
            uploader.create_table(table_name, table_schema, date_col_partitioning)

    # tables partitioned by 'time' column
    time_col_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="time",
        expiration_ms=31536000000,  # 365 days
    )
    if not uploader.table_exists('events'):
        uploader.create_table('events', bq_schema.events(), time_col_partitioning)
    return uploader


def run(file_date, aggregate_folder):
    load_dotenv()

    commerce_file = os.path.join(aggregate_folder, "commerce", "commerce_" + file_date + ".csv")
    pageviews_file = os.path.join(aggregate_folder, "pageviews", "pageviews_" + file_date + ".csv")
    pageviews_time_spent_file = os.path.join(aggregate_folder, "pageviews_time_spent", "pageviews_time_spent_" + file_date + ".csv")

    if not os.path.isfile(commerce_file):
        print("Error: file " + commerce_file + " does not exist")
        exit(-1)

    if not os.path.isfile(pageviews_file):
        print("Error: file " + pageviews_file + " does not exist")
        exit(-1)

    pattern = re.compile("(.*)pageviews_([0-9]+).csv")
    m = pattern.search(pageviews_file)
    date_str = m.group(2)
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    cur_date = date(year, month, day)

    using("init")

    bq_uploader = init_big_query_uploader(os.getenv("BIGQUERY_PROJECT_ID"), os.getenv("BIGQUERY_DATASET_ID"))
    if bq_uploader is None:
        exit(-1)

    browser_parser = BrowserParser()
    browser_parser.process_files(pageviews_file, pageviews_time_spent_file, commerce_file)
    browser_parser.upload_to_bq(bq_uploader, cur_date)

    using("After BrowserParser")




    #  TODO: UNCOMMENT!!
    #
    # user_parser = UserParser()
    # user_parser.process_files(pageviews_file, pageviews_time_spent_file)
    # # user_parser.store_in_db(conn, cur, date(year, month, day))
    #
    # using("After UserParser")
    #
    # commerce_parser = CommerceParser(cur_date)
    # commerce_parser.process_file(commerce_file)
    #
    # using("After CommerceParser")
    #
    # pageviews_parser = SharedLoginParser(cur_date)
    # pageviews_parser.process_file(pageviews_file)
    #
    # using("After SharedLoginParser")
    #
    # if os.getenv("CRM_DB_HOST") is None:
    #     print('CRM database connection settings not set in .env file, skipping churn/renewal data aggregation')
    #     return
    #
    # crm_db_conn, crm_db_cur = create_mysql_connection(
    #     os.getenv("CRM_DB_USER"),
    #     os.getenv("CRM_DB_PASS"),
    #     os.getenv("CRM_DB_DB"),
    #     os.getenv("CRM_DB_HOST")
    # )
    #
    # churn_events_parser = ChurnEventsParser(cur_date, crm_db_cur)
    # churn_events_parser.parse()
    #
    # using("After ChurnEventsParser")
    #
    # crm_db_cur.close()
    # crm_db_conn.close()


def main():
    parser = argparse.ArgumentParser(description='Script to parse elastic CSV export, process it and insert into BigQuery DB for further processing')
    parser.add_argument('date', metavar='date', help='Aggregate date, format YYYYMMDD')
    parser.add_argument('--dir', metavar='AGGREGATE_DIRECTORY', dest='dir', default=BASE_PATH, help='where to look for aggregated CSV files')

    args = parser.parse_args()
    run(args.date, args.dir)

if __name__ == '__main__':
    main()

