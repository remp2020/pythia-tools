import os.path
import psycopg2
from dotenv import load_dotenv


def load_env(env_path=None):
    if env_path is None:
        env_path = os.path.dirname(os.path.realpath(__file__)) + "/../.env"
    load_dotenv(dotenv_path=env_path)


def create_con(username, password, db, server):
    server_parts = server.split(':')
    host = server_parts[0]
    port = server_parts[1] if len(server_parts) > 1 else '5432'
    conn = psycopg2.connect(dbname=db, user=username, password=password, host=host, port=port)
    cur = conn.cursor()
    return conn, cur


def migrate(cur):
    sql = '''
CREATE TABLE IF NOT EXISTS "public"."aggregated_browser_days" (
    "date" date NOT NULL,
    "browser_id" character varying NOT NULL,
    "pageviews" integer NOT NULL,
    "timespent" integer,
    "sessions" integer NOT NULL,
    "sessions_without_ref" integer NOT NULL,    
    "browser_family" text, 
    "browser_version" text, 
    "os_family" text, 
    "os_version" text, 
    "device_family" text, 
    "device_brand" text, 
    "device_model" text,
    "is_desktop" boolean, 
    "is_mobile" boolean, 
    "is_tablet" boolean,
    "next_7_days_event" character varying NOT NULL DEFAULT 'no_conversion',
    "next_event_time" timestamp,
    PRIMARY KEY(date, browser_id)
) WITH (oids = false);
'''
    cur.execute(sql)

    sql2 = '''
    ALTER TABLE "public"."aggregated_browser_days" ADD COLUMN IF NOT EXISTS user_id text;
    
    '''
    cur.execute(sql2)