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
    "user_ids" text[] NOT NULL DEFAULT '{}',
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
    "referer_medium_pageviews" jsonb,
    "article_category_pageviews" jsonb,
    "hour_interval_pageviews" jsonb,
    PRIMARY KEY(date, browser_id)
) WITH (oids = false);
'''
    cur.execute(sql)

    sql2 = '''
ALTER TABLE "public"."aggregated_browser_days" 
    ADD COLUMN IF NOT EXISTS "pageviews_0h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_1h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_2h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_3h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_4h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_5h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_6h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_7h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_8h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_9h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_10h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_11h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_12h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_13h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_14h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_15h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_16h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_17h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_18h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_19h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_20h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_21h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_22h" integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "pageviews_23h" integer DEFAULT 0;
    '''
    cur.execute(sql2)

    # Let's bundle the hourly data into 4 hour intervals (such as 00:00 - 03:59, ...) to avoid having too many columns.
    # The division works with the following hypothesis:
    # * 0-4: Night Owls
    # * 4-8: Morning commute
    # * 8-12: Working morning / coffee
    # * 12-16: Early afternoon, browsing during lunch
    # * 16-20: Evening commute
    # * 20-24: Before bed browsing
    sql3 = '''
    ALTER TABLE "public"."aggregated_browser_days" 
        ADD COLUMN IF NOT EXISTS "pageviews_0h_4h" integer DEFAULT 0,
        ADD COLUMN IF NOT EXISTS "pageviews_4h_8h" integer DEFAULT 0,
        ADD COLUMN IF NOT EXISTS "pageviews_8h_12h" integer DEFAULT 0,
        ADD COLUMN IF NOT EXISTS "pageviews_12h_16h" integer DEFAULT 0,
        ADD COLUMN IF NOT EXISTS "pageviews_16h_20h" integer DEFAULT 0,
        ADD COLUMN IF NOT EXISTS "pageviews_20h_24h" integer DEFAULT 0;
        '''
    cur.execute(sql3)

    sql_aggregated_browser_days = '''
CREATE TABLE IF NOT EXISTS "public"."aggregated_user_days" (
    "date" date NOT NULL,
    "user_id" character varying NOT NULL,
    "browser_ids" text[] NOT NULL DEFAULT '{}',
    "pageviews" integer NOT NULL,
    "timespent" integer,
    "sessions" integer NOT NULL,
    "sessions_without_ref" integer NOT NULL,    
    "next_30_days" character varying NOT NULL DEFAULT 'ongoing',
    "next_event_time" timestamp NULL,
    "referer_medium_pageviews" jsonb,
    "article_category_pageviews" jsonb,
    "hour_interval_pageviews" jsonb,
    "pageviews_0h" integer DEFAULT 0,
    "pageviews_1h" integer DEFAULT 0,
    "pageviews_2h" integer DEFAULT 0,
    "pageviews_3h" integer DEFAULT 0,
    "pageviews_4h" integer DEFAULT 0,
    "pageviews_5h" integer DEFAULT 0,
    "pageviews_6h" integer DEFAULT 0,
    "pageviews_7h" integer DEFAULT 0,
    "pageviews_8h" integer DEFAULT 0,
    "pageviews_9h" integer DEFAULT 0,
    "pageviews_10h" integer DEFAULT 0,
    "pageviews_11h" integer DEFAULT 0,
    "pageviews_12h" integer DEFAULT 0,
    "pageviews_13h" integer DEFAULT 0,
    "pageviews_14h" integer DEFAULT 0,
    "pageviews_15h" integer DEFAULT 0,
    "pageviews_16h" integer DEFAULT 0,
    "pageviews_17h" integer DEFAULT 0,
    "pageviews_18h" integer DEFAULT 0,
    "pageviews_19h" integer DEFAULT 0,
    "pageviews_20h" integer DEFAULT 0,
    "pageviews_21h" integer DEFAULT 0,
    "pageviews_22h" integer DEFAULT 0,
    "pageviews_23h" integer DEFAULT 0,
    "pageviews_0h_4h" integer DEFAULT 0,
    "pageviews_4h_8h" integer DEFAULT 0,
    "pageviews_8h_12h" integer DEFAULT 0,
    "pageviews_12h_16h" integer DEFAULT 0,
    "pageviews_16h_20h" integer DEFAULT 0,
    "pageviews_20h_24h" integer DEFAULT 0,
    PRIMARY KEY(date, user_id)
) WITH (oids = false);
'''
    cur.execute(sql_aggregated_browser_days)

    sql_events = '''
    CREATE TABLE IF NOT EXISTS "public"."events" (
        "id" SERIAL PRIMARY KEY,
        "user_id" character varying NOT NULL,
        "browser_id" character varying NOT NULL,
        "time" timestamp NOT NULL,
        "type" character varying NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_events_user_id ON "public"."events"(user_id);
    CREATE INDEX IF NOT EXISTS idx_events_type ON "public"."events"(type);
    '''
    cur.execute(sql_events)


