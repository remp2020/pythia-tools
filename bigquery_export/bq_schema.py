from google.cloud import bigquery

# https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.mode

# Type
# Required. The field data type.
# Possible values include
# STRING, BYTES, INTEGER, INT64 (same as INTEGER), FLOAT, FLOAT64 (same as FLOAT),
# BOOLEAN, BOOL (same as BOOLEAN), TIMESTAMP, DATE, TIME, DATETIME,
# RECORD (where RECORD indicates that the field contains a nested schema) or STRUCT (same as RECORD).

# Mode
# Optional. The field mode.
# Possible values include NULLABLE, REQUIRED and REPEATED.
# The default value is NULLABLE.

def aggregated_browser_days():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("browser_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("timespent", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sessions", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sessions_without_ref", "INTEGER", mode="REQUIRED"),

        bigquery.SchemaField("browser_family", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("browser_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("os_family", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("os_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("device_family", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("device_brand", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("device_model", "STRING", mode="NULLABLE"),

        bigquery.SchemaField("is_desktop", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_mobile", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_tablet", "STRING", mode="NULLABLE"),

        bigquery.SchemaField("next_7_days_event", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("next_event_time", "TIMESTAMP", mode="NULLABLE"),

        bigquery.SchemaField("referer_medium_pageviews", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("article_category_pageviews", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("article_tag_pageviews", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hour_interval_pageviews", "STRING", mode="NULLABLE"),

        bigquery.SchemaField("pageviews_0h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_1h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_2h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_3h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_4h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_5h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_6h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_7h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_8h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_9h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_10h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_11h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_12h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_13h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_14h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_15h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_16h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_17h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_18h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_19h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_20h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_21h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_22h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_23h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_0h_4h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_4h_8h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_8h_12h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_12h_16h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_16h_20h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_20h_24h", "INTEGER", mode="NULLABLE"),
    ]
    return schema


def aggregated_browser_days_tags():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("browser_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tag", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
    ]
    return schema


def aggregated_browser_days_categories():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("browser_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
    ]
    return schema


def aggregated_browser_days_referer_mediums():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("browser_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("referer_medium", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
    ]
    return schema


def aggregated_user_days():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("browser_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("timespent", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sessions", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sessions_without_ref", "INTEGER", mode="REQUIRED"),

        bigquery.SchemaField("next_30_days", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("next_event_time", "TIMESTAMP", mode="NULLABLE"),

        bigquery.SchemaField("referer_medium_pageviews", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("article_category_pageviews", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("article_tag_pageviews", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hour_interval_pageviews", "STRING", mode="NULLABLE"),

        bigquery.SchemaField("pageviews_0h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_1h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_2h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_3h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_4h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_5h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_6h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_7h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_8h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_9h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_10h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_11h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_12h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_13h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_14h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_15h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_16h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_17h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_18h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_19h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_20h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_21h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_22h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_23h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_0h_4h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_4h_8h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_8h_12h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_12h_16h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_16h_20h", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("pageviews_20h_24h", "INTEGER", mode="NULLABLE"),
    ]
    return schema


def aggregated_user_days_tags():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tag", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
    ]
    return schema


def aggregated_user_days_categories():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
    ]
    return schema


def aggregated_user_days_referer_mediums():
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("referer_medium", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pageviews", "INTEGER", mode="REQUIRED"),
    ]
    return schema


def events():
    schema = [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("browser_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
    ]
    return schema