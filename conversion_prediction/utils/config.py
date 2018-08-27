from enum import Enum


CATEGORICAL_COLUMNS = ['device', 'browser', 'os']

NUMERIC_COLUMNS = [
    'pageview_count',
    'visit_count',
    'direct_visit_count',
    'days_active_count',
    'pageviews_per_visit',
    'timespent_sum',
    'visits_per_day_active',
    'direct_visits_share',
    'timespent_per_visit',
    'timespent_per_pageview'
]

BOOL_COLUMNS = [
    'is_desktop',
    'is_mobile',
    'is_tablet',
    'is_active_on_date'
]

CONFIG_COLUMNS = [
    'date',
    'browser_id'
]

split_type = Enum(
    'random',
    'time_based'
)

LABELS = ['no_conversion', 'shared_account_login', 'conversion']

CURRENT_MODEL_VERSION = '1.0'
