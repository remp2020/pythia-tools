from enum import Enum


CATEGORICAL_COLUMNS = ['device', 'browser', 'os']

NUMERIC_COLUMNS_ORIGINAL = [
    'pageview_count',
    'visit_count',
    'direct_visit_count',
    'days_active_count',
    'pageviews_per_visit',
    'timespent_sum',
    'visits_per_day_active',
    'direct_visits_share',
    'timespent_per_visit',
    'timespent_per_pageview',
    'days_since_last_active'
]

NUMERIC_COLUMN_WINDOW_NAME_SUFFIXES_AND_PREFIXES = [
    ['', '_first_window_half'],
    ['', '_last_window_half'],
    ['relative_', '_change_first_and_second_half']
]

NUMERIC_COLUMNS = [
    prefix_suffix_values[0] +  original_column + prefix_suffix_values[1] for original_column in NUMERIC_COLUMNS_ORIGINAL
    for prefix_suffix_values in NUMERIC_COLUMN_WINDOW_NAME_SUFFIXES_AND_PREFIXES
    if original_column != 'days_since_last_active'
] + NUMERIC_COLUMNS_ORIGINAL

BOOL_COLUMNS = [
    'is_desktop',
    'is_mobile',
    'is_tablet',
    'is_active_on_date'
]

CONFIG_COLUMNS = [
    'date',
    'browser_id',
    'user_id'
]

split_type = Enum(
    'random',
    'time_based'
)

LABELS = ['no_conversion', 'shared_account_login', 'conversion']

CURRENT_MODEL_VERSION = '1.0'

DERIVED_METRICS_CONFIG = {
    'pageviews_per_visit': {
        'nominator': 'pageview_count',
        'denominator': 'visit_count'
    },
    'visits_per_day_active': {
        'nominator': 'visit_count',
        'denominator': 'days_active_count'
    },
    'direct_visits_share': {
        'nominator': 'direct_visit_count',
        'denominator': 'visit_count'
    },
    'timespent_per_visit': {
        'nominator': 'timespent_sum',
        'denominator': 'visit_count'
    },
    'timespent_per_pageview': {
        'nominator': 'timespent_sum',
        'denominator': 'pageview_count'
    },
}
