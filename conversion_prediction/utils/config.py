from enum import Enum


CATEGORICAL_COLUMNS = ['device', 'browser', 'os', 'day_of_week']

NUMERIC_COLUMNS_BASE = [
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

NUMERIC_COLUMNS_FROM_JSON_FIELDS = [
    'referer_medium_pageviews_direct',
    'referer_medium_pageviews_email',
    'referer_medium_pageviews_external',
    'referer_medium_pageviews_internal',
    'referer_medium_pageviews_search',
    'referer_medium_pageviews_social',
    'article_category_pageviews_',
    'article_category_pageviews_blog',
    'article_category_pageviews_ekonomika',
    'article_category_pageviews_hlavna',
    'article_category_pageviews_karikatury',
    'article_category_pageviews_komentare',
    'article_category_pageviews_kultura',
    'article_category_pageviews_nezaradene',
    'article_category_pageviews_pageview',
    'article_category_pageviews_rodina-a-vztahy',
    'article_category_pageviews_slovensko',
    'article_category_pageviews_sport',
    'article_category_pageviews_svet',
    'article_category_pageviews_veda',
    'article_category_pageviews_zdravie',
    'referer_medium_pageviews_direct_count',
    'referer_medium_pageviews_email_count',
    'referer_medium_pageviews_external_count',
    'referer_medium_pageviews_internal_count',
    'referer_medium_pageviews_search_count',
    'referer_medium_pageviews_social_count',
    'article_category_pageviews__count',
    'article_category_pageviews_blog_count',
    'article_category_pageviews_ekonomika_count',
    'article_category_pageviews_hlavna_count',
    'article_category_pageviews_karikatury_count',
    'article_category_pageviews_komentare_count',
    'article_category_pageviews_kultura_count',
    'article_category_pageviews_nezaradene_count',
    'article_category_pageviews_pageview_count',
    'article_category_pageviews_rodina-a-vztahy_count',
    'article_category_pageviews_slovensko_count',
    'article_category_pageviews_sport_count',
    'article_category_pageviews_svet_count',
    'article_category_pageviews_veda_count',
    'article_category_pageviews_zdravie_count',
    # Time based features combining day_of_week with hour interval
    *[f'dow_{dow}_hours_{hours}_count'
      for dow in range(0,7)
          for hours in [
         '00:00-00:59_03:00-03:59',
         '04:00-04:59_07:00-07:59',
         '08:00-08:59_11:00-11:59',
         '12:00-12:59_15:00-15:59',
         '16:00-16:59_19:00-19:59',
         '20:00-20:59_23:00-23:59']
      ]
]

NUMERIC_COLUMNS = NUMERIC_COLUMNS_BASE + NUMERIC_COLUMNS_FROM_JSON_FIELDS

NUMERIC_COLUMN_WINDOW_NAME_SUFFIXES_AND_PREFIXES = [
    ['', '_first_window_half'],
    ['', '_last_window_half'],
    ['relative_', '_change_first_and_second_half']
]

NUMERIC_COLUMNS = [
    prefix_suffix_values[0] +  original_column + prefix_suffix_values[1] for original_column in NUMERIC_COLUMNS_BASE
    for prefix_suffix_values in NUMERIC_COLUMN_WINDOW_NAME_SUFFIXES_AND_PREFIXES
    if original_column != 'days_since_last_active'
] + NUMERIC_COLUMNS_BASE

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
        'nominator': 'timespent_count',
        'denominator': 'visit_count'
    },
    'timespent_per_pageview': {
        'nominator': 'timespent_count',
        'denominator': 'pageview_count'
    },
}

JSON_COLUMNS = ['referer_medium_pageviews', 'hour_interval_pageviews', 'article_category_pageviews']
