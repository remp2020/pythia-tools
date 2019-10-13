from typing import List, Dict


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


SUPPORTED_JSON_FIELDS_KEYS = {
    'hour_interval_pageviews': [
         '00:00-00:59_03:00-03:59',
         '04:00-04:59_07:00-07:59',
         '08:00-08:59_11:00-11:59',
         '12:00-12:59_15:00-15:59',
         '16:00-16:59_19:00-19:59',
         '20:00-20:59_23:00-23:59'],
    'referer_medium_pageviews': [
        'direct',
        'email',
        'external',
        'internal',
        'search',
        'social'
    ],
    'article_category_pageviews': [
        '',
        'blog',
        'ekonomika',
        'hlavna',
        'karikatury',
        'komentare',
        'kultura',
        'nezaradene',
        'pageview',
        'rodina-a-vztahy',
        'slovensko',
        'sport',
        'svet',
        'veda',
        'zdravie'
    ]
}

NUMERIC_COLUMN_WINDOW_NAME_SUFFIXES_AND_PREFIXES = [
    ['', '_first_window_half'],
    ['', '_last_window_half'],
    ['relative_', '_change_first_and_second_half']
]

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


def build_out_profile_based_column_names(
        normalized: bool = False
):
    if normalized is True:
        suffix = '_normalized'
    else:
        suffix = ''
    profile_numeric_columns_from_json_fields = {
        # Referral features
        'referer_medium_pageviews': [
            [f'referer_medium_pageviews_{referral_category}_count{suffix}'
             for referral_category in SUPPORTED_JSON_FIELDS_KEYS['referer_medium_pageviews']]
        ],
        # Article category (section) features
        'article_category_pageviews': [
            [f'article_category_pageviews_{article_category}_count{suffix}'
             for article_category in SUPPORTED_JSON_FIELDS_KEYS['article_category_pageviews']]
        ],
        # Time based features combining day_of_week with hour interval
        'hour_interval_pageviews': [
            [f'dow_{dow}_hours_{hours}_count{suffix}'
             for dow in range(0, 7)
             for hours in SUPPORTED_JSON_FIELDS_KEYS['hour_interval_pageviews']
             ],
            [f'dow_{dow}_count{suffix}' for dow in range(0, 7)
             ],
            [f'hours_{hours}_count{suffix}' for hours in SUPPORTED_JSON_FIELDS_KEYS['hour_interval_pageviews']]
        ]
    }

    return profile_numeric_columns_from_json_fields


def create_window_variant_permuations(
        column_list: List[str]
) -> List[str]:

    return [
            prefix_suffix_values[0] + original_column + prefix_suffix_values[1] for original_column in
            column_list
            for prefix_suffix_values in NUMERIC_COLUMN_WINDOW_NAME_SUFFIXES_AND_PREFIXES
            if original_column != 'days_since_last_active'
    ]


def unpack_profile_based_fields(
        config_dict: Dict[str, List[List[str]]]
) -> List[str]:

    return [
        column for column_set_list in config_dict.values()
        for column_set in column_set_list
        for column in column_set
    ]


class FeatureColumns(object):
    def __init__(self):
        self.categorical_columns = CATEGORICAL_COLUMNS
        self.base_numeric_columns = NUMERIC_COLUMNS_BASE
        self.profile_numeric_columns_from_json_fields = build_out_profile_based_column_names(False)

        self.numeric_columns = NUMERIC_COLUMNS_BASE + \
            unpack_profile_based_fields(self.profile_numeric_columns_from_json_fields)

        self.numeric_columns_with_window_variants = create_window_variant_permuations(self.base_numeric_columns) + \
            self.base_numeric_columns

        self.bool_columns = BOOL_COLUMNS
        self.config_columns = CONFIG_COLUMNS

    def add_normalized_profile_features_version(self):
        normalized_column_names = build_out_profile_based_column_names(True)
        normalized_column_names = create_window_variant_permuations(
            unpack_profile_based_fields(normalized_column_names)
        )

        self.numeric_columns_with_window_variants = self.numeric_columns_with_window_variants + normalized_column_names


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        }
    },
    'formatters': {
        'default': {
            'format': '%(asctime)s [%(levelname)s] %(name)s - %(message)s'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    }
}
