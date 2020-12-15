from typing import List, Dict
from sqlalchemy import func


CATEGORICAL_COLUMNS = ['day_of_week']


def sanitize_column_name(column_name: str) -> str:
    new_column_name = column_name.replace('-', '_')
    new_column_name = new_column_name.replace('-', '_')
    return new_column_name


def build_numeric_columns_base(
        aggregation_function_alias: str
) -> List[str]:
    numeric_columns_base = [
        f'pageviews_{aggregation_function_alias}',
        f'visit_{aggregation_function_alias}',
        f'direct_visit_{aggregation_function_alias}',
        'days_active_count',
        f'pageviews_per_visit_{aggregation_function_alias}',
        f'timespent_{aggregation_function_alias}',
        f'visits_per_day_active_{aggregation_function_alias}',
        f'direct_visits_share_{aggregation_function_alias}',
        f'timespent_per_visit_{aggregation_function_alias}',
        f'timespent_per_pageview_{aggregation_function_alias}',
        'days_since_last_active',
    ] + [
        f'{device}_device_{aggregation_function_alias}'
        for device in ['desktop', 'mobile', 'tablet']
    ]

    return numeric_columns_base


def get_device_information_features(
        aggregation_function_alias,
        start_time,
        end_time
):
    from .bigquery import get_prominent_device_list

    device_brands = get_prominent_device_list(
        start_time,
        end_time
    )

    # We will need to get the device names for differing handling in regards to rolling window calculations,
    # the base name won't have any aggregation
    optional_underscore = '_' if aggregation_function_alias != '' else ''

    device_brands = [
        f'{sanitize_column_name(device_brand)}_device_brand{optional_underscore}{aggregation_function_alias}'
        for device_brand in device_brands
    ]

    if not device_brands:
        raise ValueError('Device Brands Feature List Empty')

    device_types = [f'{device_type}_device{optional_underscore}{aggregation_function_alias}'
                    for device_type in EXPECTED_DEVICE_TYPES]

    return device_brands + device_types


SUPPORTED_JSON_FIELDS_KEYS = {
    'referer_mediums': [
        'direct',
        'email',
        'external',
        'internal',
        'search',
        'social'
    ],
    'categories': [
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
    'is_active_on_date'
]

CONFIG_COLUMNS = [
    'date',
    'browser_id',
    'user_ids'
]

# TODO: Check if the positive/negative distinction is still needed when using automated undersampling
LABELS = {'churn': 'negative', 'renewal': 'positive'}

CURRENT_MODEL_VERSION = '1.0'


def build_derived_metrics_config(aggregation_function_alias: str) -> Dict:
    derived_metrics_config = {
        f'pageviews_per_visit_{aggregation_function_alias}': {
            'nominator': f'pageviews_{aggregation_function_alias}',
            'denominator': f'visit_{aggregation_function_alias}'
        },
        f'visits_per_day_active_{aggregation_function_alias}': {
            'nominator': f'visit_{aggregation_function_alias}',
            'denominator': f'days_active_count'
        },
        f'direct_visits_share_{aggregation_function_alias}': {
            'nominator': f'direct_visit_{aggregation_function_alias}',
            'denominator': f'visit_{aggregation_function_alias}'
        },
        f'timespent_per_visit_{aggregation_function_alias}': {
            'nominator': f'timespent_{aggregation_function_alias}',
            'denominator': f'visit_{aggregation_function_alias}'
        },
        f'timespent_per_pageview_{aggregation_function_alias}': {
            'nominator': f'timespent_{aggregation_function_alias}',
            'denominator': f'pageviews_{aggregation_function_alias}'
        },
    }

    return derived_metrics_config


# TODO: Categorie is only there due to inconvenient table names, will hopefully get fixed later
PROFILE_COLUMNS = ['referer_mediums', 'hour_interval_pageviews', 'categories']


def generate_4_hour_interval_column_names():
    hour_ranges = []
    for i in range(0, 24, 4):
        hour_ranges.append(f'pvs_{i}h_{i + 4}h')

    return hour_ranges


def return_normalized_suffix(normalized: bool=False) -> str:
    if normalized is True:
        return '_normalized'
    else:
        return ''


def generate_all_time_based_column_names(
    aggregation_function_aliases: List[str] = ['count'],
    normalized: bool = False
) -> Dict[str, List[str]]:
    # Time based features combining day_of_week with hour interval
    suffix = return_normalized_suffix(normalized)
    time_based_columns = dict()
    time_based_columns['hour_ranges'] = [
        f'{column}_avg' for column in generate_4_hour_interval_column_names()
        for aggregation_function_alias in aggregation_function_aliases
    ]
    time_based_columns['days_of_week'] = [
        f'dow_{dow}_{aggregation_function_alias}{suffix}' for dow in range(0, 7)
        for aggregation_function_alias in aggregation_function_aliases
    ]

    # TODO: Bigquery has a limit on size of the pipeline requested via a query, this part creates too many combinations
    # time_based_columns['hour_of_day_of_week'] = [
    #     f'dow_{dow}_hours_{hours}_{aggregation_function_alias}{suffix}'
    #     for dow in range(0, 7)
    #     for hours in time_based_columns['hour_ranges']
    #     for aggregation_function_alias in aggregation_function_aliases
    #  ]

    return time_based_columns


def build_out_profile_based_column_names(
        aggregation_function_aliases: List[str] = ['count'],
        normalized: bool = False
) -> Dict[str, List[str]]:

    suffix = return_normalized_suffix(normalized)
    profile_numeric_columns_from_json_fields = {
        # Referral features
        'referer_mediums': [
            f'referer_mediums_{referral_category}_{aggregation_function_alias}{suffix}'
            for referral_category in SUPPORTED_JSON_FIELDS_KEYS['referer_mediums']
            for aggregation_function_alias in aggregation_function_aliases
        ],
        # Article category (section) features
        'categories': [
            f'categories_{article_category.replace("-", "_")}_{aggregation_function_alias}{suffix}'
            for article_category in SUPPORTED_JSON_FIELDS_KEYS['categories']
            for aggregation_function_alias in aggregation_function_aliases
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
        config_dict: Dict[str, List[str]]
) -> List[str]:

    return [
        column for column_set in config_dict.values()
        for column in column_set
    ]


class FeatureColumns(object):
    def __init__(
            self,
            aggregation_function_aliases,
            start_time,
            end_time
    ):
        # Add one version for each aggregation whenever available
        base_numeric_columns = set()
        device_based_features = set()
        for aggregation_function_alias in aggregation_function_aliases:
            base_numeric_columns.update(
                build_numeric_columns_base(
                    aggregation_function_alias
                )
            )

            base_numeric_columns.update(
                list(build_derived_metrics_config(aggregation_function_alias))
            )

        device_based_features.update(
            get_device_information_features(
                aggregation_function_aliases[0] if aggregation_function_aliases == [''] else 'sum',
                start_time,
                end_time
            )
        )

        self.device_based_features = list(device_based_features)
        self.categorical_columns = CATEGORICAL_COLUMNS
        self.base_numeric_columns = list(base_numeric_columns)
        normalized = False
        self.profile_numeric_columns_from_json_fields = build_out_profile_based_column_names(
            aggregation_function_aliases,
            normalized

        )
        self.time_based_columns = generate_all_time_based_column_names(aggregation_function_aliases, normalized)

        self.numeric_columns = self.base_numeric_columns

        self.numeric_columns_window_variants = create_window_variant_permuations(
            self.numeric_columns +
            unpack_profile_based_fields(self.profile_numeric_columns_from_json_fields) +
            unpack_profile_based_fields(self.time_based_columns)
        )

        self.numeric_columns_all = (
                self.numeric_columns_window_variants +
                self.numeric_columns +
                unpack_profile_based_fields(self.profile_numeric_columns_from_json_fields) +
                unpack_profile_based_fields(self.time_based_columns) +
                self.device_based_features
        )

        self.bool_columns = BOOL_COLUMNS
        self.config_columns = CONFIG_COLUMNS

    def add_normalized_profile_features_version(self, aggregation_function_aliases):
        normalized_column_names = build_out_profile_based_column_names(aggregation_function_aliases, True)
        normalized_column_names.update(
            generate_all_time_based_column_names(aggregation_function_aliases, True)
        )

        final_fields = create_window_variant_permuations(
            unpack_profile_based_fields(normalized_column_names)
        )

        self.numeric_columns_all = self.numeric_columns_all + final_fields

    def add_payment_history_features(self):
        self.numeric_columns_all = self.numeric_columns_all + ['clv']

    def add_global_context_features(self):
        self.numeric_columns_all = self.numeric_columns_all + [
            'article_pageviews_count', 'sum_paid', 'avg_price'
        ]
    
    def return_feature_list(self):
        return (
                self.categorical_columns +
                self.numeric_columns_all
        )

    def remove_columns_missing_in_data(self, user_profile_columns: List[str]):
        for key, column_set in self.__dict__.items():
            if isinstance(column_set, list):
                for column in column_set:
                    if column not in list(user_profile_columns):
                        self.__dict__[key] = column_set.remove(column)
            elif isinstance(column_set, dict):
                for column_subset_key, column_subset in column_set.items():
                    for column in column_subset:
                        if column not in list(user_profile_columns):
                            self.__dict__[key][
                                column_subset_key] = column_subset.remove(column)
            else:
                raise ValueError(
                    'Unknown feature set data struct encountered when removing columns not retrieved from data: '
                    f'{column_set}'
                )


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

AGGREGATION_FUNCTIONS_w_ALIASES = {
    'count': func.sum,
    'avg': func.avg,
    'min': func.min,
    'max': func.max
}

MIN_TRAINING_DAYS = 7

CURRENT_PIPELINE_VERSION = '1.01'

EXPECTED_DEVICE_TYPES = ['desktop', 'mobile', 'tablet']
