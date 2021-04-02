from typing import List, Dict
from sqlalchemy import func

CURRENT_MODEL_VERSION = '0.0'

PROFILE_COLUMNS = ['referer_mediums', 'hour_interval_pageviews', 'categories']

CURRENT_PIPELINE_VERSION = '0.0'

EXPECTED_DEVICE_TYPES = ['desktop', 'mobile', 'tablet']

AGGREGATION_FUNCTIONS_w_ALIASES = {
    'count': func.sum,
    'avg': func.avg,
    'min': func.min,
    'max': func.max
}


def sanitize_column_name(column_name: str) -> str:
    if column_name is None:
        return ''
    new_column_name = column_name.replace('-', '_')
    new_column_name = new_column_name.replace('-', '_')
    return new_column_name


def generate_4_hour_interval_column_names():
    hour_ranges = []
    for i in range(0, 24, 4):
        hour_ranges.append(f'pvs_{i}h_{i + 4}h')

    return hour_ranges


class FeatureColumns(object):
    CATEGORICAL_COLUMNS = ['day_of_week']

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

    CONFIG_COLUMNS = [
        'date'
    ]

    BOOL_COLUMNS = [
        'is_active_on_date'
    ]

    def __init__(
            self,
            aggregation_function_aliases,
            aggregation_time
    ):
        self.aggregation_function_aliases = aggregation_function_aliases
        self.aggregation_time = aggregation_time

        # The difference between columns and features with categorical variables we need to create dummy variables out
        # of the columns, categorical_columns hold the names of the columns we unpack and featues are the unpacked
        # features
        self.categorical_columns = self.CATEGORICAL_COLUMNS
        self.categorical_features = []

        # Add one version for each aggregation whenever available
        base_numeric_columns = set()
        device_based_features = set()
        for aggregation_function_alias in aggregation_function_aliases:
            base_numeric_columns.update(
                self.build_numeric_columns_base(
                    aggregation_function_alias
                )
            )

            base_numeric_columns.update(
                list(self.build_derived_metrics_config(aggregation_function_alias))
            )

        device_based_features.update(self.get_device_information_features())

        self.device_based_features = list(device_based_features)
        self.base_numeric_columns = list(base_numeric_columns)
        normalized = False
        self.profile_numeric_columns_from_json_fields = self.build_out_profile_based_column_names(normalized)
        self.time_based_columns = self.generate_all_time_based_column_names(normalized)

        self.numeric_columns = self.base_numeric_columns

        self.numeric_columns_window_variants = self.create_window_variant_permuations(
            self.numeric_columns +
            self.unpack_profile_based_fields(self.profile_numeric_columns_from_json_fields) +
            self.unpack_profile_based_fields(self.time_based_columns)
        )

        self.numeric_columns_all = (
                self.numeric_columns_window_variants +
                self.numeric_columns +
                self.unpack_profile_based_fields(self.profile_numeric_columns_from_json_fields) +
                self.unpack_profile_based_fields(self.time_based_columns) +
                self.device_based_features
        )

    def add_normalized_profile_features_version(self):
        normalized_column_names = self.build_out_profile_based_column_names(True)
        normalized_column_names.update(
            self.generate_all_time_based_column_names(True)
        )

        final_fields = self.create_window_variant_permuations(
            self.unpack_profile_based_fields(normalized_column_names)
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
                self.CATEGORICAL_COLUMNS +
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

    @staticmethod
    def return_normalized_suffix(normalized: bool = False) -> str:
        if normalized is True:
            return '_normalized'
        else:
            return ''

    def generate_all_time_based_column_names(
            self,
            normalized: bool = False
    ) -> Dict[str, List[str]]:
        # Time based features combining day_of_week with hour interval
        suffix = self.return_normalized_suffix(normalized)
        time_based_columns = dict()
        time_based_columns['hour_ranges'] = [
            f'{column}_{aggregation_function_alias}' for column in generate_4_hour_interval_column_names()
            for aggregation_function_alias in self.aggregation_function_aliases
        ]
        time_based_columns['days_of_week'] = [
            f'dow_{dow}_{aggregation_function_alias}{suffix}' for dow in range(0, 7)
            for aggregation_function_alias in self.aggregation_function_aliases
        ]

        # Bigquery has a limit on size of the pipeline requested via a query, this part creates too many
        #  combinations, if we thing these columns might be significant, the way to reintroduce them would be by
        #  and intermediary table
        # time_based_columns['hour_of_day_of_week'] = [
        #     f'dow_{dow}_hours_{hours}_{aggregation_function_alias}{suffix}'
        #     for dow in range(0, 7)
        #     for hours in time_based_columns['hour_ranges']
        #     for aggregation_function_alias in aggregation_function_aliases
        #  ]

        return time_based_columns

    def build_out_profile_based_column_names(
            self,
            normalized: bool = False
    ) -> Dict[str, List[str]]:
        suffix = self.return_normalized_suffix(normalized)
        profile_numeric_columns_from_json_fields = {
            # Referral features
            'referer_mediums': [
                f'referer_mediums_{referral_category}_{aggregation_function_alias}{suffix}'
                for referral_category in self.SUPPORTED_JSON_FIELDS_KEYS['referer_mediums']
                for aggregation_function_alias in self.aggregation_function_aliases
            ],
            # Article category (section) features
            'categories': [
                f'categories_{article_category.replace("-", "_")}_{aggregation_function_alias}{suffix}'
                for article_category in self.SUPPORTED_JSON_FIELDS_KEYS['categories']
                for aggregation_function_alias in self.aggregation_function_aliases
            ]
        }

        return profile_numeric_columns_from_json_fields

    def create_window_variant_permuations(
            self,
            column_list: List[str]
    ) -> List[str]:
        return [
            prefix_suffix_values[0] + original_column + prefix_suffix_values[1] for original_column in
            column_list
            for prefix_suffix_values in self.NUMERIC_COLUMN_WINDOW_NAME_SUFFIXES_AND_PREFIXES
            if original_column != 'days_since_last_active'
        ]

    @staticmethod
    def unpack_profile_based_fields(
            config_dict: Dict[str, List[str]]
    ) -> List[str]:
        return [
            column for column_set in config_dict.values()
            for column in column_set
        ]

    @staticmethod
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

    @staticmethod
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

    def get_device_information_features(
            self
    ):
        return {}

    def extend_categorical_variants(self, categorical_feature_list):
        self.categorical_features.extend(categorical_feature_list)

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


class ModelFeatures:
    def __init__(self):
        self.numeric_features = ['numeric_columns', 'profile_numeric_columns_from_json_fields', 'time_based_columns',
                                 'numeric_columns_with_window_variants']

        self.categorical_features = ['categorical_columns']
        self.bool_features = ['bool_columns']

    # TODO: In order to unify, we'd need to have the same column names across the two tables
    def get_expected_table_column_names(self, source: str = None):
        if source == 'rolling_profiles':
            result = [
                f'features__{column}' for column in
                self.numeric_features + self.categorical_features + self.bool_features
            ]

            result.remove('features__profile_numeric_columns_from_json_fields')
            result.remove('features__time_based_columns')
            result.extend(
                [
                    'features__profile_numeric_columns_from_json_fields__referer_mediums',
                    'features__profile_numeric_columns_from_json_fields__categories',
                    'features__time_based_columns__hour_ranges',
                    'features__time_based_columns__days_of_week'
                ]
            )

            return result
        elif source == 'models':
            return [
                f'importances__{column}' for column in
                self.numeric_features + self.categorical_features + self.bool_features
            ]
        else:
            raise ValueError('Unknown column source')
