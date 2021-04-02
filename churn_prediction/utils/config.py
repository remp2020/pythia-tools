from prediction_commons.utils.config import FeatureColumns, sanitize_column_name, EXPECTED_DEVICE_TYPES, ModelFeatures

MIN_TRAINING_DAYS = 7
CURRENT_PIPELINE_VERSION = '1.01'
LABELS = {'renewal': 'positive', 'churn': 'negative'}
CURRENT_MODEL_VERSION = '1.0'
# We want to predict x days before expiration, this variable sets x, we use 33 based on 30 days of subscription
# with 2 days for the grace period and 1 more day as a buffer
EVENT_LOOKAHEAD = 33.0


class ChurnFeatureColumns(FeatureColumns):
    CONFIG_COLUMNS = [
        'date',
        'browser_id',
        'user_id',
        'outcome',
        'outcome_date',
        'feature_aggregation_functions'
    ]

    def __init__(self, aggregation_function_aliases, aggregation_time):
        super().__init__(aggregation_function_aliases, aggregation_time)
        self.numeric_columns_subscriptions_base = [
            'total_amount', 'average_amount', 'count_subs', 'paid_subs',
            'free_subs', 'number_of_days_since_the_first_paid_sub', 'average_gap',
            'max_gap', 'total_days_paid_sub', 'total_days_free_sub',
            'upgraded_subs', 'discount_subs'
        ]

        suffixes = ['last', 'previous', 'diff']

        self.numeric_columns_subscriptions_derived = [
            f'amount_{suffix}' for suffix in suffixes
        ]

        self.numeric_columns_all.extend(
            self.numeric_columns_subscriptions_base + self.numeric_columns_subscriptions_derived
        )

        self.categorical_columns_subscriptions = [
                f'{column}_{suffix}' for column in
                [
                    'length', 'is_recurrent', 'is_recurrent_charge',
                    'web_access_level', 'sub_print_access', 'sub_print_friday_access'
                ]
                for suffix in suffixes
            ]

        self.categorical_columns.extend(
            self.categorical_columns_subscriptions
        )

    def get_device_information_features(
            self
    ):
        from .bigquery import ChurnFeatureBuilder
        churn_feature_builder = ChurnFeatureBuilder(
            # None of these parameters matter for this particular flow
            aggregation_time=self.aggregation_time,
            moving_window_length=30,
            feature_aggregation_functions=None
        )
        device_brands = churn_feature_builder.get_prominent_device_list()

        # We will need to get the device names for differing handling in regards to rolling window calculations,
        # the base name won't have any aggregation

        device_brands = [
            f'{sanitize_column_name(device_brand)}_device_brand_sum'
            for device_brand in device_brands
        ]

        if not device_brands:
            raise ValueError('Device Brands Feature List Empty')

        device_types = [f'{device_type}_device_sum'
                        for device_type in EXPECTED_DEVICE_TYPES]

        return device_brands + device_types


class ChurnModelFeatures(ModelFeatures):
    def __init__(self):
        super().__init__()

        self.numeric_features.extend(
            [
                'device_based_columns', 'numeric_columns_subscriptions_base', 'numeric_columns_subscriptions_derived',
                'categorical_columns_subscriptions'
            ]
        )
