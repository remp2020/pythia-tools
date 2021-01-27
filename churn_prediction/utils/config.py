from prediction_commons.utils.config import FeatureColumns, sanitize_column_name, EXPECTED_DEVICE_TYPES, ModelFeatures

MIN_TRAINING_DAYS = 7
CURRENT_PIPELINE_VERSION = '1.01'
LABELS = {'renewal': 'positive', 'churn': 'negative'}
CURRENT_MODEL_VERSION = '1.0'


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

        self.numeric_features.append('device_based_columns')
