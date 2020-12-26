from prediction_commons.utils.config import FeatureColumns, sanitize_column_name, EXPECTED_DEVICE_TYPES

MIN_TRAINING_DAYS = 7
CURRENT_PIPELINE_VERSION = '1.01'
LABELS = {'no_conversion': 'negative', 'shared_account_login': 'positive', 'conversion': 'positive'}
CURRENT_MODEL_VERSION = '1.0'


class ConversionFeatureColumns(FeatureColumns):
    CONFIG_COLUMNS = [
        'date',
        'browser_id',
        'user_ids',
        'outcome',
        'outcome_date',
        'feature_aggregation_functions'
    ]

    CATEGORICAL_COLUMNS = ['device', 'browser', 'os', 'day_of_week']

    BOOL_COLUMNS = [
        'is_desktop',
        'is_mobile',
        'is_tablet',
        'is_active_on_date'
    ]

    def __init__(self, aggregation_function_aliases, aggregation_time):
        super().__init__(aggregation_function_aliases, aggregation_time)


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
