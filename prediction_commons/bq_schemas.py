from google.cloud import bigquery


def rolling_daily_user_profile(id_column: str):
    schema = [
        bigquery.SchemaField('date', 'DATE'),
        bigquery.SchemaField(id_column, 'STRING'),
        bigquery.SchemaField('outcome', 'STRING'),
        bigquery.SchemaField('outcome_date', 'DATE'),
        bigquery.SchemaField('pipeline_version', 'STRING'),
        bigquery.SchemaField('created_at', 'TIMESTAMP'),
        bigquery.SchemaField('window_days', 'INTEGER'),
        bigquery.SchemaField('event_lookahead', 'INTEGER'),
        bigquery.SchemaField('feature_aggregation_functions', 'STRING'),
        bigquery.SchemaField('features__numeric_columns', 'STRING'),
        bigquery.SchemaField('features__profile_numeric_columns_from_json_fields__referer_mediums', 'STRING'),
        bigquery.SchemaField('features__profile_numeric_columns_from_json_fields__categories', 'STRING'),
        bigquery.SchemaField('features__time_based_columns__hour_ranges', 'STRING'),
        bigquery.SchemaField('features__time_based_columns__days_of_week', 'STRING'),
        bigquery.SchemaField('features__categorical_columns', 'STRING'),
        bigquery.SchemaField('features__bool_columns', 'STRING'),
        bigquery.SchemaField('features__numeric_columns_with_window_variants', 'STRING'),
        bigquery.SchemaField('features__device_based_columns', 'STRING')
    ]

    return schema


def models(model_type: str):
    schema = [
        bigquery.SchemaField('train_date', 'DATE'),
        bigquery.SchemaField('min_date', 'DATE'),
        bigquery.SchemaField('max_date', 'DATE'),
        # This resolves to churn, conversion or ltv (lifetime value)
        bigquery.SchemaField(model_type, 'STRING'),
        bigquery.SchemaField('pipeline_version', 'STRING'),
        bigquery.SchemaField('model_version', 'TIMESTAMP'),
        bigquery.SchemaField('window_days', 'INTEGER'),
        bigquery.SchemaField('event_lookahead', 'INTEGER'),
        bigquery.SchemaField('feature_aggregation_functions', 'STRING'),
        bigquery.SchemaField('importances__numeric_columns', 'STRING'),
        bigquery.SchemaField('importances__profile_numeric_columns_from_json_fields__referer_mediums', 'STRING'),
        bigquery.SchemaField('importances__profile_numeric_columns_from_json_fields__categories', 'STRING'),
        bigquery.SchemaField('importances__time_based_columns__hour_ranges', 'STRING'),
        bigquery.SchemaField('importances__time_based_columns__days_of_week', 'STRING'),
        bigquery.SchemaField('importances__categorical_columns', 'STRING'),
        bigquery.SchemaField('importances__bool_columns', 'STRING'),
        bigquery.SchemaField('importances__numeric_columns_with_window_variants', 'STRING'),
        bigquery.SchemaField('importances__device_based_columns', 'STRING')
    ]

    return schema