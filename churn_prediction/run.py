import sys
import sqlalchemy
import argparse
import json
import os
import pandas as pd

from datetime import datetime, timedelta
from dateutil.parser import parse
from google.oauth2 import service_account
from typing import Dict
from sqlalchemy import func
import logging.config
from utils.config import LABELS, FeatureColumns, CURRENT_MODEL_VERSION, AGGREGATION_FUNCTIONS_w_ALIASES, \
    MIN_TRAINING_DAYS, CURRENT_PIPELINE_VERSION, PROFILE_COLUMNS
from prediction_commons.enums import NormalizedFeatureHandling, ArtifactRetentionMode, ArtifactRetentionCollection
from prediction_commons.db_utils import create_connection
from utils.mysql import get_payment_history_features, get_global_context
from prediction_commons.model import PredictionModel
from utils.config import LOGGING

sys.path.append("../")

# environment variables
from dotenv import load_dotenv
load_dotenv('.env')

from utils.bigquery import get_feature_frame_via_sqlalchemy, insert_daily_feature_frame

# logging
logger = logging.getLogger(__name__)
logging.config.dictConfig(LOGGING)
logger.setLevel(logging.INFO)


class ChurnPredictionModel(PredictionModel):
    def __init__(
            self,
            min_date: datetime = datetime.utcnow() - timedelta(days=31),
            max_date: datetime = datetime.utcnow() - timedelta(days=1),
            moving_window_length: int = 7,
            normalization_handling: NormalizedFeatureHandling = NormalizedFeatureHandling.REPLACE_WITH,
            overwrite_files: bool = True,
            training_split_parameters=None,
            # This applies to all model artifacts that are not part of the flow output
            artifact_retention_mode: ArtifactRetentionMode = ArtifactRetentionMode.DUMP,
            # By default everything gets stored (since we expect most runs to still be in experimental model
            artifacts_to_retain: ArtifactRetentionCollection = ArtifactRetentionCollection.MODEL_TUNING,
            feature_aggregation_functions: Dict[str, sqlalchemy.func] = {'avg': func.avg},
            dry_run: bool = False,
            path_to_model_files: str = None,
            positive_event_lookahead: int = 33,
    ):
        super().__init__(
            outcome_labels=LABELS,
            min_date=min_date,
            max_date=max_date,
            moving_window_length=moving_window_length,
            normalization_handling=normalization_handling,
            overwrite_files=overwrite_files,
            training_split_parameters=training_split_parameters,
            artifact_retention_mode=artifact_retention_mode,
            artifacts_to_retain=artifacts_to_retain,
            feature_aggregation_functions=feature_aggregation_functions,
            dry_run=dry_run,
            path_to_model_files=path_to_model_files,
            positive_event_lookahead=positive_event_lookahead,
            model_record_id='user_id',
        )

        self.model_type = 'churn'
        self.current_model_version = CURRENT_MODEL_VERSION
        self.profile_columns = PROFILE_COLUMNS

        self.feature_columns = FeatureColumns(
            self.feature_aggregation_functions.keys(),
            self.min_date,
            self.max_date
        )

        self.le.fit(list(LABELS.keys()))

    def get_full_user_profiles_by_date(
            self
    ):
        '''
        Requires:
            - min_date
            - max_date
            - moving_window
            - undersampling factor
        Retrieves rolling window user profiles from the db
        using row-wise normalized features
        '''

        self.min_date = self.min_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.max_date = self.max_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.retrieve_feature_frame()
        logger.info(f'  * Query finished, processing retrieved data')

        for column in [column for column in self.feature_columns.return_feature_list()
                       if column not in self.user_profiles.columns
                       and column not in [
                              'clv', 'article_pageviews_count',
                              'sum_paid', 'avg_price'] +
                       [  # Iterate over all aggregation function types
                              f'pageviews_{aggregation_function_alias}'
                              for aggregation_function_alias in self.feature_aggregation_functions.keys()
                       ]
                       ]:
            self.user_profiles[column] = 0.0
        logger.info(f'  * Retrieved initial user profiles frame from DB')

        try:
            self.get_contextual_features_from_mysql()
            self.feature_columns.add_global_context_features()
            logger.info('Successfully added global context features from mysql')
        except Exception as e:
            logger.info(
                f'''Failed adding global context features from mysql with exception:
                {e};
                proceeding with remaining features''')
            # To make sure these columns are filled in case of failure to retrieve
            # We want them appearing in the same order to avoid having to reorder columns
            for column in ['article_pageviews_count', 'sum_paid', 'avg_price']:
                self.user_profiles[column] = 0.0

        try:
            self.get_user_history_features_from_mysql()
            self.feature_columns.add_payment_history_features()
            logger.info('Successfully added user payment history features from mysql')
        except Exception as e:
            logger.info(
                f'''Failed adding payment history features from mysql with exception:
                {e};
                proceeding with remaining features''')
            for column in ['clv']:
                self.user_profiles[column] = 0.0

        self.user_profiles[self.feature_columns.numeric_columns_all].fillna(0.0, inplace=True)
        logger.info('  * Initial data validation success')

    def update_feature_names_from_data(self):
        self.feature_columns = FeatureColumns(
            self.user_profiles['feature_aggregation_functions'].tolist()[0].split(','),
            self.min_date,
            self.max_date
        )

    def get_user_history_features_from_mysql(self):
        '''
        Requires:
            - max_date
        Retrieves clv and days since last subscription from the predplatne database data is then written to the main
        feature frame iterating over rows of payment history features, since each feature frame row might contain
        multiple user ids. Currently there is no logic for when there are multiple user ids, we simply use data
        from the last relevant payment history row.
        '''
        payment_history_features = get_payment_history_features(self.max_date)
        payment_history_features['user_id'] = payment_history_features['user_id'].astype(int).astype(str)
        self.user_profiles = self.user_profiles.merge(
            right=payment_history_features,
            left_on='user_id',
            right_on='user_id',
            how='left'
        )

        self.user_profiles['clv'] = self.user_profiles['clv'].astype(float)
        self.user_profiles['clv'].fillna(0.0, inplace=True)

        return payment_history_features

    def get_contextual_features_from_mysql(self):
        '''
        Requires:
            - user_profiles
        Retrieves & joins daily rolling article pageviews, sum paid, payment count and average price
        '''
        context = get_global_context(
            self.user_profiles['date'].min() - timedelta(days=self.moving_window),
            self.user_profiles['date'].max()
        )
        # We extract these, since we also want global context for the past positives data

        context.index = context['date']
        context.drop('date', axis=1, inplace=True)
        context.index = pd.to_datetime(context.index)
        rolling_context = (context.groupby('date')
                           .fillna(0)  # fill each missing group with 0
                           .rolling(7, min_periods=1)
                           .sum())  # do a rolling sum
        rolling_context.reset_index(inplace=True)
        rolling_context['avg_price'] = rolling_context['sum_paid'] / rolling_context['payment_count']
        rolling_context['date'] = pd.to_datetime(rolling_context['date']).dt.date
        # create str variant of the date columns since we can't join on date in pandas
        rolling_context['date_str'] = rolling_context['date'].astype(str)
        self.user_profiles['date_str'] = self.user_profiles['date'].astype(str)

        self.user_profiles = self.user_profiles.merge(
            right=rolling_context,
            on='date_str',
            how='left',
            copy=False
        )

        self.user_profiles.drop(['date_str', 'date_y'], axis=1, inplace=True)
        self.user_profiles.rename(columns={'date_x': 'date'}, inplace=True)

    def retrieve_feature_frame(
            self
    ):
        '''
        Requires:
            - min_date
            - max_date
            - moving_window
            - normalization_handling
            - feature_columns
        Feature frame applies basic sanitization (Unknown / bool columns transformation) and keeps only users
        that were active a day ago
        '''
        logger.info(f'  * Loading user profiles')
        self.user_profiles = get_feature_frame_via_sqlalchemy(
            self.min_date,
            self.max_date,
            self.moving_window,
            self.positive_event_lookahead
        )

        logger.info(f'  * Processing user profiles')

        if self.user_profiles.empty:
            raise ValueError(f'No data retrieved for {self.min_date} - {self.max_date} aborting model training')

        self.unpack_json_columns()
        self.update_feature_names_from_data()
        self.user_profiles['is_active_on_date'] = self.user_profiles['is_active_on_date'].astype(bool)
        for date_column in ['date', 'outcome_date']:
            self.user_profiles[date_column] = pd.to_datetime(
                self.user_profiles[date_column]
            ).dt.tz_localize(None).dt.date

        self.transform_bool_columns_to_int()
        logger.info('  * Filtering user profiles')
        self.user_profiles = self.user_profiles[self.user_profiles['days_active_count'] >= 1].reset_index(drop=True)

        if self.normalization_handling is not NormalizedFeatureHandling.IGNORE:
            logger.info(f'  * Normalizing user profiles')
            self.introduce_row_wise_normalized_features()

    def upload_predictions(self):
        logger.info(f'Storing predicted data')
        client_secrets_path = os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')
        database = os.getenv('BIGQUERY_PROJECT_ID')
        _, db_connection = create_connection(
            f'bigquery://{database}',
            engine_kwargs={'credentials_path': client_secrets_path}
        )

        credentials = service_account.Credentials.from_service_account_file(
            client_secrets_path,
        )

        self.predictions.to_gbq(
            destination_table=f'{os.getenv("BIGQUERY_DATASET")}.churn_predictions_log',
            project_id=database,
            credentials=credentials,
            if_exists='append'
        )

        self.prediction_job_log = self.predictions[
            ['outcome_date', 'model_version', 'created_at']].head(1)
        self.prediction_job_log['rows_predicted'] = len(self.predictions)
        self.prediction_job_log.rename({'outcome_date': 'date'}, axis=1, inplace=True)

        self.prediction_job_log.to_gbq(
            destination_table=f'{os.getenv("BIGQUERY_DATASET")}.prediction_job_log',
            project_id=database,
            credentials=credentials,
            if_exists='append',
        )

    def retrieve_and_insert(self):
        dates_for_preaggregation = [date for date in pd.date_range(self.min_date, self.max_date)]
        for date in dates_for_preaggregation:
            if len(dates_for_preaggregation) > 1:
                logger.setLevel(logging.ERROR)
            try:
                insert_daily_feature_frame(
                    date,
                    self.moving_window,
                    self.feature_aggregation_functions,
                    meta_columns_w_values={
                        'pipeline_version': CURRENT_PIPELINE_VERSION,
                        'created_at': datetime.utcnow(),
                        'window_days': self.moving_window,
                        'event_lookahead': self.positive_event_lookahead,
                        'feature_aggregation_functions': ','.join(
                            list(self.feature_aggregation_functions.keys())
                        )
                    }
                )

            except Exception as e:
                raise ValueError(f'Failed to preaggregate & upload data for data: {date} with error: {e}')

            logger.setLevel(logging.INFO)
            logger.info(f'Date {date} succesfully aggregated & uploaded to BQ')


def mkdatetime(datestr: str) -> datetime:
    '''
    Parses out a date from input string
    :param datestr:
    :return:
    '''
    try:
        return parse(datestr)
    except ValueError:
        raise ValueError('Incorrect Date String')


if __name__ == "__main__":
    logger.info(f'CHURN PREDICTION')
    parser = argparse.ArgumentParser()
    parser.add_argument('--action',
                        help='Should either be "train" for model training, "preaggregate" to preprocess the data for prediction, or "predict" for actual prediction',
                        type=str)
    parser.add_argument('--min-date',
                        help='Min date denoting from when to fetch data',
                        type=mkdatetime,
                        required=True)
    parser.add_argument('--max-date',
                        help='Max date denoting up to when to fetch data',
                        type=mkdatetime,
                        default=datetime.utcnow() - timedelta(days=1),
                        required=False)
    parser.add_argument('--moving-window-length',
                        help='Lenght for rolling sum windows for user profiles',
                        type=int,
                        default=30,
                        required=False)
    parser.add_argument('--positive-event-lookahead',
                        help='We predict and event is going to occur within k-days after the time of prediction',
                        type=int,
                        default=33,
                        required=False)
    parser.add_argument('--training-split-parameters',
                        help='Speficies split_type (random vs time_based) and split_ratio for train/test split',
                        type=json.loads,
                        default={'split': 'time_based', 'split_ratio': 0.5},
                        required=False)
    parser.add_argument('--model-arguments',
                        help='Parameters for scikit model training',
                        type=json.loads,
                        default={'n_estimators': 250},
                        required=False)
    parser.add_argument('--overwrite-files',
                        help='Bool implying whether newly trained model should overwrite existing one for the same date',
                        type=bool,
                        default=True,
                        required=False)

    args = parser.parse_args()
    args = vars(args)
    if args['min_date'] > args['max_date']:
        raise ValueError('Max date is sooner than the min date')
    if args['action'] == 'train':
        churn_prediction = ChurnPredictionModel(
            min_date=args['min_date'],
            max_date=args['max_date'],
            moving_window_length=args['moving_window_length'],
            overwrite_files=args['overwrite_files'],
            training_split_parameters=args['training_split_parameters'],
            artifact_retention_mode=ArtifactRetentionMode.DROP,
            artifacts_to_retain=ArtifactRetentionCollection.MODEL_RETRAINING,
            positive_event_lookahead=args['positive_event_lookahead']
            )

        churn_prediction.model_training_pipeline(
            model_arguments={'n_estimators': 250}
        )

        metrics = ['precision', 'recall', 'f1_score', 'suport']
        print({metrics[i]: churn_prediction.outcome_frame.to_dict('records')[i] for i in range(0, len(metrics))})
    elif args['action'] == 'predict':
        churn_prediction = ChurnPredictionModel(
            min_date=args['min_date'],
            max_date=args['max_date'],
            moving_window_length=args['moving_window_length'],
            artifact_retention_mode=ArtifactRetentionMode.DROP,
            artifacts_to_retain=ArtifactRetentionCollection.PREDICTION,
            positive_event_lookahead=args['positive_event_lookahead']
        )

        churn_prediction.generate_and_upload_prediction()
    elif args['action'] == 'preaggregate':
        churn_prediction = ChurnPredictionModel(
            min_date=args['min_date'],
            max_date=args['max_date'],
            moving_window_length=args['moving_window_length'],
            artifact_retention_mode=ArtifactRetentionMode.DROP,
            artifacts_to_retain=ArtifactRetentionCollection.MODEL_RETRAINING,
            positive_event_lookahead=args['positive_event_lookahead']
        )
        churn_prediction.pregaggregate_daily_profiles()
