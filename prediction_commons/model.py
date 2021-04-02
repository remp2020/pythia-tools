import sys

from google.cloud import bigquery
from google.oauth2 import service_account
from imblearn.under_sampling import RandomUnderSampler
import json
import os
import re
import pandas as pd
import numpy as np
import sqlalchemy
import joblib

from datetime import datetime, timedelta
from typing import List, Dict, Callable
from dateutil.parser import parse
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_recall_fscore_support
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
from sqlalchemy import func

from prediction_commons.utils.config import ModelFeatures
from prediction_commons.utils.bigquery import get_model_meta
from prediction_commons.utils.enums import SplitType, NormalizedFeatureHandling, ArtifactRetentionMode, \
    ArtifactRetentionCollection, ModelArtifacts, DataRetrievalMode
from prediction_commons.utils.data_transformations import row_wise_normalization
from prediction_commons.utils.db_utils import TableHandler, create_connection
from prediction_commons.utils.bq_schemas import rolling_daily_model_record_level_profile

sys.path.append("../")

# environment variables
from dotenv import load_dotenv

load_dotenv('.env')

# logging
import logging.config
from prediction_commons.utils.config import LOGGING

logger = logging.getLogger(__name__)
logging.config.dictConfig(LOGGING)
logger.setLevel(logging.INFO)


class PredictionModel(object):
    model_features = ModelFeatures()

    def __init__(
            self,
            outcome_labels: List[str],
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
            dry_run: bool = True,
            path_to_model_files: str = None,
            model_record_level: str = '',
    ):
        def create_util_columns():
            util_columns = ['outcome', 'feature_aggregation_functions', 'date', 'outcome_date', self.id_column]

            return util_columns

        self.model_type = 'generic'
        self.feature_columns = None
        self.current_model_version = None
        self.profile_columns = None
        self.model_record_level = model_record_level
        self.id_column = f'{model_record_level}_id'
        self.util_columns = create_util_columns()
        self.min_date = min_date
        self.max_date = max_date
        self.moving_window = moving_window_length
        self.overwrite_files = overwrite_files
        self.user_profiles = None
        self.normalization_handling = normalization_handling
        self.feature_aggregation_functions = feature_aggregation_functions
        self.category_list_dict = {}
        self.le = LabelEncoder()
        self.outcome_labels = outcome_labels
        self.X_train = pd.DataFrame()
        self.X_test = pd.DataFrame()
        self.Y_train = pd.Series()
        self.Y_test = pd.Series()
        self.sampling_function = RandomUnderSampler()
        self.scaler = MinMaxScaler()
        self.model_date = None
        self.training_split_parameters = training_split_parameters
        self.model = None
        self.outcome_frame = None
        self.scoring_date = datetime.utcnow()
        self.prediction_data = pd.DataFrame()
        self.predictions = pd.DataFrame()
        self.artifact_retention_mode = artifact_retention_mode
        self.artifacts_to_retain = [artifact.value for artifact in artifacts_to_retain.value]
        self.path_to_model_files = os.getenv('PATH_TO_MODEL_FILES', path_to_model_files)
        if not os.path.exists(self.path_to_model_files):
            os.mkdir(self.path_to_model_files)
        self.negative_outcome_frame = pd.DataFrame()
        self.variable_importances = pd.Series()
        self.dry_run = dry_run
        self.prediction_job_log = None
        self.model_meta = pd.DataFrame()

    def artifact_handler(self, artifact: ModelArtifacts):
        '''
        :param artifact:
        :return:
        Requires::w
            - artifact_retention_mode
            - path_to_model_files
            - if model artifact
                - model_date
                - model
            - else (case for all data frame artifacts):
                - min_date
                - max_date
        A function that handles model artifacts that we don't need as an output currently by either
        storing and dumping or by straight up dumping them
        '''
        if self.artifact_retention_mode == ArtifactRetentionMode.DUMP:
            if artifact == ModelArtifacts.MODEL:
                joblib.dump(
                    self.model,
                    f'{self.path_to_model_files}{self.model_type}_'
                    f'{self.model_record_level}_model_{self.model_date}.pkl'
                )
            else:
                getattr(self, artifact.value, pd.DataFrame()) \
                    .to_csv(
                    f'{self.path_to_model_files}{self.model_type}_{self.model_record_level}_artifact_'
                    f'{artifact.value}_{self.min_date}_{self.max_date}.csv'
                )
                logger.info(f'  * {artifact.value} artifact dumped to {self.path_to_model_files}')
        delattr(self, artifact.value)
        logger.info(f'  * {artifact.value} artifact dropped')

    def get_full_user_profiles_by_date(self, data_retrieval_mode: DataRetrievalMode):
        pass

    def unpack_feature_frame(
            self
    ):
        '''
        Requires:
            - normalization_handling
            - feature_columns
        Feature frame applies basic sanitization (Unknown / bool columns transformation) and keeps only users
        that were active a day ago
        '''
        logger.info(f'  * Processing user profiles')

        if self.user_profiles.empty:
            raise ValueError(f'No data retrieved for {self.min_date} - {self.max_date} aborting model training')

        self.unpack_json_columns()

        for column in [column for column in self.feature_columns.return_feature_list()
                       if column not in self.user_profiles.columns
                       and column not in ['clv', 'article_pageviews_count', 'sum_paid', 'avg_price'] +
                          [  # Iterate over all aggregation function types
                              f'pageviews_{aggregation_function_alias}'
                              for aggregation_function_alias in self.feature_aggregation_functions.keys()
                          ]
                       ]:
            self.user_profiles[column] = 0.0

        self.update_feature_names_from_data()
        self.user_profiles['is_active_on_date'] = self.user_profiles['is_active_on_date'].astype(bool)
        for date_column in ['date', 'outcome_date']:
            self.user_profiles[date_column] = pd.to_datetime(
                self.user_profiles[date_column]
            ).dt.tz_localize(None).dt.date

        self.transform_bool_columns_to_int()

        if self.normalization_handling is not NormalizedFeatureHandling.IGNORE:
            logger.info(f'  * Normalizing user profiles')
            self.introduce_row_wise_normalized_features()

        self.user_profiles[self.feature_columns.numeric_columns_all].fillna(0.0, inplace=True)

    def update_feature_names_from_data(self):
        pass

    def introduce_row_wise_normalized_features(self):
        '''
        Requires:
            - user_profiles
            - normalization_handling
            - feature_columns
        Adds normalized profile based features (normalized always within group, either replacing the original profile based
        features in group, or adding them onto the original dataframe
        '''
        self.add_missing_json_columns()
        column_sets = list(self.feature_columns.profile_numeric_columns_from_json_fields.values()) + \
                      [time_column_variant for time_column_variant in self.feature_columns.time_based_columns.values()]
        for column_set in column_sets:
            self.user_profiles[column_set] = self.user_profiles[column_set].astype(float)
            normalized_data = pd.DataFrame(row_wise_normalization(np.array(self.user_profiles[column_set])))
            normalized_data.fillna(0.0, inplace=True)
            normalized_data.columns = column_set

            if self.normalization_handling is NormalizedFeatureHandling.REPLACE_WITH:
                self.user_profiles.drop(column_set, axis=1, inplace=True)
            elif self.normalization_handling is NormalizedFeatureHandling.ADD:
                self.feature_columns.add_normalized_profile_features_version(
                    list(self.feature_aggregation_functions.keys())
                )
            else:
                raise ValueError('Unknown normalization handling parameter')

            columns_after_concat = list(self.user_profiles) + list(normalized_data)
            self.user_profiles = pd.concat(
                [
                    self.user_profiles,
                    normalized_data
                ],
                axis=1,
                ignore_index=True
            )

            self.user_profiles.columns = columns_after_concat

        logger.info('  * Feature normalization success')

    def add_missing_json_columns(self):
        '''
        Requires:
            - user_profiles
            - feature_columns
        In case some of the columns created from json columns are not available i.e. a section hasn't occured
        in the past few days, we fill it with 0.0 to avoid mismatch between datasets. The only way to add / remove
        a section should be via the config file in utils
        '''
        potential_columns = [
            column for column_list in
            self.feature_columns.profile_numeric_columns_from_json_fields.values()
            for column in column_list
        ]

        for missing_json_column in set(potential_columns) - set(self.user_profiles.columns):
            self.user_profiles[missing_json_column] = 0.0

    def unpack_json_columns(self):
        for column in self.model_features.get_expected_table_column_names('rolling_profiles'):
            expansion_frame = self.user_profiles[column].apply(json.loads)
            # We need special handling for jsons with len of 1
            if expansion_frame.apply(len).max() == 1:
                single_key = next(iter(expansion_frame.iloc[0].keys()))
                expansion_frame = pd.DataFrame(expansion_frame.apply(lambda x: x[single_key]))
                expansion_frame.columns = [single_key]
            else:
                expansion_frame = expansion_frame.apply(pd.Series)
            if expansion_frame.columns[0] in self.feature_columns.numeric_columns_all:
                expansion_frame = expansion_frame.astype(float)
            self.user_profiles = pd.concat(
                [
                    self.user_profiles,
                    expansion_frame
                ],
                axis=1,
            )
            self.user_profiles.drop(column, axis=1, inplace=True)

    def transform_bool_columns_to_int(self):
        '''
        Requires:
            - user_profiles
        Transform True / False columns into 0/1 columns
        '''
        for column in [column for column in self.feature_columns.BOOL_COLUMNS]:
            self.user_profiles[column] = self.user_profiles[column].apply(
                lambda value: True if value == 't' else False).astype(int)

    def generate_category_list_dict(self):
        '''
        Requires:
            - user_profiles
        Create lists of individual category variables for consistent encoding between train set, test set and
        prediction set
        '''
        for column in self.feature_columns.categorical_columns:
            self.category_list_dict[column] = list(self.user_profiles[column].unique()) + ['Unknown']

    def encode_unknown_categories(self, data: pd.Series):
        '''
        In train and predictiom set, we mighr encounter categorie that weren't present in the test set, in order
        to allow for the prediction algorithm to handle these, we create an Unknown category
        :param data:
        :return:
        '''
        data[~(data.isin(self.category_list_dict[data.name]))] = 'Unknown'

        return data

    def generate_dummy_columns_from_categorical_column(self, data: pd.Series) -> pd.DataFrame:
        '''
        Requires:
            - category_lists_dict
            - categorical column as an input
        Generate 0/1 (values) columns from categorical columns handling the logic for Others and Unknown categories.
        Always drops the Unknown columns (since we only need k-1 columns for k categories)
        :param data: A column to encode
        :return:
        '''
        data = self.encode_unknown_categories(data.copy())
        dummies = pd.get_dummies(pd.Categorical(data, categories=self.category_list_dict[data.name]))
        dummies.columns = [data.name + '_' + dummy_column for dummy_column in dummies.columns]
        dummies.drop(columns=data.name + '_Unknown', axis=1, inplace=True)
        dummies.index = data.index

        return dummies

    def replace_dummy_columns_with_dummies(self, data: pd.DataFrame) -> pd.DataFrame:
        '''
        Requires:
            - category_lists_dict
        Generates new dummy columns and drops the original categorical ones
        :param data:
        :param category_lists_dict:
        :return:
        '''
        for column in self.feature_columns.categorical_columns:
            dummies = self.generate_dummy_columns_from_categorical_column(data[column])
            self.feature_columns.extend_categorical_variants(dummies.columns)
            data = pd.concat(
                [
                    data,
                    dummies
                ],
                axis=1)
            data.drop(labels=column, axis=1, inplace=True)

        return data

    def create_train_test_transformations(self):
        '''
        Requires:
            - model_date
            - training_split_parameters
            - user_profiles
            - feature_columns
            - scaler
            - artifacts_to_retain
            - artifact_retention_mode
        Splits train / test applying dummification and scaling to their variables
        '''
        self.model_date = self.user_profiles['outcome_date'].max() + timedelta(days=1)
        split = SplitType(self.training_split_parameters['split'])
        split_ratio = self.training_split_parameters['split_ratio']
        
        if split is SplitType.RANDOM:
            indices = np.random.RandomState(seed=42).permutation(
                self.user_profiles[self.user_profiles['outcome_date'] >= self.min_date.date()].index
            )
            indices = pd.Index(indices)
            train_cutoff = int(round(len(indices) * split_ratio, 0))
        else:
            indices = self.user_profiles[
                self.user_profiles['outcome_date'] >= self.min_date.date()
                ].sort_values('date').index
            train_cutoff = int(round(len(indices) * split_ratio, 0))

        train_indices = indices[:train_cutoff]
        train_indices = train_indices.union(
                self.user_profiles[self.user_profiles['outcome_date'] < self.min_date.date()].index
        )
        test_indices = indices[train_cutoff:]
        self.X_train = self.user_profiles.loc[train_indices].drop(columns=self.util_columns)
        self.X_test = self.user_profiles.loc[test_indices].drop(columns=self.util_columns)
        self.generate_category_list_dict()

        with open(
                f'{self.path_to_model_files}{self.model_type}_{self.model_record_level}_category_lists_'
                f'{self.model_date}.json', 'w') as outfile:
            json.dump(self.category_list_dict, outfile)

        self.X_train = self.replace_dummy_columns_with_dummies(self.X_train)

        logger.info('  * Dummy variables generation success')

        self.Y_train = self.user_profiles.loc[train_indices, 'outcome'].sort_index()
        self.Y_test = self.user_profiles.loc[test_indices, 'outcome'].sort_index()

        self.feature_columns.numeric_columns_all.sort()

        X_train_numeric = self.user_profiles.loc[
            train_indices,
            self.feature_columns.numeric_columns_all
        ].fillna(0.0)

        self.scaler = MinMaxScaler(feature_range=(0, 1)).fit(X_train_numeric)

        X_train_numeric = pd.DataFrame(self.scaler.transform(X_train_numeric), index=train_indices,
                                       columns=self.feature_columns.numeric_columns_all).sort_index()

        logger.info('  * Numeric variables handling success')

        self.X_train = pd.concat(
            [
                X_train_numeric.sort_index(),
                self.X_train[self.feature_columns.categorical_features + self.feature_columns.BOOL_COLUMNS].sort_index()
            ],
            axis=1
        )

        self.X_train = self.sort_columns_alphabetically(self.X_train)

        joblib.dump(
            self.scaler,
            f'{self.path_to_model_files}{self.model_type}_{self.model_record_level}_scaler_{self.model_date}.pkl'
        )

        if ModelArtifacts.USER_PROFILES.value not in self.artifacts_to_retain:
            self.artifact_handler(ModelArtifacts.USER_PROFILES)

    def delete_existing_model_file_for_same_date(self, filename: str) -> datetime.date:
        '''
        Requires:
            - model_date
            - path_to_model_files
        Deletes model files should they already exist for the given date
        '''
        if filename != 'category_lists':
            suffix = 'pkl'
        else:
            suffix = 'json'

        if self.path_to_model_files in os.listdir(None):
            if f'{self.model_type}_{self.model_record_level}_scaler_{self.model_date}.pkl' in os.listdir(
                    None if self.path_to_model_files == '' else self.path_to_model_files):
                os.remove(f'{self.path_to_model_files}{self.model_type}_'
                          f'{self.model_record_level}_{filename}_{self.model_date}.{suffix}')

    def preprocess_and_train(
            self,
            model_function,
            model_arguments
    ):
        '''
        Requires:
            - user_profiles
            - model_arguments
            - artifacts_to_retain
            - artifact_retention_mode
        Trains a new model given a full dataset
        '''
        # This is necessary in case we're re-using the data, in some flows the outcome gets rewritten by its numeric
        # encoding and the encoding itself would fail
        if 0 not in self.user_profiles['outcome'].unique():
            self.user_profiles['outcome'] = self.le.transform(self.user_profiles['outcome'])

        self.create_train_test_transformations()
        self.train_model(model_function, model_arguments)

    def train_model(
            self,
            model_function,
            model_arguments
    ):
        self.X_train.fillna(0.0, inplace=True)
        self.X_test.fillna(0.0, inplace=True)

        logger.info('  * Commencing model training')

        classifier_instance = model_function(**model_arguments)
        self.model = classifier_instance.fit(self.X_train, self.Y_train)

        logger.info('  * Model training complete, generating outcome frame')

        try:
            self.variable_importances = pd.Series(
                data=self.model.feature_importances_,
                index=self.X_train.columns
            )
        except:
            # This handles parameter tuning, when some model types may not have variable importance
            self.variable_importances = pd.Series()

        self.outcome_frame = self.create_outcome_frame(
            {'train': self.Y_train, },
            {'train': self.model.predict(self.X_train), },
            self.outcome_labels,
            self.le
        )

        self.upload_model_meta()

        if self.training_split_parameters['split_ratio'] < 1.0:
            self.collect_accuracies_for_test()

        logger.info('  * Outcome frame generated')

    def collect_accuracies_for_test(self):
        self.batch_predict(self.X_test)

        test_outcome_frame = self.create_outcome_frame(
            {'test': self.predictions['outcome']},
            {'test': self.predictions['predicted_outcome']},
            self.outcome_labels,
            self.le
        )

        self.outcome_frame = pd.concat(
            [
                self.outcome_frame,
                test_outcome_frame
            ],
            axis=1
        )

    @staticmethod
    def create_outcome_frame(
            labels_actual: Dict[str, np.array],
            labels_predicted: Dict[str, np.array],
            outcome_labels,
            label_encoder
    ):
        if len(labels_actual) != len(labels_predicted):
            raise ValueError('Trying to pass differing lengths of actual and predicted data')
        elif labels_actual.keys() != labels_predicted.keys():
            raise ValueError('Unaligned number of outcome sets provided')

        def format_outcome_frame(
                sets_in_outcome: List[str] = ['train', 'test']
        ):
            '''
            Takes a given outcome frame and polishes the row & column names
            :param outcome_frame:
            :param label_range:
            :param label_encoder:
            :param sets_in_outcome
            :return:
            '''
            train_outcome_columns = (
                [str(label) + '_train' for label in outcome_frame.columns[0:len(label_range)]]
                if 'train' in sets_in_outcome
                else []
            )

            test_outcome_columns = (
                [
                    str(label) + '_test' for label in outcome_frame.columns[
                                                      # We either have 6 columns (3 for train and 3 for test) or 3
                                                      # (test only), therefore we need to adjust indexing
                                                      len(label_range) * (len(sets_in_outcome) - 1):(
                                                              len(sets_in_outcome) * len(label_range))]
                ]
                if 'test' in sets_in_outcome
                else []
            )

            outcome_frame.columns = train_outcome_columns + test_outcome_columns

            for i in label_range:
                outcome_frame.columns = [re.sub(str(i), label_encoder.inverse_transform([i])[0], column)
                                         for column in outcome_frame.columns]

            outcome_frame.index = ['precision', 'recall', 'f-score', 'support']

        label_range = list(range(0, len(outcome_labels)))
        outcome_frame = pd.DataFrame()
        for i in range(0, len(labels_actual)):
            actual = list(labels_actual.values())[i]
            predicted = list(labels_predicted.values())[i]
            outcome_frame_partial = pd.DataFrame(
                list(
                    precision_recall_fscore_support(
                        actual,
                        predicted,
                        labels=label_range
                    )
                )
            )

            if outcome_frame.empty:
                outcome_frame = outcome_frame_partial
            else:
                outcome_frame = pd.concat(
                    [
                        outcome_frame,
                        outcome_frame_partial
                    ],
                    axis=1
                )

        format_outcome_frame(
            sets_in_outcome=list(labels_actual.keys())
        )

        return outcome_frame

    def model_training_pipeline(
            self,
            model_function: Callable = RandomForestClassifier,
            model_arguments: Dict = {'n_estimators': 250},
            sampling_function: Callable = None
    ):
        '''
        Requires:
            - model_arguments
            - artifacts_to_retain
            - artifact_retention_mode
            - min_date
            - max_date
            - moving_window
            - normalization_handling
            - feature_columns
            - model_date
            - path_to_model_files
        Pipeline that outputs a trained model and it's accuracy measures
        '''
        logger.info(f'Executing training pipeline')
        # Make sure we have enough days for training, this statement is behind the condition since sometimes
        # we might be reusing training data from a previous run
        self.get_full_user_profiles_by_date(DataRetrievalMode.MODEL_TRAIN_DATA)

        # Some users have no outcome due to having renewed via a non-payment option, thus we need to drop them
        # for the purpose of training a model
        self.user_profiles = self.user_profiles[~self.user_profiles['outcome'].isna()]

        if sampling_function:
            try:
                self.sampling_function = sampling_function(n_jobs=-1)
            except Exception as e:
                if e == "__init__() got an unexpected keyword argument 'n_jobs'":
                    self.sampling_function = sampling_function()
                else:
                    raise TypeError(f'Failed initializing sampler with error: {e}')

        self.undersample_majority_class()
        self.unpack_feature_frame()

        if self.overwrite_files:
            for model_file in ['category_lists', 'scaler', 'model']:
                self.delete_existing_model_file_for_same_date(model_file)

        self.preprocess_and_train(
            model_function,
            model_arguments
        )

        logger.info(f'Training ready, dumping to file')

        joblib.dump(
            self.model,
            f'{self.path_to_model_files}{self.model_type}_{self.model_record_level}_model_{self.model_date}.pkl'
        )

        logger.info(f'Saved to {self.path_to_model_files}model_{self.model_date}.pkl')

        self.remove_model_training_artefacts()

    def upload_model_meta(self):
        logger.info(f'Storing model metadata')
        client_secrets_path = os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')
        database = os.getenv('BIGQUERY_PROJECT_ID')
        _, db_connection = create_connection(
            f'bigquery://{database}',
            engine_kwargs={'credentials_path': client_secrets_path}
        )

        credentials = service_account.Credentials.from_service_account_file(
            client_secrets_path,
        )

        self.model_meta = pd.DataFrame(
            {
                'train_date': datetime.utcnow(),
                'min_date': self.min_date,
                # We need to take max date from train since that is the max model date
                'max_date': self.max_date,
                'model_type': self.model_type,
                'model_version': self.current_model_version,
                'window_days': self.moving_window,
            },
            index=[0]
        )

        for feature_set_name, feature_set in {
            'numeric_columns': self.feature_columns.numeric_columns,
            'profile_numeric_columns_from_json_fields': self.feature_columns.profile_numeric_columns_from_json_fields,
            'time_based_columns': self.feature_columns.time_based_columns,
            'categorical_columns': [
                column for column in self.variable_importances.index
                for category in self.feature_columns.categorical_columns if f'{category}_' in column
            ],
            'bool_columns': self.feature_columns.BOOL_COLUMNS,
            'numeric_columns_with_window_variants': self.feature_columns.numeric_columns_window_variants,
            'device_based_columns': self.feature_columns.device_based_features
        }.items():
            if isinstance(feature_set, list):
                feature_set_elements = feature_set

                self.model_meta[f'importances__{feature_set_name}'] = str(
                    self.variable_importances[
                        feature_set_elements
                    ].to_dict()
                )
            elif isinstance(feature_set, dict):
                feature_set_elements = sum(feature_set.values(), [])
                self.model_meta[f'importances__{feature_set_name}'] = str(
                    self.variable_importances[
                        feature_set_elements
                    ].to_dict()
                )

        self.model_meta.to_gbq(
            destination_table=f'{os.getenv("BIGQUERY_DATASET")}.models',
            project_id=database,
            credentials=credentials,
            if_exists='append'
        )

        logger.info(f'Model metadata stored')

    def remove_model_training_artefacts(self):
        for artifact in [
            ModelArtifacts.TRAIN_DATA_FEATURES, ModelArtifacts.TRAIN_DATA_OUTCOME,
            ModelArtifacts.TEST_DATA_FEATURES, ModelArtifacts.TEST_DATA_OUTCOME,
        ]:
            if artifact.value not in self.artifacts_to_retain:
                self.artifact_handler(artifact)

    def load_model_related_constructs(self):
        '''
        Requires:
            - path_to_model_files
            - scoring_date
        Serves for the prediction pipeline in order to load model & additional transformation config objects
        '''

        model_definition_prefix = f'{self.model_type}_{self.model_record_level}'
        model_related_file_list = [
            file for file in os.listdir(self.path_to_model_files)
            if model_definition_prefix in file
        ]

        last_model_related_files = {}
        for model_related_file in ['category_lists', 'scaler', 'model']:
            non_date_file_name_part = f'{model_definition_prefix}_{model_related_file}_|.json|.pkl|None'

            last_file_date = {
                parse(re.sub(non_date_file_name_part, '', filename)):
                abs(parse(re.sub(non_date_file_name_part, '', filename)).date() - self.scoring_date.date())
                for filename in model_related_file_list
                if re.search(f'{model_related_file}_', filename)
            }

            last_file_date = [date for date, diff in last_file_date.items() if diff == min(last_file_date.values())][0]
            last_model_related_files[model_related_file] = last_file_date.date()
        if len(set(last_model_related_files.values())) > 1:
            raise ValueError(f'''Unaligned model file dates
                category_list date: {last_model_related_files['category_lists']}
                scaler date: {last_model_related_files['scaler']}
                model date: {last_model_related_files['model']}
                'variable importances': {last_model_related_files['variable_importances']}
                ''')

        if not self.path_to_model_files:
            self.path_to_model_files = ''
        with open(self.path_to_model_files +
                  f'{model_definition_prefix}_category_lists_' + str(last_model_related_files['category_lists']) + '.json', 'r') as outfile:
            self.category_list_dict = json.load(outfile)

        self.scaler = joblib.load(f"{self.path_to_model_files}{model_definition_prefix}_scaler_{str(last_model_related_files['scaler'])}.pkl")
        self.model = joblib.load(f"{self.path_to_model_files}{model_definition_prefix}_model_{str(last_model_related_files['model'])}.pkl")

        logger.info('  * Model constructs loaded')

    def batch_predict(self, data):
        '''
        Requires:
            - min_date
            - max_date
            - moving_window
            - normalization_handling
            - feature_columns
            - model_date
            - path_to_model_files
            - scoring_date
            - label_encoder
        Outputs predictions for a given time period
        '''
        logger.info('  * Preparing data for prediction')

        if self.model is None:
            self.load_model_related_constructs()

        numeric_columns_train = []
        categorical_columns_train = []

        if self.model_meta.empty:
            self.model_meta = get_model_meta(
                self.min_date,
                self.model_type,
                self.current_model_version,
                self.moving_window
            )

        for column in self.model_features.get_expected_table_column_names('models'):
            feature_set = column.replace('importances__', '')
            if feature_set in self.model_features.numeric_features:
                expected_features = json.loads(
                    self.model_meta[column].str.replace("'", '"').values[0]
                )

                numeric_columns_train.extend(list(expected_features.keys()))
            elif feature_set in self.model_features.categorical_features:
                expected_features = json.loads(
                    self.model_meta[column].str.replace("'", '"').values[0]
                )

                categorical_columns_train.extend(list(expected_features.keys()))

        numeric_columns_train = set(numeric_columns_train)
        categorical_columns_train = set(categorical_columns_train)

        # Add columns present in train, missing in predict
        for column in numeric_columns_train.difference(set(self.feature_columns.numeric_columns_all)):
            data[column] = 0.0
            self.feature_columns.numeric_columns_all.append(column)

        # Remove columns present in predict, missing in train
        for column in set(self.feature_columns.numeric_columns_all).difference(numeric_columns_train):
            data.drop(column, axis=1, inplace=True)
            self.feature_columns.numeric_columns_all.remove(column)

        self.feature_columns.numeric_columns_all.sort()
        self.scaler.transform(
            data[self.feature_columns.numeric_columns_all]
        )
        feature_frame_numeric = pd.DataFrame(
            self.scaler.transform(
                data[
                    self.feature_columns.numeric_columns_all]
            ),
            index=data.index,
            columns=self.feature_columns.numeric_columns_all).sort_index()

        self.prediction_data = pd.concat(
            [
                feature_frame_numeric.sort_index(),
                data[self.feature_columns.BOOL_COLUMNS + self.feature_columns.categorical_columns].sort_index()
            ],
            axis=1
        )

        self.prediction_data = self.replace_dummy_columns_with_dummies(self.prediction_data)

        for categorical_column in self.category_list_dict.keys():
            for categorical_column_value in self.category_list_dict[categorical_column]:
                categorical_feature = f'{categorical_column}_{categorical_column_value}'
                if categorical_feature not in self.prediction_data \
                        and categorical_feature in categorical_columns_train:
                    self.prediction_data[categorical_feature] = 0

        self.prediction_data = self.sort_columns_alphabetically(self.prediction_data)
        self.prediction_data.fillna(0.0, inplace=True)
        predictions = pd.DataFrame(self.model.predict_proba(self.prediction_data), index=self.prediction_data.index)
        logger.info('  * Prediction generation success, handling artifacts')

        label_range = range(len(self.outcome_labels))

        for i in label_range:
            predictions.columns = [re.sub(str(i), self.le.inverse_transform([i])[0] + '_probability', str(column))
                                   for column in predictions.columns]
        predicted_outcomes = self.model.predict(self.prediction_data)
        # We are adding outcome only for the sake of the batch test approach, we'll be dropping it in the actual
        # prediction pipeline. Since we're also now using batch predict for train / test, we won't be adding the id
        # columns back in based on whether that's the case
        if self.X_train.empty:
            self.predictions = pd.concat(
                [
                    data[['date', self.id_column, 'outcome', 'outcome_date']],
                    predictions
                ],
                axis=1
            )
            predicted_outcomes = self.le.inverse_transform(predicted_outcomes)
        else:  # We still need the outcome column to collect test accuracies if we're in training
            self.predictions = pd.concat(
                [
                    predictions.sort_index(),
                    pd.DataFrame(self.Y_test, columns=['outcome'], index=self.Y_test.index)
                ],
                axis=1
            )
        self.predictions['predicted_outcome'] = predicted_outcomes

    @staticmethod
    def sort_columns_alphabetically(df):
        column_order = list(df.columns)
        column_order.sort()
        df = df[column_order]

        return df

    def upload_predictions(self):
        pass

    def generate_and_upload_prediction(self):
        '''
        Requires:
            - min_date
            - max_date
            - moving_window
            - normalization_handling
            - feature_columns
            - model_date
            - path_to_model_files
            - scoring_date
            - label_encoder
        Generates outcome prediction for churn and uploads them to the DB
        '''
        logger.info(f'Executing prediction generation')
        self.get_full_user_profiles_by_date(DataRetrievalMode.PREDICT_DATA)
        self.unpack_feature_frame()
        self.artifact_retention_mode = ArtifactRetentionMode.DROP

        logger.setLevel(logging.INFO)

        self.batch_predict(self.user_profiles)
        logging.info('  * Generatincg predictions')

        self.predictions['model_version'] = self.current_model_version
        self.predictions['created_at'] = datetime.utcnow()

        # Dry run tends to be used for testing new models, so we want to be able to calculate accuracy metrics
        if not self.dry_run:
            self.predictions.drop('outcome', axis=1, inplace=True)
            self.upload_predictions()
        else:
            # Sometimes we're missing the ground truth label due to holes in the logic for outcome resolution
            self.predictions = self.predictions[~self.predictions['outcome'].isna()]
            actual_labels = {'test': self.le.transform(self.predictions['outcome'])}
            predicted_labels = {'test': self.le.transform(self.predictions['predicted_outcome'])}

            self.outcome_frame = self.create_outcome_frame(
                actual_labels,
                predicted_labels,
                self.outcome_labels,
                self.le
            )

        logger.info('Predictions are now ready')

        for artifact in [ModelArtifacts.MODEL, ModelArtifacts.PREDICTION_DATA, ModelArtifacts.USER_PROFILES]:
            self.artifact_handler(artifact)

    def retrieve_and_insert(self):
        pass

    @staticmethod
    def handle_profiles_table():
        pass

    def pregaggregate_daily_profiles(
            self
    ):

        self.handle_table(
            rolling_daily_model_record_level_profile(self.id_column),
            f'rolling_daily_{self.model_record_level}_profile'
        )
        logger.info(f'Starting with preaggregation for date range {self.min_date.date()} - {self.max_date.date()}')
        self.retrieve_and_insert()

    def undersample_majority_class(self):
        sampler = self.sampling_function
        Y = self.user_profiles['outcome']
        self.user_profiles.drop('outcome', axis=1, inplace=True)
        X_undersampled, Y_undersampled = sampler.fit_resample(
            self.user_profiles,
            Y
        )

        self.user_profiles = pd.concat(
            [
                X_undersampled,
                Y_undersampled
            ],
            axis=1
        )
    
    @staticmethod
    def handle_table(
            schema: List[bigquery.SchemaField],
            table_name: str
    ):
        client_secrets_path = os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')
        database = os.getenv('BIGQUERY_PROJECT_ID')
        _, db_connection = create_connection(
            f'bigquery://{database}',
            engine_kwargs={'credentials_path': client_secrets_path}
        )

        credentials = service_account.Credentials.from_service_account_file(
            client_secrets_path,
        )

        handler = TableHandler(
            database.replace('bigquery://', ''),
            credentials,
            table_name,
            schema
        )

        handler.create_table(logger)
