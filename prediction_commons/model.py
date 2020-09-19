import sys

from imblearn.under_sampling import RandomUnderSampler

sys.path.append("../")

# environment variables
from dotenv import load_dotenv

load_dotenv('.env')

# logging
import logging.config
from utils.config import LOGGING

logger = logging.getLogger(__name__)
logging.config.dictConfig(LOGGING)
logger.setLevel(logging.INFO)

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

from .enums import SplitType, NormalizedFeatureHandling, DataRetrievalMode, ArtifactRetentionMode, \
    ArtifactRetentionCollection, ModelArtifacts
from .data_transformations import row_wise_normalization


class PredictionModel(object):
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
            dry_run: bool = False,
            path_to_model_files: str = None,
            positive_event_lookahead: int = 33,
            model_record_id: str = 'id',
    ):
        def create_util_columns():
            util_columns = ['outcome', 'feature_aggregation_functions', 'date', 'outcome_date', self.id_column]

            return util_columns

        self.feature_columns = None,
        self.current_model_version = None,
        self.profile_columns = None,
        self.id_column = model_record_id
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
        self.X_train_undersampled = pd.DataFrame()
        self.Y_train = pd.Series()
        self.Y_test = pd.Series()
        self.Y_train_undersampled = pd.DataFrame()
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
        self.positive_event_lookahead = positive_event_lookahead

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
                    f'{self.path_to_model_files}model_{self.model_date}.pkl'
                )
            else:
                getattr(self, artifact.value, pd.DataFrame()) \
                    .to_csv(f'{self.path_to_model_files}artifact_{artifact.value}_{self.min_date}_{self.max_date}.csv')
                logger.info(f'  * {artifact.value} artifact dumped to {self.path_to_model_files}')
        delattr(self, artifact.value)
        logger.info(f'  * {artifact.value} artifact dropped')

    def get_full_user_profiles_by_date(self):
        pass

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
        for column in [column for column in self.user_profiles.columns if 'features' in column]:
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
        for column in [column for column in self.feature_columns.bool_columns]:
            self.user_profiles[column] = self.user_profiles[column].apply(
                lambda value: True if value == 't' else False).astype(int)

    def generate_category_list_dict(self):
        '''
        Requires:
            - user_profiles
        Create lists of individual category variables for consistent encoding between train set, test set and
        prediciton set
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
            data = pd.concat(
                [
                    data,
                    self.generate_dummy_columns_from_categorical_column(
                        data[column])
                ],
                axis=1)
        data.drop(columns=self.feature_columns.categorical_columns, axis=1, inplace=True)

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
            indices = np.random.RandomState(seed=42).permutation(self.user_profiles.index)
        else:
            indices = self.user_profiles.sort_values('date').index

        self.user_profiles = self.user_profiles.iloc[indices].reset_index(drop=True)
        train_cutoff = int(round(len(indices) * split_ratio, 0))
        train_indices = indices[:train_cutoff]
        test_indices = indices[train_cutoff:]

        self.X_train = self.user_profiles.loc[train_indices].drop(columns=self.util_columns)
        self.X_test = self.user_profiles.loc[test_indices].drop(columns=self.util_columns)
        self.generate_category_list_dict()

        with open(
                f'{self.path_to_model_files}category_lists_{self.model_date}.json', 'w') as outfile:
            json.dump(self.category_list_dict, outfile)

        self.X_train = self.replace_dummy_columns_with_dummies(self.X_train)

        self.Y_train = self.user_profiles.loc[train_indices, 'outcome'].sort_index()
        self.Y_test = self.user_profiles.loc[test_indices, 'outcome'].sort_index()

        logger.info('  * Dummy variables generation success')

        self.feature_columns.numeric_columns_all.sort()
        X_train_numeric = self.user_profiles.loc[
            train_indices,
            self.feature_columns.numeric_columns_all
        ].fillna(0.0)

        self.scaler = MinMaxScaler(feature_range=(0, 1)).fit(X_train_numeric)

        X_train_numeric = pd.DataFrame(self.scaler.transform(X_train_numeric), index=train_indices,
                                       columns=self.feature_columns.numeric_columns_all).sort_index()

        logger.info('  * Numeric variables handling success')

        self.X_train = pd.concat([X_train_numeric.sort_index(), self.X_train[
            [column for column in self.X_train.columns
             if column not in self.feature_columns.numeric_columns_all +
             self.feature_columns.config_columns]
        ].sort_index()], axis=1)

        self.X_train = self.sort_columns_alphabetically(self.X_train)

        joblib.dump(
            self.scaler,
            f'{self.path_to_model_files}scaler_{self.model_date}.pkl'
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
            if f'scaler_{self.model_date}.pkl' in os.listdir(
                    None if self.path_to_model_files == '' else self.path_to_model_files):
                os.remove(f'{self.path_to_model_files}{filename}_{self.model_date}.{suffix}')

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
        if 0 not in self.user_profiles['outcome'].unique():
            self.user_profiles['outcome'] = self.le.transform(self.user_profiles['outcome'])

        self.create_train_test_transformations()
        self.train_model(model_function, model_arguments)

    def train_model(
            self,
            model_function,
            model_arguments
    ):
        self.undersample_majority_class()
        self.X_train.fillna(0.0, inplace=True)
        self.X_train_undersampled.fillna(0.0, inplace=True)
        self.X_test.fillna(0.0, inplace=True)

        logger.info('  * Commencing model training')

        classifier_instance = model_function(**model_arguments)
        self.model = classifier_instance.fit(self.X_train_undersampled, self.Y_train_undersampled)

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
        if self.user_profiles is None:
            self.get_full_user_profiles_by_date()

        # Some users have no outcome due to having renewed via a non-payment option, thus we need to drop them
        # for the purpose of training a model
        self.user_profiles = self.user_profiles[~self.user_profiles['outcome'].isna()]

        if self.overwrite_files:
            for model_file in ['category_lists', 'scaler', 'model']:
                self.delete_existing_model_file_for_same_date(model_file)

        if sampling_function:
            try:
                self.sampling_function = sampling_function(n_jobs=-1)
            except:
                self.sampling_function = sampling_function()

        self.preprocess_and_train(
            model_function,
            model_arguments
        )

        logger.info(f'Training ready, dumping to file')

        joblib.dump(
            self.model,
            f'{self.path_to_model_files}model_{self.model_date}.pkl'
        )

        logger.info(f'Saved to {self.path_to_model_files}model_{self.model_date}.pkl')

        self.remove_model_training_artefacts()
        # TODO: This would eventually be replaced with storing variable importances to DB
        self.variable_importances.to_csv(
            f'{self.path_to_model_files}variable_importances_{self.model_date}.csv',
            index=True,
            header=False
        )

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

        model_related_file_list = os.listdir(self.path_to_model_files)
        last_model_related_files = {}
        for model_related_file in ['category_lists', 'scaler', 'model', 'variable_importances']:
            last_file_date = {parse(re.sub(f'{model_related_file}_|.json|.pkl|.csv|None', '', filename)):
                                  abs(parse(re.sub(f'{model_related_file}_|.json|.pkl|.csv|None', '', filename)).date()
                                      - self.scoring_date.date())
                              for filename in model_related_file_list
                              if re.search(f'{model_related_file}_', filename)}
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
                  'category_lists_' + str(last_model_related_files['category_lists']) + '.json', 'r') as outfile:
            self.category_list_dict = json.load(outfile)

        self.scaler = joblib.load(f"{self.path_to_model_files}scaler_{str(last_model_related_files['scaler'])}.pkl")
        self.model = joblib.load(f"{self.path_to_model_files}model_{str(last_model_related_files['model'])}.pkl")
        # TODO: This would eventually be replaced with loading variable importances from DB
        self.variable_importances = pd.read_csv(
            f"{self.path_to_model_files}variable_importances_{str(last_model_related_files['variable_importances'])}.csv",
            squeeze=True,
            index_col=0,
            header=None
        )

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

        # Add missing columns that were in the train set
        for column in self.variable_importances.index:
            if column not in data.columns:
                if column in self.feature_columns.numeric_columns_all:
                    data[column] = 0.0
                    self.feature_columns.numeric_columns_all.append(column)
                # This is a hacky solution, but device brand (and potentially other profile columns
                # have the most fluctuations in terms of values that translate to new columns:
                for profile_column_flag in self.profile_columns + ['_device_brand_']:
                    if profile_column_flag in column and column:
                        data[column] = 0.0
                        self.feature_columns.numeric_columns_all.append(column)

        # Remove new columns (such as new devices appearing in shorter time ranges)
        for column in [
            column for column in data.columns
            if column not in self.variable_importances.index
               and column in self.feature_columns.numeric_columns_all
        ]:
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

        self.prediction_data = pd.concat([
            feature_frame_numeric, data[
                [column for column in data.columns
                 if column not in self.feature_columns.numeric_columns_all +
                 self.feature_columns.config_columns
                 ]].sort_index()], axis=1)

        self.prediction_data = self.replace_dummy_columns_with_dummies(self.prediction_data)

        self.align_prediction_frame_with_train_columns()
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

    def align_prediction_frame_with_train_columns(self):
        # Sometimes the columns used to train a model don't align with columns im prediction set
        # 1. Drop columns that are in new data, but weren't used in training
        self.prediction_data.drop(
            [column for column in self.prediction_data.columns if column not in self.variable_importances.index],
            axis=1,
            inplace=True
        )

        # 2. Add 0 columns that were in train, but aren't in new data
        for column in [column for column in self.variable_importances.index
                       if column not in self.prediction_data.columns]:
            self.prediction_data[column] = 0.0

        # 3. Make sure the columns have the same order as original data, since sklearn ignores column names
        self.prediction_data = self.prediction_data[list(self.variable_importances.index)]

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
        self.get_full_user_profiles_by_date()
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
        self.handle_profiles_table()
        logger.info(f'Starting with preaggregation for date range {self.min_date.date()} - {self.max_date.date()}')
        self.retrieve_and_insert()

    def undersample_majority_class(self):
        sampler = self.sampling_function
        self.X_train_undersampled, self.Y_train_undersampled = sampler.fit_resample(
            self.X_train,
            self.Y_train
        )
