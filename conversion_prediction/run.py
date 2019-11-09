# environment variables
from dotenv import load_dotenv
load_dotenv()

# logging
import logging.config
from utils.config import LOGGING
logger = logging.getLogger(__name__)
logging.config.dictConfig(LOGGING)
logger.setLevel(logging.INFO)

import argparse
import json
import os
import re
import pandas as pd
import numpy as np
import sqlalchemy
import joblib

from datetime import datetime, timedelta
from typing import List, Dict
from dateutil.parser import parse
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, LabelEncoder

from utils.db_utils import create_predictions_table, create_predictions_job_log
from utils.config import LABELS, FeatureColumns, CURRENT_MODEL_VERSION
from utils.enums import SplitType, NormalizedFeatureHandling
from utils.enums import ArtifactRetentionMode, ArtifactRetentionCollection, ModelArtifacts
from utils.db_utils import create_connection
from utils.queries import queries
from utils.queries import get_feature_frame_via_sqlalchemy, get_payment_history_features, get_global_context
from utils.data_transformations import unique_list, row_wise_normalization


class ConversionPredictionModel(object):
    def __init__(
            self,
            min_date: datetime = datetime.utcnow() - timedelta(days=31),
            max_date: datetime = datetime.utcnow() - timedelta(days=1),
            moving_window_length: int = 7,
            normalization_handling: NormalizedFeatureHandling = NormalizedFeatureHandling.REPLACE_WITH,
            outcome_labels: List[str] = LABELS,
            overwrite_files: bool = True,
            training_split_parameters=None,
            model_arguments = None,
            undersampling_factor=500,
            # This applies to all model artifacts that are not part of the flow output
            artifact_retention_mode: ArtifactRetentionMode = ArtifactRetentionMode.DUMP,
            # By default everything gets stored (since we expect most runs to still be in experimental model
            artifacts_to_retain: ArtifactRetentionCollection = ArtifactRetentionCollection.MODEL_TUNING
    ):
        self.min_date = min_date
        self.max_date = max_date
        self.moving_window = moving_window_length
        self.overwrite_files = overwrite_files
        self.user_profiles = None
        self.normalization_handling = normalization_handling
        self.feature_columns = FeatureColumns()
        self.category_list_dict = {}
        self.le = LabelEncoder()
        self.outcome_labels = outcome_labels
        self.le.fit(outcome_labels)
        self.X_train = pd.DataFrame()
        self.X_test = pd.DataFrame()
        self.Y_train = pd.Series()
        self.Y_test = pd.Series()
        self.scaler = MinMaxScaler()
        self.model_date = None
        self.training_split_parameters = training_split_parameters
        self.model_arguments = model_arguments
        self.undersampling_factor = undersampling_factor
        self.X_train_undersampled = pd.DataFrame()
        self.Y_train_undersampled = pd.Series()
        self.model = None
        self.outcome_frame = None
        self.scoring_date = datetime.utcnow()
        self.model = None
        self.prediction_data = pd.DataFrame()
        self.predictions = pd.DataFrame()
        self.artifact_retention_mode = artifact_retention_mode
        self.artifacts_to_retain = artifacts_to_retain
        self.path_to_model_files = os.getenv('PATH_TO_MODEL_FILES')
        self.path_to_csvs = os.getenv('PATH_TO_CSV_FILES')

    def artifact_handler(self, artifact: ModelArtifacts):
        '''
        :param artifact:
        :return:
        Requires:
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
                getattr(self, artifact.value, pd.DataFrame())\
                    .to_csv(f'{self.path_to_model_files}artifact_{artifact.value}_{self.min_date}_{self.max_date}.csv')
                logger.info(f'  * {artifact.value} artifact dumped to {self.path_to_model_files}')
        delattr(self, artifact.value)
        logger.info(f'  * {artifact.value} artifact dropped')

    def get_user_profiles_by_date(
            self
    ):
        '''
        Requires:
            - min_date
            - max_date
            - moving_window
        Retrieves rolling window user profiles from the db
        using row-wise normalized features
        '''
        self.min_date = self.min_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.max_date = self.max_date.replace(hour=0, minute=0, second=0, microsecond=0)

        self.user_profiles = get_feature_frame_via_sqlalchemy(
            self.min_date,
            self.max_date,
            self.moving_window
        )

        logger.info(f'  * Retrieved initial user profiles frame from DB')

        try:
            self.get_contextual_features_from_mysql()
            self.feature_columns.add_global_context_features()
            logger.info('Successfully added global context features from csvs')
        except Exception as e:
             logger.info(
                f'''Failed adding global context features from mysql with exception:
               {e};
               proceeding with remaining features''')

        try:
            self.get_payment_window_features_from_csvs()
            self.feature_columns.add_commerce_csv_features()
            logger.info('Successfully added commerce flow features from csvs')
        except Exception as e:
            logger.info(
                f'''Failed adding commerce flow features from csvs with exception: 
                {e};
                proceeding with remaining features''')
        
        self.user_profiles['date'] = pd.to_datetime(self.user_profiles['date']).dt.date
        #try:
        self.get_user_history_features_from_mysql()
        self.feature_columns.add_payment_history_features()
        #except Exception as e:
        #    logger.info(
        #        f'''Failed adding payment history features from mysql with exception: 
        #        {e};
        #        proceeding with remaining features''')

        self.user_profiles[self.feature_columns.numeric_columns_with_window_variants].fillna(0, inplace=True)
        self.user_profiles['user_ids'] = self.user_profiles['user_ids'].apply(unique_list)

        if self.user_profiles['date'].min() > self.min_date.date():
            raise ValueError(f'''While the specified min_date is {self.min_date.date()},
            only data going back to {self.user_profiles['date'].min()} were retrieved''')

        if self.user_profiles['date'].max() < self.max_date.date():
            raise ValueError(f'''While the specified max_date is {self.max_date.date()},
            only data up until {self.user_profiles['date'].max()} were retrieved''')
        logger.info('  * Initial data validation success')

    def get_payment_window_features_from_csvs(self):
        '''
        Requires:
            - min_date
            - max_date
        Loads in csvs (location needs to be in .env), counts the number of events per browser_id and joins these counts
        to the main feature frame
        '''
        commerce = pd.DataFrame()
        dates = [date.date() for date in pd.date_range(self.min_date - timedelta(days=7), self.max_date)]
        dates = [re.sub('-', '', str(date)) for date in dates]
        for date in dates:
            commerce_daily = pd.read_csv(f'{self.path_to_csvs}/commerce_{date}.csv.gz')
            commerce = commerce.append(commerce_daily)

        commerce = commerce[commerce['browser_id'].isin(self.user_profiles['browser_id'].unique())]
        commerce['date'] = pd.to_datetime(commerce['time']).dt.date

        commerce['dummy_column'] = 1.0
        commerce_features = commerce[['date', 'step', 'dummy_column', 'browser_id']].groupby(
            ['date', 'step', 'browser_id']).sum().reset_index()
        commerce_features.head()

        commerce_pivoted = pd.DataFrame()

        # We are using all the commerce steps based on the assumption that all of our postgres data is filtered for only
        # days before conversion, we will do a left join on the postgres data assuming this removes look-ahead
        for step in commerce['step'].unique():
            if commerce_pivoted.empty:
                commerce_pivoted = commerce_features[commerce_features['step'] == step].rename(
                    columns={'dummy_column': step})
            else:
                commerce_pivoted = commerce_pivoted.merge(
                    right=commerce_features[commerce_features['step'] == step].rename(columns={'dummy_column': step}),
                    on=['browser_id', 'date'],
                    how='outer'
                )
            commerce_pivoted.drop('step', axis=1, inplace=True)

        commerce_pivoted.index = commerce_pivoted['date']
        commerce_pivoted.drop('date', axis=1, inplace=True)
        commerce_pivoted.index = pd.to_datetime(commerce_pivoted.index)

        commerce_pivoted.fillna(0.0, inplace=True)
        dates = self.user_profiles['date'].unique()
        dates = [date.date() for date in pd.date_range(dates.min() - timedelta(days=7), dates.max())]
        rolling_commerce_pivotted = (commerce_pivoted.groupby(['browser_id'])
                                     .apply(lambda x: x.reindex(dates)  # fill in the missing dates for each group)
                                            .fillna(0.0)  # fill each missing group with 0
                                            .rolling(7, min_periods=1)
                                            .sum()))  # do a rolling sum

        rolling_commerce_pivotted.reset_index(inplace=True)

        rolling_commerce_pivotted = rolling_commerce_pivotted[
            (rolling_commerce_pivotted['date'] >= self.min_date.date()) &
            (rolling_commerce_pivotted['date'] <= self.max_date.date())
            ]
        rolling_commerce_pivotted['date'] = pd.to_datetime(rolling_commerce_pivotted['date']).dt.date

        self.user_profiles = self.user_profiles.merge(
            right=rolling_commerce_pivotted,
            on=['browser_id', 'date'],
            how='left',
            copy=False
        )
        # TODO: Come up with a better handling for these features for past positives (currently no handling)

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

        # We'll only be matching the first user_id in the list for speed reasons
        self.user_profiles['first_user_id'] = self.user_profiles['user_ids'].apply(
            lambda x: x[0]
        ).str.extract('([0-9]+)')
        # TODO: Come up with a better handling for these features for past positives (currently no handling)
        # for index, row in payment_history_features.iterrows():
        #     if index % int(len(payment_history_features) / 10) == 0:
        #         print(f'{index / len(payment_history_features)} % done')
        #     possible_matches = self.user_profiles[
        #         self.user_profiles['user_ids'] != pd.Series([[''] for i in range(len(self.user_profiles))])
        #     ]
        #     matching_index = possible_matches[
        #         # user_id contained in the list of user_ids for a browser
        #         (possible_matches['user_ids'].astype(str).fillna('').str.contains(str(row['user_id']))) &
        #         # user is not a past positive, since the payment history would be a look ahead
        #         (possible_matches['date'] >= row['last_subscription_end'].date())
        #         ].index
        #     self.user_profiles.loc[matching_index, 'days_since_last_subscription'] = row['days_since_last_subscription']
        #     self.user_profiles.loc[matching_index, 'clv'] = row['clv']
        
        payment_history_features['user_id'] = payment_history_features['user_id'].astype(int).astype(str)
        self.user_profiles = self.user_profiles.merge(
            right=payment_history_features,
            left_on='first_user_id',
            right_on='user_id',
            how='left'
        )

        self.user_profiles['clv'] = self.user_profiles['clv'].astype(float)
        self.user_profiles['clv'].fillna(0.0, inplace=True)
        # In case the user had additional subscriptions that have an end date after the date of the user profile,
        # we treat it as a missing value.
        # TODO: Add better treatment for look-ahead removal
        self.user_profiles['last_subscription_end'] = pd.to_datetime(self.user_profiles['last_subscription_end']).dt.date 
        self.user_profiles.loc[
            self.user_profiles['last_subscription_end'] >= self.user_profiles['date'],
            'clv'
        ] = 0.0
        # The 1000 days is an arbitrary choice here
        self.user_profiles['days_since_last_subscription'].fillna(1000.0, inplace=True)
        self.user_profiles.loc[
            self.user_profiles['last_subscription_end'] >= self.user_profiles['date'],
            'days_since_last_subscription'
        ] = 1000.0

        self.user_profiles.drop(['last_subscription_end', 'first_user_id'], axis=1, inplace=True)

        return payment_history_features

    def get_contextual_features_from_mysql(self):
        '''
        Requires:
            - user_profiles
        Retrieves & joins daily rolling article pageviews, sum paid, payment count and average price
        '''
        # We extract these, since we also want global context for the past positives data
        context = get_global_context(
            self.user_profiles['date'].min(),
            self.user_profiles['date'].max()
        )
        
        context.index = context['date']
        context.drop('date', axis=1, inplace=True)
        context.index = pd.to_datetime(context.index)
        dates = [date.date() for date in pd.date_range(context.index.min(), context.index.max())]
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

    def introduce_row_wise_normalized_features(self):
        self.user_profiles.drop(['date_str', 'date_y'], axis=1, inplace=True)
        self.user_profiles.rename(columns={'date_x': 'date'}, inplace=True)

    def introduce_row_wise_normalized_features(self):
        '''
        Requires:
            - user_profiles
            - normalization_handling
            - feature_columns
        Adds normalized profile based features (normalized always within group, either replacing the original profile based
        features in group, or adding them onto the original dataframe
        '''
        merge_columns = ['date', 'browser_id']
        self.add_missing_json_columns()
        for column_set_list in self.feature_columns.profile_numeric_columns_from_json_fields.values():
            for column_set in column_set_list:
                normalized_data = pd.DataFrame(row_wise_normalization(np.array(self.user_profiles[column_set])))
                normalized_data.fillna(0.0, inplace=True)
                normalized_data.columns = column_set

                if self.normalization_handling is NormalizedFeatureHandling.REPLACE_WITH:
                    self.user_profiles.drop(column_set, axis=1, inplace=True)
                elif self.normalization_handling is NormalizedFeatureHandling.ADD:
                    self.feature_columns.add_normalized_profile_features_version()
                else:
                    raise ValueError('Unknown normalization handling parameter')

                self.user_profiles = pd.concat(
                    [
                        self.user_profiles,
                        normalized_data
                    ],
                    axis=1
                )
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
            column for column_list_set in
            self.feature_columns.profile_numeric_columns_from_json_fields.values()
            for column_list in column_list_set
            for column in column_list
        ]

        for missing_json_column in set(potential_columns) - set(self.user_profiles.columns):
            self.user_profiles[missing_json_column] = 0.0

    def create_feature_frame(self):
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
        self.get_user_profiles_by_date()
        logger.info(f'  * Processing user profiles')
        test_outcome = self.user_profiles['outcome'].fillna(
            self.user_profiles.groupby('browser_id')['outcome'].fillna(method='bfill')
        )
        test_outcome = test_outcome.fillna(
            self.user_profiles.groupby('browser_id')['outcome'].fillna(method='ffill')
        )
        self.user_profiles['outcome'] = test_outcome
        self.encode_uncommon_categories()
        self.transform_bool_columns_to_int()
        logger.info('  * Filtering user profiles')
        self.user_profiles = self.user_profiles[self.user_profiles['days_active_count'] >= 1].reset_index(drop=True)

        if self.normalization_handling is not NormalizedFeatureHandling.IGNORE:
            logger.info(f'  * Normalizing user profiles')
            self.introduce_row_wise_normalized_features()

    def transform_bool_columns_to_int(self):
        '''
        Requires:
            - user_profiles
        Transform True / False columns into 0/1 columns
        '''
        for column in [column for column in self.feature_columns.bool_columns]:
            self.user_profiles[column] = self.user_profiles[column].astype(int)

    def encode_uncommon_categories(self):
        '''
        Requires:
            - user_profiles
        Categories for columns such as browser with less than 5 % frequency get lumped into 'Other'
        '''
        self.user_profiles.loc[self.user_profiles['device'] == 0, 'device'] = 'Desktop'
        for column in self.feature_columns.categorical_columns:
            column_values_to_recode = list(
                self.user_profiles[column
                ].value_counts(normalize=True)[self.user_profiles[column].value_counts(normalize=True) < 0.05].index)
            self.user_profiles.loc[self.user_profiles[column].isin(column_values_to_recode), column] = 'Other'

    def generate_category_lists_dict(self) -> Dict:
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
        In train and predictiom set, we mighr encounter categories that weren't present in the test set, in order
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
        Generate 0/1 columns from categorical columns handling the logic for Others and Unknown categories. Always drops
        the Unknown columns (since we only need k-1 columns for k categories)
        :param data: A column to encode
        :return:
        '''
        data = self.encode_unknown_categories(data)
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
        self.model_date = self.user_profiles['date'].max() + timedelta(days=1)
        split = SplitType(self.training_split_parameters['split'])
        split_ratio = self.training_split_parameters['split_ratio']
        if split is SplitType.RANDOM:
            train, test = train_test_split(self.user_profiles, test_size=(1 - split_ratio), random_state=42)
            train_indices = train.index
            test_indices = test.index
            del (train, test)
        else:
            dates = pd.to_datetime(
                pd.Series([date.date() for date in pd.date_range(
                    self.user_profiles['date'].min(),
                    self.user_profiles['date'].max())])
            )

            train_date = dates[0:int(round(len(dates) * split_ratio, 0))].max()
            train_indices = self.user_profiles[self.user_profiles['date'] <= train_date.date()].index
            test_indices = self.user_profiles[self.user_profiles['date'] > train_date.date()].index

        self.X_train = self.user_profiles.iloc[train_indices].drop(columns=['outcome', 'user_ids'])
        self.X_test = self.user_profiles.iloc[test_indices].drop(columns=['outcome', 'user_ids'])
        category_lists_dict = self.generate_category_lists_dict()

        with open(
                f'{self.path_to_model_files}category_lists_{self.model_date}.json', 'w') as outfile:
            json.dump(category_lists_dict, outfile)

        self.X_train = self.replace_dummy_columns_with_dummies(self.X_train)
        self.X_test = self.replace_dummy_columns_with_dummies(self.X_test)

        self.Y_train = self.user_profiles.loc[train_indices, 'outcome'].sort_index()
        self.Y_test = self.user_profiles.loc[test_indices, 'outcome'].sort_index()

        logger.info('  * Dummy variables generation success')

        X_train_numeric = self.user_profiles.loc[
            train_indices,
            self.feature_columns.numeric_columns_with_window_variants
        ].fillna(0)
        X_test_numeric = self.user_profiles.loc[
            test_indices,
            self.feature_columns.numeric_columns_with_window_variants
        ].fillna(0)

        X_train_numeric = pd.DataFrame(self.scaler.fit_transform(X_train_numeric), index=train_indices,
                                       columns=self.feature_columns.numeric_columns_with_window_variants).sort_index()

        X_test_numeric = pd.DataFrame(self.scaler.transform(X_test_numeric), index=test_indices,
                                      columns=self.feature_columns.numeric_columns_with_window_variants).sort_index()

        logger.info('  * Numeric variables handling success')

        self.X_train = pd.concat([X_train_numeric.sort_index(), self.X_train[
            [column for column in self.X_train.columns
             if column not in self.feature_columns.numeric_columns_with_window_variants +
             self.feature_columns.config_columns +
             self.feature_columns.bool_columns]
        ].sort_index()], axis=1)
        self.X_test = pd.concat([X_test_numeric.sort_index(), self.X_test[
            [column for column in self.X_train.columns
             if column not in self.feature_columns.numeric_columns_with_window_variants +
             self.feature_columns.config_columns +
             self.feature_columns.bool_columns]
        ].sort_index()], axis=1)

        joblib.dump(
            self.scaler,
            f'{self.path_to_model_files}scaler_{self.model_date}.pkl'
        )

        if ModelArtifacts.USER_PROFILES not in self.artifacts_to_retain.value:
            ConversionPredictionModel.artifact_handler(self, ModelArtifacts.USER_PROFILES)

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

        if f'scaler_{self.model_date}.pkl' in os.listdir(
                None if self.path_to_model_files == '' else self.path_to_model_files):
            os.remove(f'{self.path_to_model_files}{filename}_{self.model_date}.{suffix}')

    def undersample_majority_class(self):
        '''
        Requires:
            - user_profiles
            - undersampling_factor
        A function that undersamples majority class to help the algorithm identify the classes with
        lower occurence. Operates based on an undersampling factor.
        TODO: Currently hardcodes the conversion prediction class encoding, could be done based on actual frequencies
        TODO: or at least not use check label encodings (0 = no_conversion, 1 = conversion, 2 = shared_account_login)
        :return:
        '''
        joined_df = pd.concat([self.X_train, self.Y_train], axis=1)
        positive_outcomes = joined_df[joined_df['outcome'].isin([0, 2])]
        negative_outcome = joined_df[joined_df['outcome'] == 1].sample(
            frac=1 / self.undersampling_factor,
            random_state=42
        )
        sampled_df = pd.concat([positive_outcomes, negative_outcome], axis=0)
        self.X_train_undersampled = sampled_df.drop('outcome', axis=1)
        self.Y_train_undersampled = sampled_df['outcome']

        logger.info('  * Train undersampling success')

    def train_model(
            self):
        '''
        Requires:
            - user_profiles
            - undersampling_factor
            - model_arguments
            - artifacts_to_retain
            - artifact_retention_mode
        Trains a new model given a full dataset
        '''
        label_range = list(range(0, len(self.outcome_labels)))
        if 0 not in self.user_profiles['outcome'].unique():
            self.user_profiles['outcome'] = self.le.transform(self.user_profiles['outcome'])

        self.create_train_test_transformations()
        self.undersample_majority_class()
        self.X_train.fillna(0.0, inplace=True)
        self.X_test.fillna(0.0, inplace=True)
        self.X_train_undersampled.fillna(0.0, inplace=True)
        self.Y_train_undersampled.fillna(0.0, inplace=True)

        logger.info('  * Commencing model training')

        classifier_instance = RandomForestClassifier(**self.model_arguments)
        self.model = classifier_instance.fit(self.X_train_undersampled, self.Y_train_undersampled)

        logger.info('  * Model training complete, generating outcome frame')

        self.outcome_frame = pd.concat([
            pd.DataFrame(list(precision_recall_fscore_support(
                self.Y_train,
                self.model.predict(self.X_train),
                labels=label_range))
            ),
            pd.DataFrame(list(
                precision_recall_fscore_support(
                    self.Y_test,
                    self.model.predict(self.X_test),
                    labels=label_range))
            )
        ],
            axis=1)

        for artifact in [
            ModelArtifacts.TRAIN_DATA_FEATURES, ModelArtifacts.TRAIN_DATA_OUTCOME,
            ModelArtifacts.UNDERSAMPLED_TRAIN_DATA_FEATURES, ModelArtifacts.UNDERSAMPLED_TRAIN_DATA_OUTCOME,
            ModelArtifacts.TEST_DATA_FEATURES, ModelArtifacts.TEST_DATA_OUTCOME
        ]:
            if artifact not in self.artifacts_to_retain.value:
                ConversionPredictionModel.artifact_handler(self, artifact)

        self.outcome_frame.columns = [str(label) + '_train'
                                 for label in self.outcome_frame.columns[0:len(label_range)]] + \
                                [str(label) + '_test'
                                 for label in
                                 self.outcome_frame.columns[
                                 len(label_range):(2*len(label_range))]]
        for i in label_range:
            self.outcome_frame.columns = [re.sub(str(i), self.le.inverse_transform([i])[0], column)
                                     for column in self.outcome_frame.columns]
        self.outcome_frame.index = ['precision', 'recall', 'f-score', 'support']

        logger.info('  * Outcome frame generated')

    def model_training_pipeline(self):
        '''
        Requires:
            - undersampling_factor
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
        if self.user_profiles is None:
            self.create_feature_frame()

        if self.overwrite_files:
            for model_file in ['category_lists', 'scaler', 'model']:
                self.delete_existing_model_file_for_same_date(model_file)

        self.train_model()

        logger.info(f'Training ready, dumping to file')

        joblib.dump(
            self.model,
            f'{self.path_to_model_files}model_{self.model_date}.pkl'
        )

        logger.info(f'Saved to {self.path_to_model_files}model_{self.model_date}.pkl')

    def load_model_related_constructs(self):
        '''
        Requires:
            - path_to_model_files
            - scoring_date
        Serves for the prediction pipeline in order to load model & additional transformation config objects
        '''

        model_related_file_list = os.listdir(self.path_to_model_files)
        last_model_related_files = {}
        for model_related_file in ['category_lists', 'scaler', 'model']:
            last_file_date = {parse(re.sub(f'{model_related_file}_|.json|.pkl', '', filename)):
                              abs(parse(re.sub(f'{model_related_file}_|.json|.pkl', '', filename)).date()
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
                ''')
        if not self.path_to_model_files:
            self.path_to_model_files = ''
        with open(self.path_to_model_files +
                  'category_lists_' + str(last_model_related_files['category_lists']) + '.json', 'r') as outfile:
            self.category_list_dict = json.load(outfile)

        self.scaler = joblib.load(self.path_to_model_files + 'scaler_' + str(last_model_related_files['scaler']) + '.pkl')
        self.model = joblib.load(self.path_to_model_files + 'model_' + str(last_model_related_files['model']) + '.pkl')

        logger.info('  * Model constructs loaded')

    def batch_predict(self):
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
        self.create_feature_frame()
        self.load_model_related_constructs()
        self.replace_dummy_columns_with_dummies()

        feature_frame_numeric = pd.DataFrame(
            self.scaler.transform(
                self.user_profiles[
                self.feature_columns.numeric_columns_with_window_variants]
            ),
            index=self.user_profiles.index,
            columns=self.feature_columns.numeric_columns_with_window_variants).sort_index()

        self.prediction_data = pd.concat([
            feature_frame_numeric, self.user_profiles[
                [column for column in self.user_profiles.columns
                 if column not in self.feature_columns.numeric_columns_with_window_variants +
                 self.feature_columns.config_columns +
                 self.feature_columns.bool_columns]].sort_index()], axis=1)

        self.prediction_data = self.prediction_data.drop(['outcome', 'user_ids'], axis=1)
        logger.info('  * Prediction data ready')

        predictions = pd.DataFrame(self.model.predict_proba(self.prediction_data))
        logger.info('  * Prediction generation success, handling artifacts')

        label_range = range(len(LABELS))

        for i in label_range:
            predictions.columns = [re.sub(str(i), self.le.inverse_transform(i) + '_probability', str(column))
                                   for column in predictions.columns]

        self.predictions = pd.concat([self.user_profiles[['date', 'browser_id', 'user_ids']],  predictions], axis=1)
        self.predictions['predicted_outcome'] = self.le.inverse_transform(self.model.predict(self.prediction_data))

        for artifact in [ModelArtifacts.MODEL, ModelArtifacts.PREDICTION_DATA, ModelArtifacts.USER_PROFILES]:
            ConversionPredictionModel.artifact_handler(self, artifact)

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
        Generates outcome prediction for conversion and uploads them to the DB
        '''
        logger.info(f'Executing prediction generation')
        self.batch_predict()

        self.predictions['model_version'] = CURRENT_MODEL_VERSION
        self.predictions['created_at'] = datetime.utcnow()
        self.predictions['updated_at'] = datetime.utcnow()

        logger.info(f'Storing predicted data')
        _, postgres = create_connection(os.getenv('POSTGRES_CONNECTION_STRING'))
        create_predictions_table(postgres)
        create_predictions_job_log(postgres)
        postgres.execute(
            sqlalchemy.sql.text(queries['upsert_predictions']), self.predictions.to_dict('records')
        )

        prediction_job_log = self.predictions[
            ['date', 'model_version', 'created_at', 'updated_at']].head(1).to_dict('records')[0]
        prediction_job_log['rows_predicted'] = len(self.predictions)
        postgres.execute(
            sqlalchemy.sql.text(queries['upsert_prediction_job_log']), [prediction_job_log]
        )

        logger.info('Predictions are now ready')


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
    logger.info(f'CONVERSION PREDICTION')
    parser = argparse.ArgumentParser()
    parser.add_argument('--action',
                        help='Should either be "train" for model training or "predict for prediction"',
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
                        default=7,
                        required=False)
    parser.add_argument('--training-split-parameters',
                        help='Speficies split_type (random vs time_based) and split_ratio for train/test split',
                        type=json.loads,
                        default={'split': 'time_based', 'split_ratio': 6 / 10},
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
        conversion_prediction = ConversionPredictionModel(
            min_date=args['min_date'],
            max_date=args['max_date'],
            moving_window_length=args['moving_window_length'],
            overwrite_files=args['overwrite_files'],
            training_split_parameters=args['training_split_parameters'],
            model_arguments={'n_estimators': 250},
            undersampling_factor=500,
            artifact_retention_mode=ArtifactRetentionMode.DROP,
            artifacts_to_retain=ArtifactRetentionCollection.MODEL_RETRAINING
            )
        conversion_prediction.model_training_pipeline()

        metrics = ['precision', 'recall', 'f1_score', 'suport']
        print({metrics[i]: conversion_prediction.outcome_frame.to_dict('records')[i] for i in range(0, len(metrics))})
    elif args['action'] == 'predict':
        conversion_prediction = ConversionPredictionModel(
            min_date=args['min_date'],
            max_date=args['max_date'],
            moving_window_length=args['moving_window_length'],
            artifact_retention_mode=ArtifactRetentionMode.DROP,
            artifacts_to_retain=ArtifactRetentionCollection.PREDICTION
        )
