import argparse
import json
import os
import re
import pandas as pd
import sqlalchemy

from datetime import datetime, timedelta
from os.path import isfile
from typing import List, Dict
from dateutil.parser import parse
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, LabelEncoder

from .utils.db_utils import create_predictions_table, create_predictions_job_log
from .utils.config import NUMERIC_COLUMNS, BOOL_COLUMNS, CATEGORICAL_COLUMNS, CONFIG_COLUMNS, \
    split_type, LABELS, CURRENT_MODEL_VERSION
from .utils.db_utils import create_connection, retrieve_data_for_query_key
from .utils.queries import queries
from .utils.queries import get_feature_frame_via_sqlalchemy
from .utils.data_transformations import unique_list

load_dotenv()


def get_user_profiles_by_date(
        min_date: datetime=datetime.utcnow() - timedelta(days=31),
        max_date: datetime=datetime.utcnow() - timedelta(days=1),
        moving_window_length: int=7
) -> pd.DataFrame:
    '''
    Retrives rolling window user profiles from the db
    :param min_date:
    :param max_date:
    :param moving_window_length:
    :return:
    '''
    min_date = min_date.replace(hour=0, minute=0, second=0, microsecond=0)
    max_date = max_date.replace(hour=0, minute=0, second=0, microsecond=0)

    user_profiles_by_date = get_feature_frame_via_sqlalchemy(
        min_date,
        max_date,
        moving_window_length
    )

    user_profiles_by_date[NUMERIC_COLUMNS].fillna(0, inplace=True)
    user_profiles_by_date['user_ids'] = user_profiles_by_date['user_ids'].apply(unique_list)

    if user_profiles_by_date['date'].min()  > min_date.date():
        raise ValueError(f'''While the specified min_date is {min_date.date()}, 
        only data going back to {user_profiles_by_date['date'].min()} were retrieved''')

    if user_profiles_by_date['date'].max() < max_date.date():
        raise ValueError(f'''While the specified max_date is {max_date.date()}, 
        only data up until {user_profiles_by_date['date'].max()} were retrieved''')

    return user_profiles_by_date


def transform_bool_columns_to_int(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform True / False columns into 0/1 columns
    :param data:
    :return:
    '''
    for column in [column for column in data.columns if re.search('is_', column)]:
        data[column] = data[column].astype(int)

    return data


def encode_uncommon_categories(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Categories for columns such as browser with less than 5 % frequency get lumped into 'Other'
    :param data:
    :return:
    '''
    data.loc[data['device'] == 0, 'device'] = 'Desktop'
    for column in CATEGORICAL_COLUMNS:
        column_values_to_recode = list(
            data[column].value_counts(normalize=True)[data[column].value_counts(normalize=True) < 0.05].index)
        data.loc[data[column].isin(column_values_to_recode), column] = 'Other'

    return data


def generate_category_lists_dict(data: pd.DataFrame, categorical_columns: List[str]) -> Dict:
    '''
    Create lists of individual category variables for consistent encoding between train set, test set and
    prediciton set
    :param data:
    :param categorical_columns:
    :return:
    '''
    category_list_dict = {}
    for column in categorical_columns:
        category_list_dict[column] = list(data[column].unique()) + ['Unknown']

    return category_list_dict


def encode_unknown_categories(data: pd.Series, category_list: List[str]) -> pd.Series:
    '''
    In train and predictiom set, we mighr encounter categories that weren't present in the test set, in order
    to allow for the prediction algorithm to handle these, we create an Unknown category
    :param data:
    :param category_list:
    :return:
    '''
    data[~(data.isin(category_list))] = 'Unknown'

    return data


def generate_dummy_columns_from_categorical_column(data: pd.Series, category_lists_dict: Dict) -> pd.DataFrame:
    '''
    Generate 0/1 columns from categorical columns handling the logic for Others and Unknown categories. Always drops
    the Unknown columns (since we only need k-1 columns for k categories)
    :param data:
    :param category_lists_dict:
    :return:
    '''
    data = encode_unknown_categories(data, category_lists_dict[data.name])
    dummies = pd.get_dummies(pd.Categorical(data, categories=category_lists_dict[data.name]))
    dummies.columns = [data.name + '_' + dummy_column for dummy_column in dummies.columns]
    dummies.drop(columns=data.name + '_Unknown', axis=1, inplace=True)
    dummies.index = data.index

    return dummies


def replace_dummy_columns_with_dummies(data: pd.DataFrame, category_lists_dict: Dict) -> pd.DataFrame:
    '''
    Generates new dummy columns and drops the original categorical ones
    :param data:
    :param category_lists_dict:
    :return:
    '''
    for column in CATEGORICAL_COLUMNS:
        data = pd.concat(
            [
                data,
                generate_dummy_columns_from_categorical_column(
                    data[column],
                    category_lists_dict)],
            axis=1)
    data.drop(columns=CATEGORICAL_COLUMNS, axis=1, inplace=True)

    return data


def instantiate_label_encoder(labels: List['str']):
    '''
    encodes outcome labels into ints
    :param labels:
    :return:
    '''
    le = LabelEncoder()
    le.fit(labels)

    return le


def create_train_test_transformations(
        data: pd.DataFrame,
        split_type: split_type = 'random',
        split_ratio: float = 7 / 10,
        ):
    '''
    Splits train / test applying dummification and scaling to their variables
    :param data:
    :param split_type:
    :param split_ratio:
    :return:
    '''
    model_date = data['date'].max() + timedelta(days=1)

    if split_type == 'random':
        train, test = train_test_split(data, test_size=(1 - split_ratio), random_state=42)
        train_indices = train.index
        test_indices = test.index
        del (train, test)
    else:
        dates = pd.to_datetime(
            pd.Series([date.date() for date in pd.date_range(data['date'].min(), data['date'].max())]))
        train_date = dates[0:int(round(len(dates) * split_ratio, 0))].max()
        train_indices = data[data['date'] <= train_date.date()].index
        test_indices = data[data['date'] > train_date.date()].index

    X_train = data.iloc[train_indices].drop(columns=['outcome', 'user_ids'])
    X_test = data.iloc[test_indices].drop(columns=['outcome', 'user_ids'])
    category_lists_dict = generate_category_lists_dict(X_train, CATEGORICAL_COLUMNS)

    path_to_model_files = os.getenv('PATH_TO_MODEL_FILES')
    with open(
            f'{path_to_model_files}category_lists_{model_date}.json', 'w') as outfile:
        json.dump(category_lists_dict, outfile)

    X_train = replace_dummy_columns_with_dummies(X_train, category_lists_dict)
    X_test = replace_dummy_columns_with_dummies(X_test, category_lists_dict)

    Y_train = data.loc[train_indices, 'outcome'].sort_index()
    Y_test = data.loc[test_indices, 'outcome'].sort_index()
    X_train_numeric = data.loc[train_indices, NUMERIC_COLUMNS].fillna(0)
    X_test_numeric = data.loc[test_indices, NUMERIC_COLUMNS].fillna(0)
    scaler = MinMaxScaler()
    X_train_numeric = pd.DataFrame(scaler.fit_transform(X_train_numeric), index=train_indices,
                                   columns=NUMERIC_COLUMNS).sort_index()

    X_test_numeric = pd.DataFrame(scaler.transform(X_test_numeric), index=test_indices,
                                  columns=NUMERIC_COLUMNS).sort_index()
    X_train = pd.concat([X_train_numeric.sort_index(), X_train[
        [column for column in X_train.columns
         if column not in NUMERIC_COLUMNS + CONFIG_COLUMNS + BOOL_COLUMNS]].sort_index()], axis=1)
    X_test = pd.concat([X_test_numeric.sort_index(), X_test[
        [column for column in X_train.columns
         if column not in NUMERIC_COLUMNS + CONFIG_COLUMNS + BOOL_COLUMNS]].sort_index()], axis=1)

    joblib.dump(
        scaler,
        f'{path_to_model_files}scaler_{model_date}.pkl'
    )

    return X_train, X_test, Y_train, Y_test


def delete_existing_model_file_for_same_date(filename: str, model_date) -> datetime.date:
    '''
    Deletes model files should they already exist for the given date
    :param filename:
    :param model_date:
    :return:
    '''
    if filename != 'category_lists':
        suffix = 'pkl'
    else:
        suffix = 'json'
    path_to_model_files = os.getenv('PATH_TO_MODEL_FILES')
    if f'scaler_{model_date}.pkl' in os.listdir(
            path_to_model_files):
        os.remove(f'{path_to_model_files}{filename}_{model_date}.{suffix}')


def train_model(
        data: pd.DataFrame,
        training_split_parameters: Dict={},
        model_arguments: Dict = {'n_estimators': 100},
        undersampling_factor=1
) -> (pd.DataFrame, RandomForestClassifier):
    '''
    Trains a new model given a full dataset
    :param data:
    :param training_split_parameters:
    :param model_arguments:
    :return:
    '''
    label_encoder = instantiate_label_encoder(LABELS)
    label_range = list(range(0, len(LABELS)))
    data['outcome'] = label_encoder.transform(data['outcome'])

    X_train, X_test, Y_train, Y_test = create_train_test_transformations(
        data,
        **training_split_parameters)
    X_train, Y_train = undersample_majority_class(X_train, Y_train, undersampling_factor)
    X_train.fillna(0.0, inplace=True)
    X_test.fillna(0.0, inplace=True)

    classifier_instance = RandomForestClassifier(**model_arguments)
    classifier_model = classifier_instance.fit(X_train, Y_train)
    outcome_frame = pd.concat([
        pd.DataFrame(list(precision_recall_fscore_support(Y_train,
                                                          classifier_model.predict(X_train),
                                                          labels=label_range))),
        pd.DataFrame(list(
            precision_recall_fscore_support(Y_test, classifier_model.predict(X_test), labels=label_range)))
    ],
        axis=1)
    outcome_frame.columns = [str(label) + '_train'
                             for label in outcome_frame.columns[0:len(label_range)]] + \
                            [str(label) + '_test'
                             for label in
                             outcome_frame.columns[
                             len(label_range):(2*len(label_range))]]
    for i in label_range:
        outcome_frame.columns = [re.sub(str(i), label_encoder.inverse_transform([i])[0], column)
                                 for column in outcome_frame.columns]
    outcome_frame.index = ['precision', 'recall', 'f-score', 'support']

    return classifier_model, outcome_frame


def create_feature_frame(
        min_date: datetime = datetime.utcnow() - timedelta(days=31),
        max_date: datetime = datetime.utcnow() - timedelta(days=1),
        moving_window_length: int=7
) -> pd.DataFrame:
    '''
    Feature frame applies basic sanitization (Unknown / bool columns transformation) and keeps only users
    that were active a day ago
    :param min_date:
    :param max_date:
    :param moving_window_length:
    :return:
    '''
    user_profiles = get_user_profiles_by_date(max_date=max_date,
                                              min_date=min_date,
                                              moving_window_length=moving_window_length)
    test_outcome = user_profiles['outcome'].fillna(
        user_profiles.groupby('browser_id')['outcome'].fillna(method='bfill')
    )
    test_outcome = test_outcome.fillna(
        user_profiles.groupby('browser_id')['outcome'].fillna(method='ffill')
    )
    user_profiles['outcome'] = test_outcome
    user_profiles = encode_uncommon_categories(user_profiles)
    user_profiles = transform_bool_columns_to_int(user_profiles)
    user_profiles_for_prediction = user_profiles[user_profiles['days_active_count'] >= 1].reset_index(drop=True)

    return user_profiles_for_prediction


def model_training_pipeline(
        min_date: datetime = datetime.utcnow() - timedelta(days=31),
        max_date: datetime = datetime.utcnow() - timedelta(days=1),
        moving_window_length: int=7,
        training_split_parameters: Dict={'split_type': 'random', 'split_ratio': 7/10},
        model_arguments: Dict = {'n_estimators': 100},
        overwrite_files: bool=True,
        undersampling_factor: int=1
) -> pd.DataFrame:
    '''
    Pipeline that outputs a trained model and it's accuracy measures
    :param min_date:
    :param max_date:
    :param moving_window_length:
    :param training_split_parameters:
    :param model_arguments:
    :param overwrite_files:
    :return:
    '''
    feature_frame = create_feature_frame(
        max_date=max_date,
        min_date=min_date,
        moving_window_length=moving_window_length
    )
    model_date = feature_frame['date'].max() + timedelta(days=1)

    if overwrite_files:
        for model_file in ['category_lists', 'scaler', 'model']:
            delete_existing_model_file_for_same_date(model_file, model_date)

    model, outcome_frame = train_model(feature_frame,
                                       training_split_parameters=training_split_parameters,
                                       model_arguments=model_arguments,
                                       undersampling_factor=undersampling_factor)
    path_to_model_files = os.getenv('PATH_TO_MODEL_FILES')
    joblib.dump(
        model,
        f'{path_to_model_files}model_{model_date}.pkl'
    )

    return outcome_frame


def load_model_related_constructs(
        scoring_date: datetime=datetime.utcnow()
) -> (Dict, MinMaxScaler, RandomForestClassifier):
    '''
    Serves for the prediction pipeline in order to load model & additional transformation config objects
    :param scoring_date:
    :return:
    '''
    path_to_model_files = os.getenv('PATH_TO_MODEL_FILES')        
    model_related_file_list = os.listdir(path_to_model_files)
    last_model_related_files = {}
    for model_related_file in ['category_lists', 'scaler', 'model']:
        last_file_date = {parse(re.sub(f'{model_related_file}_|.json|.pkl', '', filename)):
                          abs(parse(re.sub(f'{model_related_file}_|.json|.pkl', '', filename)).date()
                              - scoring_date.date())
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
    if not path_to_model_files:
        path_to_model_files = ''
    with open(path_to_model_files +
              'category_lists_' + str(last_model_related_files['category_lists']) + '.json', 'r') as outfile:
        category_lists_dict = json.load(outfile)

    scaler = joblib.load(path_to_model_files + 'scaler_' + str(last_model_related_files['scaler']) + '.pkl')
    model = joblib.load(path_to_model_files + 'model_' + str(last_model_related_files['model']) + '.pkl')

    return category_lists_dict, scaler, model


def batch_predict(
        min_date: datetime = datetime.utcnow() - timedelta(days=2),
        max_date: datetime = datetime.utcnow() - timedelta(days=1),
        moving_window_length: int=7,
) -> pd.DataFrame:
    '''
    Outputs predictions for a given time period
    :param min_date:
    :param max_date:
    :param moving_window_length:
    :return:
    '''
    feature_frame = create_feature_frame(
        max_date=max_date,
        min_date=min_date,
        moving_window_length=moving_window_length
    )

    category_lists_dict, scaler, model = load_model_related_constructs()
    feature_frame = replace_dummy_columns_with_dummies(feature_frame, category_lists_dict)
    feature_frame_numeric = pd.DataFrame(scaler.transform(feature_frame[NUMERIC_COLUMNS]), index=feature_frame.index,
                                         columns=NUMERIC_COLUMNS).sort_index()
    X_all = pd.concat([feature_frame_numeric, feature_frame[
        [column for column in feature_frame.columns
         if column not in NUMERIC_COLUMNS + CONFIG_COLUMNS + BOOL_COLUMNS]].sort_index()], axis=1)
    X_all = X_all.drop(['outcome', 'user_ids'], axis=1)
    predictions = pd.DataFrame(model.predict_proba(X_all))
    label_range = range(len(LABELS))
    label_encoder = instantiate_label_encoder(LABELS)
    for i in label_range:
        predictions.columns = [re.sub(str(i), label_encoder.inverse_transform(i) + '_probability', str(column))
                               for column in predictions.columns]

    predictions = pd.concat([feature_frame[['date', 'browser_id', 'user_ids']],  predictions], axis=1)
    predictions['predicted_outcome'] = label_encoder.inverse_transform(model.predict(X_all))

    return predictions


def generate_and_upload_prediction(
        min_date: datetime=datetime.utcnow() - timedelta(days=2),
        max_date: datetime=datetime.utcnow() - timedelta(days=1),
        moving_window_length: int=7,
):
    '''
    Generates outcome prediction for conversion and uploads them to the DB
    :param min_date:
    :param max_date:
    :param moving_window_length:
    :return:
    '''
    predictions = batch_predict(
        min_date,
        max_date,
        moving_window_length,
    )

    predictions['model_version'] = CURRENT_MODEL_VERSION
    predictions['created_at'] = datetime.utcnow()
    predictions['updated_at'] = datetime.utcnow()

    _, postgres = create_connection(os.getenv('POSTGRES_CONNECTION_STRING'))
    create_predictions_table(postgres)
    create_predictions_job_log(postgres)
    postgres.execute(
        sqlalchemy.sql.text(queries['upsert_predictions']), predictions.to_dict('records')
    )

    prediction_job_log = predictions[
        ['date', 'model_version', 'created_at', 'updated_at']].head(1).to_dict('records')[0]
    prediction_job_log['rows_predicted'] = len(predictions)
    postgres.execute(
        sqlalchemy.sql.text(queries['upsert_prediction_job_log']), [prediction_job_log]
    )


def mkdatetime(datestr: str) -> datetime:
    try:
        return parse(datestr)
    except ValueError:
        raise ValueError('Incorrect Date String')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('action',
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
                        default={'split_type': 'time_based', 'split_ratio': 6 / 10},
                        required=False)
    parser.add_argument('--model-arguments',
                        help='Parameters for scikit model training',
                        type=json.loads,
                        default={'n_estimators': 50},
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
        outcome_frame = model_training_pipeline(
            min_date=args['min_date'],
            max_date=args['max_date'],
            moving_window_length=args['moving_window_length'],
            training_split_parameters=args['training_split_parameters'],
            # model_arguments=args['model_arguments'],
            model_arguments={'n_estimators': 250},
            overwrite_files=args['overwrite_files'],
            undersampling_factor=500
        )

        metrics = ['precision', 'recall', 'f1_score', 'suport']
        print({metrics[i]: outcome_frame.to_dict('records')[i] for i in range(0, len(metrics))})
    elif args['action'] == 'predict':
        generate_and_upload_prediction(
            min_date=args['min_date'],
            max_date=args['max_date'],
            moving_window_length=args['moving_window_length'],
        )
    else:
        raise ValueError('Unknown action specified')


def undersample_majority_class(X_train, Y_train, undersampling_factor=1):
    joined_df = pd.concat([X_train, Y_train], axis=1)
    positive_outcomes = joined_df[joined_df['outcome'].isin([0, 2])]
    negative_outcome = joined_df[joined_df['outcome'] == 1].sample(frac=1 / undersampling_factor, random_state=42)
    sampled_df = pd.concat([positive_outcomes, negative_outcome], axis=0)
    X_sample = sampled_df.drop('outcome', axis=1)
    Y_sample = sampled_df['outcome']

    return X_sample, Y_sample
