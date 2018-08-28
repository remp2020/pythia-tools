from os import environ
from os.path import isfile

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

def create_connection(connection_string: str, autocommit: bool=True):
    '''
    Creates a connection to a DB based on a connection string in .env file
    :param connection_string: connection string to connect to
    :param autocommit:
    :return:
    '''
    if connection_string is None:
        raise ValueError(f'Unknown connection, please check if .env contains connection string')

    engine = sqlalchemy \
        .create_engine(connection_string)

    connection = engine \
        .connect() \
        .execution_options(autocommit=True)

    return engine, connection


def retrieve_data_for_query_key(
        query_string: str,
        query_arguments: dict,
        connection
) -> pd.DataFrame:
    query_string = sqlalchemy.sql.text(query_string)
    query = connection.execute(query_string, **query_arguments)
    data = pd.DataFrame(query.fetchall())

    if data.empty:
        raise ValueError(f'No data available')

    data.columns = query.keys()

    return data


def create_predictions_table():
    _, postgres = create_connection(os.getenv('POSTGRES_CONNECTION_STRING'))

    if check_table_existence('conversion_predictions_daily'):
        query = '''
                DROP TABLE conversion_predictions_daily;
            '''
        postgres.execute(query)

    if not check_user_defined_type_exists('conversion_prediction_outcomes'):
        query = '''
            CREATE TYPE conversion_prediction_outcomes AS ENUM ('conversion','no_conversion', 'shared_account_login');
        '''
        postgres.execute(query)

    if not check_user_defined_type_exists('conversion_prediction_model_versions'):
        query = '''
            CREATE TYPE conversion_prediction_model_versions AS ENUM ('1.0');
        '''
        postgres.execute(query)

    query = '''
        CREATE TABLE IF NOT EXISTS conversion_predictions_daily (
          id SERIAL PRIMARY KEY,
          date TIMESTAMPTZ,
          browser_id TEXT,
          predicted_outcome conversion_prediction_outcomes,
          conversion_probability FLOAT,
          no_conversion_probability FLOAT,
          shared_account_login_probability FLOAT,
          model_version conversion_prediction_model_versions,
          created_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ,
          UNIQUE(date, browser_id)
        );
    '''

    postgres.execute(query)

    for index_query in [
        'CREATE INDEX browser_id ON conversion_predictions_daily (browser_id);',
        'CREATE INDEX browser_date ON conversion_predictions_daily (date, browser_id);',
        'CREATE INDEX predicted_outcome ON conversion_predictions_daily (predicted_outcome)'
    ]:
        postgres.execute(index_query)


def create_predictions_job_log():
    _, postgres = create_connection(os.getenv('POSTGRES_CONNECTION_STRING'))

    if check_table_existence('prediction_job_log'):
        query = '''
                DROP TABLE prediction_job_log;
            '''
        postgres.execute(query)

    query = '''
        CREATE TABLE IF NOT EXISTS prediction_job_log (
          id SERIAL PRIMARY KEY,
          date TIMESTAMPTZ,
          rows_predicted INT,
          model_version conversion_prediction_model_versions,
          created_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ,
          UNIQUE(date, model_version)
        );
    '''

    postgres.execute(query)

    for index_query in [
        'CREATE INDEX date ON prediction_job_log (date);',
    ]:
        postgres.execute(index_query)


def check_table_existence(table_name: str) -> bool:
    _, postgres = create_connection(os.getenv('POSTGRES_CONNECTION_STRING'))

    query = sqlalchemy.sql.text('''
        SELECT 
          EXISTS (
            SELECT 
              1
            FROM   
              information_schema.tables
            WHERE
              table_name = :table_name
       );
    ''')

    return postgres.execute(query, table_name=table_name).fetchall()[0][0]


def check_user_defined_type_exists(type_name: str) -> bool:
    _, postgres = create_connection(os.getenv('POSTGRES_CONNECTION_STRING'))

    query = sqlalchemy.sql.text('''
        SELECT 
          EXISTS (
            SELECT 
              1
            FROM   
              pg_type
            WHERE
              typname = :type_name
       );
    ''')

    return postgres.execute(query, type_name=type_name).fetchall()[0][0]
