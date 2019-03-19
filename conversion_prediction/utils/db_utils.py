import pandas as pd
import sqlalchemy
from typing import List


CUSTOM_USER_DEFINED_TYPES = ['conversion_prediction_outcomes', 'conversion_prediction_model_versions']


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
        connection: sqlalchemy.engine
) -> pd.DataFrame:
    query_string = sqlalchemy.sql.text(query_string)
    query = connection.execute(query_string, **query_arguments)
    data = pd.DataFrame(query.fetchall())

    if data.empty:
        raise ValueError(f'No data available')

    data.columns = query.keys()

    return data


def create_predictions_table(connection: sqlalchemy.engine):
    existing_user_defined_types = retrieve_user_defined_type_existence(connection)

    if 'conversion_prediction_outcomes' not in existing_user_defined_types:
        query = '''
            CREATE TYPE conversion_prediction_outcomes AS ENUM ('conversion','no_conversion', 'shared_account_login');
        '''
        connection.execute(query)

    if 'conversion_prediction_model_versions' not in existing_user_defined_types:
        query = '''
            CREATE TYPE conversion_prediction_model_versions AS ENUM ('1.0');
        '''
        connection.execute(query)

    query = '''
        CREATE TABLE IF NOT EXISTS conversion_predictions_daily (
          id SERIAL PRIMARY KEY,
          date TIMESTAMPTZ,
          browser_id TEXT,
          user_id TEXT NULL,
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

    connection.execute(query)

    if not check_indices_existence('conversion_predictions_daily', connection):
        for index_query in [
            'CREATE INDEX browser_id ON conversion_predictions_daily (browser_id);',
            'CREATE INDEX user_id ON conversion_predictions_daily (user_id);',
            'CREATE INDEX browser_date ON conversion_predictions_daily (date, browser_id);',
            'CREATE INDEX predicted_outcome ON conversion_predictions_daily (predicted_outcome)'
        ]:
            connection.execute(index_query)


def create_predictions_job_log(connection: sqlalchemy.engine):
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

    connection.execute(query)

    if not check_indices_existence('prediction_job_log', connection):
        for index_query in [
            'CREATE INDEX date ON prediction_job_log (date);',
        ]:
            connection.execute(index_query)


def retrieve_user_defined_type_existence(connection: sqlalchemy.engine) -> List:
    query = sqlalchemy.sql.text(
        '''
          SELECT
            typname
        FROM
          pg_type
        WHERE
        typname IN :user_defined_types;
    ''')

    type_names = connection.execute(query, user_defined_types=tuple(CUSTOM_USER_DEFINED_TYPES)).fetchall()
    type_names = [type_name[0] for type_name in type_names]

    return type_names


def check_indices_existence(table_name: str, connection: sqlalchemy.engine) -> True:
    query = sqlalchemy.sql.text(
        '''
        SELECT
            t.relname AS table_name,
            i.relname AS index_name,
            a.attname AS column_name
        FROM
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a
        WHERE
            t.oid = ix.indrelid
            AND i.oid = ix.indexrelid
            AND a.attrelid = t.oid
            AND a.attnum = ANY(ix.indkey)
            AND t.relkind = 'r'
            AND t.relname = :table_name
        '''
    )

    indices = connection.execute(query, table_name=table_name).fetchall()

    return len(indices) != 0
