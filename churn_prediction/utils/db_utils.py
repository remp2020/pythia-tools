import pandas as pd
import sqlalchemy
import os
from typing import List, Dict, Any
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table
from google.cloud import bigquery
from cmd.bigquery_export.upload import BigQueryUploader


CUSTOM_USER_DEFINED_TYPES = ['conversion_prediction_outcomes', 'conversion_prediction_model_versions']


def create_connection(connection_string: str, engine_kwargs: Dict[str, Any] = {}):
    '''
    Creates a connection to a DB based on a connection string in .env file
    :param connection_string: connection string to connect to
    :param engine_kwargs:
    :return:
    '''
    if connection_string is None:
        raise ValueError(f'Unknown connection, please check if .env contains connection string')

    engine = sqlalchemy \
        .create_engine(connection_string, **engine_kwargs)

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


def get_sqla_table(table_name, engine, kwargs: Dict[str, str] = None):
    if not kwargs:
        kwargs = {}
    meta = MetaData(bind=engine)
    table = Table(table_name, meta, autoload=True, autoload_with=engine, **kwargs)
    return table


class DailyProfilesHandler:
    def __init__(
            self,
            project_id,
            credentials,
            csv_path='csv'
    ):
        self.project_id = project_id
        self.credentials = credentials
        self.csv_path = csv_path
        # 1 years of data should always be ok
        self.default_table_expiration = 24 * 60 * 60 * 365
        self.date_col_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
            expiration_ms=self.default_table_expiration * 1000,
        )
        self.uploader = BigQueryUploader(
            project_id=self.project_id,
            dataset_id=os.getenv("SCHEMA"),
            tmp_folder=self.csv_path,
            credentials=self.credentials
        )

    def create_daily_profiles_table(self, logger):
        table = 'rolling_daily_user_profile'

        if not self.uploader.table_exists(table):
            schema = [
                bigquery.SchemaField('date', 'DATE'),
                bigquery.SchemaField('user_id', 'STRING'),
                bigquery.SchemaField('outcome', 'STRING'),
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
            ]

            self.uploader.create_table(table_id=table, schema=schema, time_partitioning=self.date_col_partitioning)
            logger.info(f'Created table {table}')
        else:
            logger.info(f'Table {table} already exists')

    def upload_data(self, data):
        data['date'] = data['date'].astype(str)
        data['created_at'] = data['created_at'].astype(str)
        self.uploader.upload_csv_to_table(
            table_id='rolling_daily_user_profile',
            data_source=data
        )


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
          user_ids TEXT NULL,
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


def migrate_user_id_to_user_ids(db_engine):
    '''
    Originally user_id column only hosted one user_id, but our prediction is on the level of browser which meants
    this added unintented granularity to the data. This migration introduces an ARRAY column user_ids that handled the
    1:N relationship between browser_ids and user_ids
    :param db_engine:
    :return:
    '''
    for table_name in ['aggregated_browser_days', 'conversion_predictions_daily']:
        table = get_sqla_table(table_name=table_name, engine=db_engine)
        table_columns = [column.name for column in table.columns]
        if 'user_id' in table_columns and 'user_ids' not in table_columns:
            db_engine.execute(f'ALTER TABLE {table_name} ADD column user_ids TEXT[]')
            db_engine.execute(f"UPDATE {table_name} SET user_ids = STRING_TO_ARRAY(user_id, ',')")
            db_engine.execute(f"ALTER TABLE {table_name} DROP column user_id")


# Stolen from https://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType

# python2/3 compatible.
PY3 = str is not bytes
text = str if PY3 else unicode
int_type = int if PY3 else (int, long)
str_type = str if PY3 else (str, unicode)


class StringLiteral(String):
    """Teach SA how to literalize various things."""
    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, int_type):
                return text(value)
            if not isinstance(value, str_type):
                value = text(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result
        return process


class LiteralDialect(DefaultDialect):
    colspecs = {
        # prevent various encoding explosions
        String: StringLiteral,
        # teach SA about how to literalize a datetime
        DateTime: StringLiteral,
        # don't format py2 long integers to NULL
        NullType: StringLiteral,
    }


def literalquery(statement):
    """NOTE: This is entirely insecure. DO NOT execute the resulting strings."""
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        statement = statement.statement
    return statement.compile(
        dialect=LiteralDialect(),
        compile_kwargs={'literal_binds': True},
    ).string


class UserIdHandler:
    def __init__(
            self,
            date,
            expiration_lookahead: int=30
    ):
        from .mysql import get_users_with_expirations
        self.user_ids = get_users_with_expirations(
            date,
            expiration_lookahead
        )
        self.user_ids_frame = pd.DataFrame()

    def upload_user_ids(self):
        self.user_ids_frame['user_id'] = self.user_ids
        from google.oauth2 import service_account
        client_secrets_path = os.getenv('PATH_TO_GCLOUD_CREDENTIALS_JSON')
        credentials = service_account.Credentials.from_service_account_file(
            client_secrets_path,
        )

        self.user_ids_frame.to_gbq(
            destination_table=f'{os.getenv("SCHEMA")}.user_ids_filter',
            project_id=os.getenv('BQ_DATABASE'),
            credentials=credentials,
            if_exists='replace'
        )