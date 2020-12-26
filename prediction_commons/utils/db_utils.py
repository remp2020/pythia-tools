import pandas as pd
import sqlalchemy
import os
from typing import Dict, Any, List
from sqlalchemy import MetaData, Table
from google.cloud import bigquery
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from bigquery_export.upload import BigQueryUploader


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


def get_sqlalchemy_tables_w_session(
        table_names: List[str] = None
) -> (Dict, Engine):
    table_mapping = {}
    database = os.getenv('BIGQUERY_PROJECT_ID')
    _, db_connection = create_connection(
        f'bigquery://{database}',
        {'credentials_path': os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')}
    )
    schema = os.getenv('BIGQUERY_DATASET')
    for table in table_names:
        table_mapping[table] = get_sqla_table(
            table_name=f'{database}.{schema}.{table}', engine=db_connection,
        )

    table_mapping['session'] = sessionmaker(bind=db_connection)()

    return table_mapping, db_connection


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


class TableHandler:
    def __init__(
            self,
            project_id,
            credentials,
            table_name,
            table_schema,
            csv_path='csv'
    ):
        self.project_id = project_id
        self.credentials = credentials
        self.table_name = table_name
        self.table_schema = table_schema
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
            dataset_id=os.getenv("BIGQUERY_DATASET"),
            tmp_folder=self.csv_path,
            credentials=self.credentials
        )

    def create_table(self, logger):
        if not self.uploader.table_exists(self.table_name):
            self.uploader.create_table(
                table_id=self.table_name,
                schema=self.table_schema,
                time_partitioning=self.date_col_partitioning
            )
            logger.info(f'Created table {self.table_name}')
        else:
            logger.info(f'Table {self.table_name} already exists')

    def upload_data(self, data):
        data['date'] = data['date'].astype(str)
        data['created_at'] = data['created_at'].astype(str)
        self.uploader.upload_csv_to_table(
            table_id='rolling_daily_user_profile',
            data_source=data
        )


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
