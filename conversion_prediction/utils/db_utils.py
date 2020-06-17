import pandas as pd
import sqlalchemy
import os
from typing import List, Dict, Any
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table


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
