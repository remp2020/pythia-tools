from db_utils import get_sqlalchemy_tables_w_session, create_connection
import os
from dotenv import load_dotenv
load_dotenv('../../../.env')

create_connection(os.getenv('BQ_CONNECTION_STRING'), engine_kwargs={'credentials_path': '../../../client_secrets.json'})
