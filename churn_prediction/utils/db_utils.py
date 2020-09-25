import pandas as pd
import os


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
        client_secrets_path = os.getenv('GCLOUD_CREDENTIALS_SERVICE_ACCOUNT_JSON_KEY_PATH')
        credentials = service_account.Credentials.from_service_account_file(
            client_secrets_path,
        )

        self.user_ids_frame.to_gbq(
            destination_table=f'{os.getenv("BIGQUERY_DATASET")}.user_ids_filter',
            project_id=os.getenv('BIGQUERY_PROJECT_ID'),
            credentials=credentials,
            if_exists='replace'
        )