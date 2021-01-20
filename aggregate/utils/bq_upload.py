from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
import os
import csv
import json
import pandas as pd

CSV_BASE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'csv')

class BigQueryUploader:
    def __init__(self, project_id, dataset_id, tmp_folder=CSV_BASE_PATH, credentials=None):
        """
        :param project_id:
        :param dataset_id:
        :param tmp_folder:
        :param credentials: are meant to be used in case we're not running the script from shell with a public key,
        minimal example:
        from google.oauth2 import service_account
        service_account.Credentials.from_service_account_file(path_to_secret_json_file)
        """
        if credentials is None:
            self.client = bigquery.Client()
        else:
            self.client = bigquery.Client(project_id, credentials)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.tmp_folder = tmp_folder

    def __create_tmp_json(self, data_source, array_columns):
        if isinstance(data_source, str):
            return self.__create_tmp_json_from_csv(data_source, array_columns)
        elif isinstance(data_source, pd.DataFrame):
            return self._create_tmp_json_from_pandas_dataframe(data_source, array_columns)
        else:
            raise TypeError(
                f'Unsupported function signature for given data source reference of type {type(data_source)}'
            )

    def is_dataset_ready(self):
        try:
            self.client.get_dataset(self.dataset_id)
            return True
        except NotFound:
            print("Dataset {} is not found".format(self.dataset_id))
            return False

    def list_tables(self):
        tables = self.client.list_tables(self.dataset_id)
        print("Tables contained in '{}':".format(self.dataset_id))
        for table in tables:
            print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def __tid(self, table_id):
        return self.project_id + '.' + self.dataset_id + '.' + table_id

    def table_exists(self, table_id):
        try:
            self.client.get_table(self.__tid(table_id))
            return True
        except NotFound:
            return False

    def get_table(self, table_id):
        return self.client.get_table(self.__tid(table_id))

    def __json_tmp_file(self):
        return os.path.join(self.tmp_folder, "tmp.json")

    def __create_tmp_json_from_csv(self, csv_path, array_columns=None):
        tmpfile_path = self.__json_tmp_file()

        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='|')
            with open(tmpfile_path, 'w') as jsonfile:
                for row in reader:
                    # convert string array columns to arrays
                    if array_columns:
                        for col in array_columns:
                            # array columns exported from PSQL are formated as {value1, value2, ...}
                            content = row[col]
                            # remove opening and closing brackets {}
                            # make an array
                            row[col] = content[1:-1].split(',')

                    jsonfile.write(json.dumps(row) + "\n")
        return tmpfile_path

    def _create_tmp_json_from_pandas_dataframe(self, data, array_columns=None):
        tmpfile_path = self.__json_tmp_file()

        with open(tmpfile_path, 'w') as jsonfile:
            for row in data.to_dict(orient='records'):
                # convert string array columns to arrays
                if array_columns:
                    for col in array_columns:
                        # array columns exported from PSQL are formated as {value1, value2, ...}
                        content = row[col]
                        # remove opening and closing brackets {}
                        # make an array
                        if not isinstance(row[col], list):
                            row[col] = content[1:-1].split(',')

                jsonfile.write(json.dumps(row) + "\n")
        return tmpfile_path

    def upload_to_table(self, table_id, data_source, array_columns=None):
        # TODO: this doesn't correctly process repeated fields, therefore doing conversion to JSON first
        # Pandas DataFrame can be uploaded directly
        # if isinstance(data_source, pd.DataFrame):
        #     load_job = None
        #     try:
        #         table = self.get_table(table_id)
        #         job_config = bigquery.LoadJobConfig()
        #
        #         load_job = self.client.load_table_from_dataframe(data_source, table, job_config=job_config)
        #         load_job.result()  # Waits for the job to complete.
        #         rows_count = data_source.shape[0]
        #         print(
        #             "Uploaded DataFrame ({} row(s)) to BQ table '{}.{}.{}'".format(rows_count, table.project, table.dataset_id, table.table_id)
        #         )
        #     except BadRequest:
        #         print("Unable to upload dataframe, errors:\n")
        #         for err in load_job.errors:
        #             print(err['message'])
        #         raise
        #     return

        rows_count = ''
        if isinstance(data_source, pd.DataFrame):
            rows_count = "(rows: " + str(data_source.shape[0]) + ") "

        # First, we need to convert our data to newline delimited JSON (CSV load doesn't support repeated fields)
        json_path = self.__create_tmp_json(data_source=data_source, array_columns=array_columns)
        try:
            if os.path.getsize(json_path) == 0:
                print("CSV contains no data (after conversion), not uploading")
                return

            table = self.get_table(table_id)

            job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
            with open(json_path, "rb") as source_file:
                load_job = self.client.load_table_from_file(source_file, table,
                                                            job_config=job_config)

            load_job.result()  # Waits for the job to complete.
            print(
                "Uploaded data {}to table {}.{}.{}".format(rows_count, table.project, table.dataset_id, table.table_id)
            )
        except BadRequest:
            print("Unable to upload file, errors:\n")
            for err in load_job.errors:
                print(err['message'])
            raise
        finally:
            if os.path.exists(json_path):
                os.remove(json_path)

    def create_table(self, table_id, schema, time_partitioning=None):
        table = bigquery.Table(self.__tid(table_id), schema=schema)

        if time_partitioning:
            table.time_partitioning = time_partitioning

        table = self.client.create_table(table)  # Make an API request.
        print(
            "Created BiqQuery table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
