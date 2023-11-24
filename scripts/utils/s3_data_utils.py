import boto3
import logging
import os
import io

from scripts.utils.file_utils import read_config
from botocore.exceptions import NoCredentialsError
from scripts.init_config import config
import pandas as pd


class S3DataUtils:

    @staticmethod
    def get_s3_client():

        cred = read_config(section='AWS')
        region_name = cred['REGION_NAME']
        aws_access_key_id = cred['AWS_SECRET_KEY']
        aws_secret_access_key = cred['AWS_SECRET_ACCESS_KEY']
        bucket_name = cred['BUCKET_NAME']

        # Create an S3 client
        client = boto3.client(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        return client, bucket_name

    @staticmethod
    def list_objects_in_folder(folder_path: str):

        log = logging.getLogger(os.path.basename(__file__))

        try:
            s3_client, bucket_name = S3DataUtils.get_s3_client()

            # List objects in the specified folder
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

            # Print the object keys
            log.info(f"Objects in {bucket_name}/{folder_path}:")

            for obj in response.get('Contents', []):
                log.info(f"{obj['Key']}")

        except NoCredentialsError as nc:
            log.error(f"Credentials not available or incorrect {nc}.")

        except Exception as e:
            log.error(f"Error: {e}")


    @staticmethod
    def download_from_s3_to_dataframe(table_name: str):

        log = logging.getLogger(os.path.basename(__file__))
        df = None
        try:

            file_key = f'student4/migrationData/{table_name}.csv'

            # Download locally
            s3_client, bucket_name = S3DataUtils.get_s3_client()
            s3_client.download_file(bucket_name, file_key , os.path.join(config.DIR_S3_DOWNLOAD, f'{table_name}.csv'))

            # From obj to dataframe
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            csv_data = response['Body'].read().decode('utf-8')

                # Convert the CSV data string into a file-like object
            csv_file = io.StringIO(csv_data)

            df = pd.read_csv(csv_file, sep='|')

        except NoCredentialsError as nc:
            log.info(f"Credentials not available or incorrect {nc}.")
        except Exception as e:
            log.info(f"Error: {e}")

        return df


    @staticmethod
    def upload_to_s3():

        log = logging.getLogger(os.path.basename(__file__))

        try:
            # read credentials
            cred = read_config(section='AWS')
            region_name = cred['REGION_NAME']
            aws_access_key_id = cred['AWS_SECRET_KEY']
            aws_secret_access_key = cred['AWS_SECRET_ACCESS_KEY']

            bucket_name = cred['BUCKET_NAME']
            _upload_folder = 'student4/migrationData'

            # set logs

            # Create an S3 client
            s3 = boto3.client(
                service_name='s3',
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )

            # Get a list of all files in the local directory
            local_files = [os.path.join(config.DIR_CSV, file) for file in os.listdir(config.DIR_CSV) if
                           file.endswith('.csv')]

            # Upload each local file to S3
            for _file in local_files:
                # Extract the file name without the path
                file_name = os.path.basename(_file)

                # Construct the S3 key by combining the folder_name and file_name
                s3_key = f"{_upload_folder}/{file_name}"

                # Upload the file to S3
                s3.upload_file(_file, bucket_name, s3_key)

                log.info(f"Uploaded {file_name} to {bucket_name}/{s3_key}")

        except NoCredentialsError:
            log.error("Credentials not available or incorrect.")
            return False
        except ValueError as ve:
            log.error(f"Error: {ve}")
            return False
        except Exception as e:
            log.error(f"Error: {e}")
            return False




