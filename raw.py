import boto3
import logging
import os


from scripts.utils.s3_data_utils import S3DataUtils as S3Ut
from scripts.utils.file_utils import read_config
from botocore.exceptions import NoCredentialsError
from scripts.init_config import config






def download_from_s3():

    try:

        cred = read_config(section='AWS')
        region_name = cred['REGION_NAME']
        aws_access_key_id = cred['AWS_SECRET_KEY']
        aws_secret_access_key = cred['AWS_SECRET_ACCESS_KEY']
        bucket_name = cred['BUCKET_NAME']



        # Create an S3 client
        s3 = boto3.client(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        file_path = os.getcwd()
        s3.download_file( 'intership2023', 'student4/expData/CONTACTS.csv', os.path.join(file_path, 'CONTACTS.csv'))

    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except Exception as e:
        print(f"Error: {e}")

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






if __name__ == '__main__':

    config.setup(logs_level='info')

    #check_s3_conn_with_resource()

    #check_s3_connection_with_client()

    #list_s3_objects('intership2023')

    #download_from_s3()

    #list_objects_in_folder('student4/')

    #upload_to_s3()

    #S3Ut.list_objects_in_folder('student4/migrationData/')





