'''Upload files in data folder to google cloud storage

https://cloud.google.com/docs/authentication/client-libraries#python
'''

# %% Initialize
import os
import sys
import time
import logging

from google.oauth2 import service_account
from google.cloud import storage

# pylint: disable=invalid-name

# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilFuncs as utils
# pylint:enable=import-error, wrong-import-position

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)


# %% Create API client.
def GCS_client():
    '''Connect to GCS bucket'''
    # Get GCS credentials
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    project_id = os.getenv('GCS_PROJECT_ID')
    private_key_id = os.getenv('GCS_PRIVATE_KEY_ID')
    private_key = os.getenv('GCS_PRIVATE_KEY').replace('\\n', '\n') # https://github.com/firebase/firebase-admin-python/issues/188
    client_email = os.getenv('GCS_CLIENT_EMAIL')
    client_id = os.getenv('GCS_CLIENT_ID')
    client_x509_cert_url = os.getenv('GCS_CLIENT_X509_CERT_URL')

    # logger.info('bucket_name: %s', bucket_name)
    # logger.info('project_id: %s', project_id)
    # logger.info('private_key_id: %s', private_key_id)
    # logger.info('private_key: %s', private_key)
    # logger.info('client_email: %s', client_email)
    # logger.info('client_id: %s', client_id)
    # logger.info('client_x509_cert_url: %s', client_x509_cert_url)

    # Make GCS connection
    credDict = {
                "type": "service_account","project_id": project_id,
                "private_key_id": private_key_id, "private_key": private_key,
                "client_email": client_email,
                "client_id": client_id,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": client_x509_cert_url
                }
    # logger.info('credDict: %s', credDict)
    credentials = service_account.Credentials.from_service_account_info(credDict)

    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)

    return bucket

# %% Upload File
def upload_blob(source_file_name, destination_blob_name, bucket):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logNote = f'{source_file_name} uploaded to {destination_blob_name}'
    logger.info(logNote)

# %% Run as script
def main():
    '''Upload all files in data folder
    '''
    bucket = GCS_client()
    current_time = time.time()
    for file in os.listdir(dataFolder):
        source_file_name = f'{dataFolder}/{file}'
        time_delta = current_time - os.path.getmtime(source_file_name)

        # Only upload recently updated files
        # https://stackoverflow.com/questions/68377717/using-python-select-files-in-a-directory-that-have-been-modified-in-the-last-60
        if time_delta < 60 * 60:
            destination_blob_name = f'data/{file}'
            upload_blob(source_file_name, destination_blob_name, bucket)
        else:
            logNote = f'{file} not modified recently, do not upload to GCS'
            logger.debug(logNote)

dataFolder = utils.get_data_folder()

if __name__ == '__main__':
    dataFolder = os.getcwd() + '/data'
    print(dataFolder)
    main()
