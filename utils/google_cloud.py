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

# Get GCS credentials
bucket_name = os.getenv('GCS_bucket_name')
project_id = os.getenv('GCS_project_id')
private_key_id = os.getenv('GCS_private_key_id')
private_key = os.getenv('GCS_private_key')
client_email = os.getenv('GCS_client_email')
client_id = os.getenv('GCS_client_id')
client_x509_cert_url = os.getenv('GCS_client_x509_cert_url')

# %% Create API client.
credentials = service_account.Credentials.from_service_account_info({
    "type": "service_account",
    "project_id": project_id,
    "private_key_id": private_key_id,
    "private_key": private_key,
    "client_email": client_email,
    "client_id": client_id,
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": client_x509_cert_url
    })

client = storage.Client(credentials=credentials)
bucket = client.bucket(bucket_name)

# %% Upload File
def upload_blob(source_file_name, destination_blob_name):
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
    current_time = time.time()
    for file in os.listdir(dataFolder):
        source_file_name = f'{dataFolder}/{file}'
        time_delta = current_time - os.path.getmtime(source_file_name)

        if time_delta < 60 * 60:
            destination_blob_name = f'data/{file}'
            upload_blob(source_file_name, destination_blob_name)
        else:
            logNote = f'{file} not modified recently, do not upload to GCS'
            logger.info(logNote)

dataFolder = utils.get_data_folder()

if __name__ == '__main__':
    dataFolder = os.getcwd() + '/data'
    print(dataFolder)
    main()
