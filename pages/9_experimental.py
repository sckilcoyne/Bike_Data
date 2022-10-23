'''

Connect Streamlit to Google Cloud Storage: 
    https://docs.streamlit.io/knowledge-base/tutorials/databases/gcs
Authenticate to Cloud services using client libraries:
    https://cloud.google.com/docs/authentication/client-libraries#python
Cloud Storage client libraries: 
    https://cloud.google.com/storage/docs/reference/libraries
Reading and writing pickles using Google Cloud: 
    https://stackoverflow.com/questions/60729667/reading-and-writing-pickles-using-google-cloud
How to upload a file to Google Cloud Storage on Python 3?: 
    https://stackoverflow.com/questions/37003862/how-to-upload-a-file-to-google-cloud-storage-on-python-3

'''
# %% Initialize
import streamlit as st
from google.oauth2 import service_account
from google.cloud import storage
import pickle
import pandas as pd

# pylint: disable=invalid-name

bucket_name = "boston-bike-data"

# %% Create API client.
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = storage.Client(credentials=credentials)

# %% Load file
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def read_pickle(fileName):
    """Read file from Google Cloud Storage

    Args:
        fileName (str): Google Cloud Storage file name and path

    Returns:
        Content loaded from Pickle file
    """
    bucket = client.bucket(bucket_name)
    content = bucket.blob(fileName).download_as_string()
    data = pickle.loads(content)
    return data

files = ['broadway_daily_totals.pkl', '4001.pkl']

file_path = f'data/{files[1]}'

df = read_pickle(file_path)

st.write(df)
st.write(df.columns)

