'''

https://pirateweather.net/

Google NOAA Datasets
    https://console.cloud.google.com/marketplace/browse?filter=partner:NOAA&project=python-232920
Amazon NOAA Datasets
    https://registry.opendata.aws/collab/noaa/


High Resolution Rapid Refresh Model (HRRR) 
    https://rapidrefresh.noaa.gov/hrrr/

    The HRRR Zarr Archive Managed by MesoWest by Taylor Gowan 
        https://mesowest.utah.edu/html/hrrr/

    HRRR-B Python package: download and read HRRR grib2 files by Brian Blaylock 
        https://github.com/blaylockbk/Herbie

    Using Cloud Computing to Analyze Model Output Archived in Zarr Format 
        https://journals.ametsoc.org/view/journals/atot/39/4/JTECH-D-21-0106.1.xml
'''
from google.cloud import storage

# https://console.cloud.google.com/storage/browser/high-resolution-rapid-refresh?prefix=&forceOnObjectsSortingFiltering=false&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))
bucket_name = 'high-resolution-rapid-refresh'
source_blob_name = ''
destination_file_name = ''


def download_public_file(bucket_name, source_blob_name, destination_file_name):
    """Downloads a public blob from the bucket.

    bucket_name = "your-bucket-name"
    source_blob_name = "storage-object-name"
    destination_file_name = "local/path/to/file"

    https://cloud.google.com/storage/docs/access-public-data?hl=en_US#storage-download-public-object-python
    """
    


    storage_client = storage.Client.create_anonymous_client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded public blob {} from bucket {} to {}.".format(
            source_blob_name, bucket.name, destination_file_name
        )
    )

if __name__ == '__main__':
    print('WEATHER')