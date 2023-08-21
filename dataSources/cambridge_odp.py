# -*- coding: utf-8 -*-
'''
Download data from Cambridge Open Data Portal API

Counters:
    Broadway Eco-Totem

https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg
https://data.cambridgema.gov/resource/q8v9-mcfg.json

'''

# %% Initialize
# pylint: disable=invalid-name, broad-except

# Import standard modules
# import pickle
# import time
# import calendar
import os
# import sys
import logging
# from datetime import date, timedelta, datetime
import datetime
import json

from sodapy import Socrata
# from scipy import stats
import pandas as pd

cols_standard = ['StationID', 'StationName', 'Mode', 'DateTime', 'Count']
# StationID: Counter station code
# StationName: Station name used in posts
# Mode: Mode count is for e.g. bike/ped
# DateTime: datetime of count
# Count: Total count for the DateTime of the Mode
#   Counts for specific directions can be extra columns

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)

# %% Handle data files


def load_count_data():
    """Load saved data from files

    Returns:
        [type]: [description]
    """
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    logger.debug('cwd: %s', currentFolder)

    if currentFolder == 'dataSources':
        parent = os.path.dirname(path)
        dataFolder = parent + '/data'
    else:
        dataFolder = path + '/data'

    logger.debug('dataFolder: %s', dataFolder)
    logger.debug('dataFolder contents: %s', os.listdir(path=dataFolder))

    dailyTotals = pd.read_pickle(f'{dataFolder}/broadway-daily_totals.pkl')

    completeData = pd.read_pickle(f'{dataFolder}/broadway-complete.pkl')
    rawData = pd.read_pickle(f'{dataFolder}/broadway-raw.pkl')

    return dailyTotals, completeData, rawData

def save_count_data(newDailyCounts, completeData, rawData):
    """Append new daily counts to saved files.

    Args:
        dailyCounts (dataframe): _description_
        completeData (dataframe): _description_
    """
    completeData.to_pickle('data/broadway-complete.pkl', protocol=3)
    newDailyCounts.to_pickle('data/broadway-daily_totals.pkl', protocol=3)
    rawData.to_pickle('data/broadway-raw.pkl', protocol=3)

    logger.info('Saved updated daily counts.')


# %% API Info
# https://data.cambridgema.gov/resource/q8v9-mcfg.json

appToken = ''  # https://dev.socrata.com/docs/app-tokens.html

# apiBroadway = 'q8v9-mcfg'
cambData = 'data.cambridgema.gov'


# %% Query Dataset

def query_api(counterInfo, startDate, endDate):
    """Download data from Socrata API for the Cambridge Totem.

    Args:
        startDate ([type]): [description]
        endDate ([type]): [description]

    Raises:
        Exception: [description]

    Returns:
        pandas dataframe: [description]
    """
    # queries are inclusive of start and end date
    dateString = 'date between \'' + \
        str(startDate) + '\' and \'' + str(endDate) + '\''

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata(cambData, None)

    # Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.cambridgema.gov,
    #                  MyAppToken,
    #                  userame="user@example.com",
    #                  password="AFakePassword")

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    queryData = client.get(counterInfo[0], select='*', where=dateString)

    if queryData == []:
        logger.error('No downloaded data.')
        # print('print: No downloaded data.')
        raise ValueError('Data Download Failure')

    # Convert to pandas DataFrame
    queryData_df = pd.DataFrame.from_records(queryData)

    # Clean up DataFrame
    queryData_df.rename(columns={'datetime': 'DateTime',
                               'date': 'Date',
                               'time': 'Time',
                               'total': 'Total',
                               'entries': 'Westbound',
                               'exits': 'Eastbound'}, inplace=True)

    queryData_df = queryData_df.astype({'Westbound': int,
                                    'Eastbound': int,
                                    'Total': int,
                                    'DateTime': 'datetime64'})

    queryData_df['Time'] = pd.to_datetime(
        queryData_df['Time'], format='%H:%M:%S', errors='raise')
    queryData_df['Year'] = queryData_df['DateTime'].dt.year

    return queryData_df


def query_dataset(counterInfo, dateList):
    """Determine days not already downloaded from Socrata and download them.

    Args:
        broadwayDailyTotals ([type]): [description]

    Returns:
        pandas dataframe: New data from Socrata
    """

    startDate = min(dateList)
    endDate = max(dateList)

    downloadDatesStr = f'Download data from {startDate} to {endDate}'
    logger.info(downloadDatesStr)

    # Download data
    downloadedData_df = query_api(counterInfo, startDate, endDate)

    return downloadedData_df


# %% Main

def main(counterInfo, datelist):
    """Download new data from Broadway Eco-Totem, clean it, and save.

    Returns:
        newData_df: Dataframe of newly downloaded data
        completeData: Updated complete history data for Broadway
    """
    logger.info('Execute cambridge_totem>main')

    # Download new data
    try:
        newData_df = query_dataset(counterInfo, datelist)

    except Exception as e:
        logger.error('Error updating daily data.',  exc_info=e)
        return None

    return newData_df


# %% Run Script

if __name__ == '__main__':
    # pylint: disable=ungrouped-imports
    import logging.config
    logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')
    logger.debug("Logging is configured.")

    counter_list = json.load(open("codp_counters.json", encoding="utf8"))
    yday = datetime.datetime.now()-datetime.timedelta(days=2)

    data = main(counter_list[0], [yday])

    # if posts is not None:
    #     logger.info(posts)
    #     print(posts)
    #     logger.debug(dailyCounts.tail(10))
    # else:
    #     logger.info('No outputs from Cambridge Open Data Portal')
