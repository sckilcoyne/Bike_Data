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
import pickle
# import time
import calendar
import os
import sys
import logging
from datetime import date, timedelta, datetime

from sodapy import Socrata

from scipy import stats
import pandas as pd

# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilFuncs as utils
import utils.data_analysis as da
# pylint:enable=import-error, wrong-import-position

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

apiBroadway = 'q8v9-mcfg'
cambData = 'data.cambridgema.gov'


# %% Query Dataset

def query_api(startDate, endDate):
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
    queryData = client.get(apiBroadway, select='*',
                         where=dateString)

    if queryData == []:
        logger.error('No downloaded data.')
        # print('print: No downloaded data.')
        raise Exception('Data Download Failure')

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


def query_dataset(broadwayDailyTotals=None):
    """Determine days not already downloaded from Socrata and download them.

    Args:
        broadwayDailyTotals ([type]): [description]

    Returns:
        pandas dataframe: New data from Socrata
    """
    # Determine missing data days to download
    if broadwayDailyTotals is not None:
        lastDay = broadwayDailyTotals['Date'].max()
    else:
        lastDay = date.today() - timedelta(days=5)

    startDate = lastDay.date() + timedelta(days=1)
    yesterday = date.today() - timedelta(days=1)

    # Download data if not up to date
    if startDate > yesterday:
        logger.info('Data up to date.')
        return None
    else:
        downloadDatesStr = 'Download data from ' + \
            str(startDate) + ' to ' + str(yesterday)
        logger.info(downloadDatesStr)

        # Download data
        downloadedData_df = query_api(startDate, yesterday)

        check_missing_data(downloadedData_df)

        return downloadedData_df


def check_missing_data(downloadedData_df):
    """[summary]

    Args:
        downloadedData_df (pandas dataframe): [description]
    """
    lastDay = datetime.date(datetime.strptime(
        downloadedData_df['Date'].max(), '%Y-%m-%dT00:00:00.000'))
    yesterday = date.today() - timedelta(days=1)

    # print(lastDay, type(lastDay))

    if lastDay < yesterday:
        firstMissing = lastDay + timedelta(days=1)

        dataUpdateStr = 'NOTE: Data not updated/returned from ' + \
            str(firstMissing) + ' to ' + str(yesterday) + '\n'
        logger.info(dataUpdateStr)


# def daily_counts(newData):
#     """[summary]

#     Args:
#         results_df ([type]): [description]

#     Returns:
#         [type]: [description]
#     """
#     # Total counted for each day
#     updateDaily = newData.groupby('Date')['Total'].sum().to_frame()

#     updateDaily['Date'] = updateDaily.index.values
#     updateDaily = updateDaily.astype({'Date': 'datetime64'})

#     updateDaily['Year'] = updateDaily['Date'].dt.year
#     updateDaily['Month'] = updateDaily['Date'].dt.month
#     updateDaily['MonthName'] = updateDaily['Date'].dt.month_name()
#     updateDaily['DayofWeek'] = updateDaily['Date'].dt.day_name()
#     updateDaily['MonthApprev'] = updateDaily['Date'].dt.strftime('%b')

#     return updateDaily

def standardize_df(newData):
    '''Clean up scraped data into consistent format for easier working
    '''
    stdData = newData

    # Clean up data
    stdData['DateTime'] = pd.to_datetime(stdData['DateTime'])

    stdData['MonthName'] = stdData['DateTime'].dt.month_name()
    stdData['Month'] = stdData['DateTime'].dt.month
    stdData['Time'] = stdData['DateTime'].dt.time
    stdData['Date'] = stdData['DateTime'].dt.date
    stdData['Year'] = stdData['DateTime'].dt.year
    stdData['MonthApprev'] = stdData['DateTime'].dt.strftime('%b')

    stdData.rename(columns = {'Day': 'DayofWeek'}, inplace=True)

    return stdData


# %% Main


def main():
    """Download new data from Broadway Eco-Totem, clean it, tweet and save.

    Returns:
        tweetList: List of tweets generated from new data
        newData_df: Dataframe of newly downloaded data
        newDailyCounts: Daily counts from new data
        recordsNew: Updated records for Broadway
        completeData: Updated complete history data for Broadway
    """
    logger.info('Execute cambridge_totem>main')

    # Load saved data
    try:
        dailyTotals, completeData, rawData = load_count_data()
    except Exception as e:
        logger.error('Failed to load pickeled data.',  exc_info=e)

    # Download new data
    try:
        newData_df = query_dataset(dailyTotals)

    except Exception as e:
        logger.error('Error updating daily data.',  exc_info=e)
        return None, None, None, None, None

    # Analyze new data
    else:
        logger.debug(type(newData_df))

        if newData_df is not None:
            # Update Raw Data
            rawData = pd.concat([rawData, newData_df])
            # Clean up new raw data
            cleanData = standardize_df(newData_df)
            # Update Complete Data
            completeData = pd.concat([completeData, cleanData])
            # Calculate daily totals
            newDailyCounts = da.daily_counts(cleanData)
            # Update daily totals data
            dailyTotals = pd.concat([dailyTotals, newDailyCounts])

            _, tweetList = format_tweet(dailyTotals,
                newDailyCounts)

            save_count_data(dailyTotals, completeData, rawData)

            logger.debug(type(newData_df), type(newDailyCounts),
                          type(tweetList))

            return tweetList, newData_df, newDailyCounts, completeData
        else:
            return None, None, None, None, None


# %% Run Script

if __name__ == '__main__':
    # pylint: disable=ungrouped-imports
    import logging.config
    logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')
    logger.debug("Logging is configured.")

    tweets, _, dailyCounts, _, _ = main()
    if tweets is not None:
        logger.info(tweets)
        print(tweets)
        logger.debug(dailyCounts.tail(10))
    else:
        logger.info('No outputs from Cambridge Totem Data')
