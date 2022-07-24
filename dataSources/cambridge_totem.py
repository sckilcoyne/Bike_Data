# -*- coding: utf-8 -*-
"""

https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg
https://data.cambridgema.gov/resource/q8v9-mcfg.json

"""

# %% Initialize

import pickle
import time
import calendar
import os
import sys
import logging

sys.path.insert(0,os.getcwd())

import pandas as pd
from sodapy import Socrata
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
import utils.utilFuncs as utils

# logFormat = "%(levelname)s %(asctime)s - %(message)s"
# logFormat = "%(message)s"
# logging.basicConfig(stream=sys.stdout,
#                     level=logging.INFO,
#                     format=logFormat)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
consoleStream = logging.StreamHandler(stream=sys.stdout)
# consoleStream.setFormatter(logFormat)
logger.addHandler(consoleStream)

yesterday = date.today() - timedelta(days=1)

# %% Load Pickled Data


def load_pickled_data():
    """[summary]

    Returns:
        [type]: [description]
    """
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    logging.debug('cwd: ' + currentFolder)

    if currentFolder == 'dataSources':
        parent = os.path.dirname(path)
        dataFolder = parent + '\data'
    else:
        dataFolder = path + '\data'

    broadwayDailyTotals = pd.read_pickle(
        dataFolder + r'\braodway_daily_totals.pkl')

    infile = open(dataFolder + r'\broadway_records.pkl', 'rb')
    broadwayRecords = pickle.load(infile)
    # print(records)
    infile.close()

    return broadwayDailyTotals, broadwayRecords


# %% API Info
# https://data.cambridgema.gov/resource/q8v9-mcfg.json

appToken = ''  # https://dev.socrata.com/docs/app-tokens.html

apiBroadway = 'q8v9-mcfg'
cambData = 'data.cambridgema.gov'


# %% Query Dataset
def query_precheck(dailyTotals):
    """[summary]

    Args:
        dailyTotals ([type]): [description]
    """
    lastDay = dailyTotals['Date'].max()

    if (lastDay.date() < yesterday - timedelta(days=2)):
        # Need to get multiple days worth of data and iterate over each
        logger.info('Data last saved multiple days ago')
        query_api(yesterday, lastDay.date())
    elif (lastDay.date() == yesterday - timedelta(days=1)) & (time.localtime().tm_hour > 13):
        # Pull yesterday's data
        logger.info('last data two days ago and it is past 1pm')
        query_api(yesterday)
    else:
        # Don't need to query
        logger.info('Saved data is current')


def query_api(startDate, endDate):
    """[summary]

    Args:
        startDate ([type]): [description]
        endDate ([type]): [description]

    Raises:
        Exception: [description]

    Returns:
        [type]: [description]
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
    results = client.get(apiBroadway, select='*',
                         where=dateString)

    if results == []:
        logger.error('No downloaded data.')
        raise Exception('Data Download Failure')

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    # Clean up DataFrame
    results_df.rename(columns={'datetime': 'DateTime',
                               'date': 'Date',
                               'time': 'Time',
                               'total': 'Total',
                               'entries': 'Westbound',
                               'exits': 'Eastbound'}, inplace=True)

    results_df = results_df.astype({'Westbound': int,
                                    'Eastbound': int,
                                    'Total': int,
                                    'DateTime': 'datetime64'})

    results_df['Time'] = pd.to_datetime(
        results_df['Time'], format='%H:%M:%S', errors='raise')
    results_df['Year'] = results_df['DateTime'].dt.year

    return results_df


def query_dataset(broadwayDailyTotals):
    """[summary]

    Args:
        broadwayDailyTotals ([type]): [description]

    Returns:
        [type]: [description]
    """
    # query_precheck(broadwayDailyTotals)

    # Determine missing data days to download
    lastDay = broadwayDailyTotals['Date'].max()
    startDate = lastDay.date() + timedelta(days=1)

    if startDate > yesterday:
        logging.info('Data up to date.')
        return

    downloadDatesStr = 'Download data from ' + \
        str(startDate) + ' to ' + str(yesterday)
    logger.info(downloadDatesStr)

    # Download data
    results_df = query_api(startDate, yesterday)
    # print(results_df)

    check_missing_data(results_df)

    return results_df


def check_missing_data(results_df):
    """[summary]

    Args:
        results_df ([type]): [description]
    """
    lastDay = datetime.date(datetime.strptime(
        results_df['Date'].max(), '%Y-%m-%dT00:00:00.000'))

    # print(lastDay, type(lastDay))

    if lastDay < yesterday:
        firstMissing = lastDay + timedelta(days=1)

        dataUpdateStr = 'NOTE: Data not updated/returned from ' + \
            str(firstMissing) + ' to ' + str(yesterday) + '\n'
        logger.info(dataUpdateStr)


def daily_counts(results_df):
    """[summary]

    Args:
        results_df ([type]): [description]

    Returns:
        [type]: [description]
    """
    # Total counted for each day
    updateDaily = results_df.groupby('Date')['Total'].sum().to_frame()

    updateDaily['Date'] = updateDaily.index.values
    updateDaily = updateDaily.astype({'Date': 'datetime64'})

    updateDaily['Year'] = updateDaily['Date'].dt.year
    updateDaily['Month'] = updateDaily['Date'].dt.month

    return updateDaily

# %% Records


def records_compare(updateDaily, records):
    """[summary]

    Args:
        updateDaily ([type]): [description]
        records ([type]): [description]

    Returns:
        [type]: [description]
    """
    tweetList = []

    for idx, day in updateDaily.iterrows():
        total = day['Total']
        dailyRecord = records['dailyRecord']
        monthlyRecord = records['monthlyRecords'][day['Month']]

        dateString = day['Date']
        dateString = dateString.strftime('%a %b %d')

        localeStr = 'Broadway in Cambridge (Eco-Totem)\n'

        countStr = str(total) + ' riders on ' + dateString
        logger.info('With ' + countStr + '...')

        dailyRecordStr = None

        # Daily record for given month
        if total > monthlyRecord:
            monthName = str(calendar.month_name[day['Month']])
            dailyRecordStr = 'New daily record for month of ' + monthName + ' of ' + total + \
                ' riders on ' + day['Date'] + \
                '! (Previous record for month was ' + monthlyRecord + '.)'
            logger.info(dailyRecordStr)
            records['monthlyRecords'][day['Month']] = total
        else:
            logger.info('...did not break the daily record for this month')

        # All time daily count record
        if total > dailyRecord:
            dailyRecordStr = 'New all-time daily record of ' + total + ' riders on ' + \
                day['Date'] + '! (Previous record ' + dailyRecord + '.)'
            logger.info(dailyRecordStr)
            records['dailyRecord'] = total
        else:
            logger.info('...did not break the all-time daily record')

        if dailyRecordStr is not None:
            tweetList.append(localeStr + dailyRecordStr)
        else:
            tweetList.append(localeStr + countStr)

    return records, tweetList

# %% Save Data


def save_count_data(dailyCounts, records):
    dailyCounts.to_pickle('data/braodway_daily_totals.pkl', protocol=3)
    # records.to_pickle('data/broadway_records.pkl', protocol=3)
    utils.pickle_dict(records, 'data/broadway_records.pkl')

    logger.info('Saved updated daily counts and records.')


# %% Plot Data


def plot_data(results_df):
    """[summary]

    Args:
        results_df ([type]): [description]
    """
    results_df.plot('time', 'total')

# %% Main


def main():
    """[summary]

    Returns:
        [type]: [description]
    """
    broadwayDailyTotals, broadwayRecords = load_pickled_data()

    try:
        results_df = query_dataset(broadwayDailyTotals)

    except:
        logger.error('Error updating daily data.')

    else:
        logging.debug(type(results_df))

        if results_df is not None:
            updateDaily = daily_counts(results_df)

            recordsNew, tweetList = records_compare(
                updateDaily, broadwayRecords)

            save_count_data(updateDaily, recordsNew)

            logging.debug(type(results_df), type(updateDaily),
                          type(recordsNew), type(tweetList))

            return tweetList, results_df, updateDaily, recordsNew


# %% Run Script
if __name__ == '__main__':
    outputs = main()
    if outputs is not None:
        tweetList, results_df, updateDaily, recordsNew = outputs
        logging.info(tweetList)
    else:
        logging.info('No outputs from Cambridge Totem Data')
