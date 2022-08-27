# -*- coding: utf-8 -*-
"""

https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg
https://data.cambridgema.gov/resource/q8v9-mcfg.json

"""

# %% Initialize
# pylint: disable=invalid-name

# Import standard modules
import pickle
# import time
import calendar
import os
import sys
import logging
from datetime import date, timedelta, datetime
from sodapy import Socrata

import pandas as pd
# import matplotlib.pyplot as plt

# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilFuncs as utils
# pylint:enable=import-error, wrong-import-position

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# %% Load Pickled Data


def load_pickled_data():
    """[summary]

    Returns:
        [type]: [description]
    """
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    logger.debug('cwd: %s', currentFolder)
    # print('print: cwd: ', currentFolder)

    if currentFolder == 'dataSources':
        parent = os.path.dirname(path)
        dataFolder = parent + '/data'
    else:
        dataFolder = path + '/data'

    logger.debug('dataFolder: %s', dataFolder)
    logger.debug('dataFolder contents: %s', os.listdir(path=dataFolder))

    broadwayDailyTotals = pd.read_pickle(
        dataFolder + '/broadway_daily_totals.pkl')

    infile = open(dataFolder + '/broadway_records.pkl', 'rb')
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
# def query_precheck(dailyTotals):
#     """[summary]

#     Args:
#         dailyTotals ([type]): [description]
#     """
#     lastDay = dailyTotals['Date'].max()
#     yesterday = date.today() - timedelta(days=1)

#     if (lastDay.date() < yesterday - timedelta(days=2)):
#         # Need to get multiple days worth of data and iterate over each
#         logger.info('Data last saved multiple days ago')
#         query_api(yesterday, lastDay.date())
#     elif (lastDay.date() == yesterday - timedelta(days=1)) & (time.localtime().tm_hour > 13):
#         # Pull yesterday's data
#         logger.info('last data two days ago and it is past 1pm')
#         query_api(yesterday)
#     else:
#         # Don't need to query
#         logger.info('Saved data is current')


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
        # print('print: No downloaded data.')
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


def query_dataset(broadwayDailyTotals=None):
    """[summary]

    Args:
        broadwayDailyTotals ([type]): [description]

    Returns:
        [type]: [description]
    """
    # query_precheck(broadwayDailyTotals)

    # Determine missing data days to download
    if broadwayDailyTotals is not None:
        lastDay = broadwayDailyTotals['Date'].max()
    else:
        lastDay = date.today() - timedelta(days=5)

    startDate = lastDay.date() + timedelta(days=1)
    yesterday = date.today() - timedelta(days=1)
        


    if startDate > yesterday:
        logger.info('Data up to date.')
        # print('print: Data up to date.')
        return

    downloadDatesStr = 'Download data from ' + \
        str(startDate) + ' to ' + str(yesterday)
    logger.info(downloadDatesStr)
    # print('print: ', downloadDatesStr)

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
    yesterday = date.today() - timedelta(days=1)

    # print(lastDay, type(lastDay))

    if lastDay < yesterday:
        firstMissing = lastDay + timedelta(days=1)

        dataUpdateStr = 'NOTE: Data not updated/returned from ' + \
            str(firstMissing) + ' to ' + str(yesterday) + '\n'
        logger.info(dataUpdateStr)
        # print('print: ', dataUpdateStr)


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
        logger.info('With %s ...', countStr)

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
    """_summary_

    Args:
        dailyCounts (_type_): _description_
        records (_type_): _description_
    """
    dailyCounts.to_pickle('data/broadway_daily_totals.pkl', protocol=3)
    # records.to_pickle('data/broadway_records.pkl', protocol=3)
    utils.pickle_dict(records, 'data/broadway_records')

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
    logger.info('Execute cambridge_totem>main')
    # print('print: Execute cambridge_totem>main')

    try:
        broadwayDailyTotals, broadwayRecords = load_pickled_data()
    except Exception as e:
        logger.error('Failed to load pickeled data.',  exc_info=e)
        # print('print: Failed to load pickeled data.\n', e)

    try:
        results_df = query_dataset(broadwayDailyTotals)

    except Exception as e:
        logger.error('Error updating daily data.',  exc_info=e)
        # print('print: Error updating daily data.') 
        return None, None, None, None

    else:
        logger.debug(type(results_df))

        if results_df is not None:
            updateDaily = daily_counts(results_df)

            recordsNew, tweetList = records_compare(
                updateDaily, broadwayRecords)

            save_count_data(updateDaily, recordsNew)

            logger.debug(type(results_df), type(updateDaily),
                          type(recordsNew), type(tweetList))

            return tweetList, results_df, updateDaily, recordsNew
        else:
            return None, None, None, None


# %% Run Script
if __name__ == '__main__':
    # pylint: disable=ungrouped-imports
    import logging.config
    # logging.config.fileConfig(os.path.join( os.getcwd(), '..', 'log.conf'))
    logging.config.fileConfig('log.conf')
    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")

    outputs = main()
    if outputs is not None:
        tweetList, results_df, updateDaily, recordsNew = outputs
        logger.info(tweetList)
    else:
        logger.info('No outputs from Cambridge Totem Data')
