# -*- coding: utf-8 -*-
"""

https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg
https://data.cambridgema.gov/resource/q8v9-mcfg.json

"""

# %% Initialize

import pandas as pd
from sodapy import Socrata
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
import pickle
import time
import calendar
import os
import logging
import sys

logFormat = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format=logFormat)
logger = logging.getLogger()
# streamHandler = logging.StreamHandler(sys.stdout)
# logger.addHandler(streamHandler)

yesterday = date.today() - timedelta(days=1)

# %% Load Pickled Data


def load_pickled_data():
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    if currentFolder == 'utils':
        parent = os.path.dirname(path)
        dataFolder = parent + '\data'
    else:
        dataFolder = currentFolder + '\data'

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
    # query_precheck(broadwayDailyTotals)

    # Determine missing data days to download
    lastDay = broadwayDailyTotals['Date'].max()
    startDate = lastDay.date() + timedelta(days=1)

    downloadDatesStr = 'Download data from ' + \
        str(startDate) + ' to ' + str(yesterday)
    logger.info(downloadDatesStr)

    # Download data
    results_df = query_api(startDate, yesterday)
    # print(results_df)

    check_missing_data(results_df)

    return results_df


def check_missing_data(results_df):
    lastDay = datetime.date(datetime.strptime(
        results_df['Date'].max(), '%Y-%m-%dT00:00:00.000'))

    # print(lastDay, type(lastDay))

    if lastDay < yesterday:
        firstMissing = lastDay + timedelta(days=1)

        dataUpdateStr = 'NOTE: Data not updated from ' + \
            str(firstMissing) + ' to ' + str(yesterday) + '\n'
        logger.info(dataUpdateStr)


def daily_counts(results_df):
    # Total counted for each day
    updateDaily = results_df.groupby('Date')['Total'].sum().to_frame()

    updateDaily['Date'] = updateDaily.index.values
    updateDaily = updateDaily.astype({'Date': 'datetime64'})

    updateDaily['Year'] = updateDaily['Date'].dt.year
    updateDaily['Month'] = updateDaily['Date'].dt.month

    return updateDaily

# %% Records


def records_compare(updateDaily, records):
    for idx, day in updateDaily.iterrows():
        total = day['Total']
        dailyRecord = records['dailyRecord']
        monthlyRecord = records['monthlyRecords'][day['Month']]

        dateString = day['Date']
        dateString = dateString.strftime('%a %b %d')

        countStr = 'With ' + str(total) + ' riders on ' + dateString + '...'
        logger.info(countStr)
        if total > dailyRecord:
            dailyRecordStr = 'New daily record of ' + total + ' on ' + \
                day['Date'] + '! (Previous record ' + dailyRecord + '.)'
            logger.info(dailyRecordStr)
            records['dailyRecord'] = total
        else:
            dailyRecordStr = '...did not break the all-time daily record'
            logger.info(dailyRecordStr)
        if total > monthlyRecord:
            monthName = str(calendar.month_name[day['Month']])
            dailyRecordStr = 'New daily record in ' + monthName + ' of ' + total + \
                ' on ' + day['Date'] + \
                '! (Previous record ' + monthlyRecord + '.)'
            logger.info(dailyRecordStr)
            records['monthlyRecords'][day['Month']] = total
        else:
            recordStr = '...did not break the daily record for this month'
            logger.info(recordStr)
    return records

# %% Plot Data


def plot_data(results_df):

    results_df.plot('time', 'total')

# %% Main


def main():
    broadwayDailyTotals, broadwayRecords = load_pickled_data()

    try:
        results_df = query_dataset(broadwayDailyTotals)

        updateDaily = daily_counts(results_df)

        recordsNew = records_compare(updateDaily, broadwayRecords)

        return results_df, updateDaily, recordsNew

    except:
        logger.error('Error updating daily data.')


# %% Run Script
if __name__ == '__main__':
    main()
