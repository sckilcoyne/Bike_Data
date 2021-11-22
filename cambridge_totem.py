# -*- coding: utf-8 -*-
"""

"""

# %% Initialize

import pandas as pd
from sodapy import Socrata
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
import pickle
import time

yesterday = date.today() - timedelta(days=1)

# %% Load Pickled Data


def load_pickled_data():
    broadwayDailyTotals = pd.read_pickle(r'data\braodway_daily_totals.pkl')

    infile = open(r'data\broadway_records.pkl', 'rb')
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

    if (lastDay.date() < yesterday - 2):
        # Need to get multiple days worth of data and iterate over each
        print('last data multiple days ago')
    elif (lastDay.date() == yesterday - 1) & (time.localtime().tm_hour > 9):
        # Pull yesterday's data
        print('last data two days ago and it is past 9am')
    else:
        # Don't need to query
        print('data is current')


def query_api(endDate, startDate=None):

    if startDate == None:
        startDate = endDate

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
                         where='date=\'' + str(yesterday) + '\'')

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    # Clean up DataFrame
    results_df = results_df.astype(
        {'entries': int, 'exits': int, 'total': int, })  # 'datetime': 'datetime64',

    results_df['time'] = pd.to_datetime(results_df['time'], format=' %H:%M:%S')
    # datetime.strptime(test['time'],'%H:%M:%S').time()

    return results_df


def query_dataset():
    query_precheck(broadwayDailyTotals)
    print()


# %% Records


def records_compare():
    print()

# %% Plot Data


def plot_data(results_df):

    results_df.plot('time', 'total')


# %% Run Script
if __name__ == '__main__':
    broadwayDailyTotals, broadwayRecords = load_pickled_data()

    results_df = query_dataset()

    dailyTotal = results_df['total'].sum()
