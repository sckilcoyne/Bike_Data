# -*- coding: utf-8 -*-
"""
Created on Sat Nov 20 12:45:07 2021

https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg

@author: Scott
"""

# %% Initialize
import pandas as pd
import pickle
# import csv
import requests

dataset = '..\data\Eco-Totem_Broadway_Bicycle_Count.csv'

# %% Load Full Dataset


def load_dataset(dataset):
    history = pd.read_csv(dataset)

    # Clean up data
    history['DateTime'] = pd.to_datetime(history['DateTime'])

    history['Month'] = history['DateTime'].dt.month_name()
    history['Time'] = history['DateTime'].dt.time
    history['Date'] = history['DateTime'].dt.date
    history['Year'] = history['DateTime'].dt.year

    return history


def download_dataset(dataset):
    url = 'https://data.cambridgema.gov/api/views/q8v9-mcfg/rows.csv'

    response = requests.get(url)

    file = open(dataset, 'wb')
    file.write(response.content)
    file.close()


# %% Get Records


def daily_totals(history):
    # Total counted for each day
    dailyTotals = history.groupby('Date')['Total'].sum().to_frame()

    dailyTotals['Date'] = dailyTotals.index.values
    dailyTotals = dailyTotals.astype({'Date': 'datetime64'})

    dailyTotals['Year'] = dailyTotals['Date'].dt.year
    dailyTotals['Month'] = dailyTotals['Date'].dt.month
    return dailyTotals


def record_daily_month(dailyTotals):
    # Highest day for each month
    monthTotals = dailyTotals.groupby('Month')['Total'].max()

    return monthTotals


def create_records(history):
    # Create a dict with all records
    dailyTotals = daily_totals(history)

    dailyRecord = dailyTotals['Total'].max()

    monthlyRecords = record_daily_month(dailyTotals)

    recordsDict = {'dailyRecord': dailyRecord,
                   'monthlyRecords': monthlyRecords}

    return dailyTotals, recordsDict

# %% Pickle


def pickle_dict(pklDict, file):
    # create a binary pickle file
    f = open('..\data\\' + file + '.pkl', 'wb')

    # write the python object (dict) to pickle file
    pickle.dump(pklDict, f, protocol=3)

    # close file
    f.close()


# %% Run Script

if __name__ == '__main__':

    if input('Download full dataset? (y)  ') == 'y':
        print('Downloading dataset...')
        download_dataset(dataset)
        print('Download complete')

    print('Loading dataset...')
    history = load_dataset(dataset)

    dailyTotals, records = create_records(history)

    dailyTotals.to_pickle('../data/braodway_daily_totals.pkl', protocol=3)
    pickle_dict(records, 'broadway_records')

    print('History and records saved')
