# -*- coding: utf-8 -*-
"""
Download Cambridge Eco-totem data.


Created on Sat Nov 20 12:45:07 2021

https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg

@author: Scott
"""

# %% Initialize
# pylint: disable=invalid-name

import logging
import sys
import os
# import pickle
# import csv
import requests
import pandas as pd

# Custom Modules
# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())
# pylint: disable=import-error, wrong-import-position
import utils.utilFuncs as utils
# pylint: enable=import-error, wrong-import-position

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Set location of csv file
path = os.getcwd()
currentFolder = os.path.basename(path)
logger.debug('cwd: %s', currentFolder)

if currentFolder == 'dataSources':
    parent = os.path.dirname(path)
    dataFolder = parent + '/data'
else:
    dataFolder = path + '/data'

datasetCSVlocation = dataFolder + '/Eco-Totem_Broadway_Bicycle_Count.csv'


# %% Load Full Dataset


def load_dataset(totemdataset):
    """_summary_

    Args:
        dataset (_type_): _description_

    Returns:
        _type_: _description_
    """
    rawData = pd.read_csv(totemdataset)
    completeData = rawData

    # Clean up data
    completeData['DateTime'] = pd.to_datetime(completeData['DateTime'])

    completeData['MonthName'] = completeData['DateTime'].dt.month_name()
    completeData['Month'] = completeData['DateTime'].dt.month
    completeData['Time'] = completeData['DateTime'].dt.time
    completeData['Date'] = completeData['DateTime'].dt.date
    completeData['Year'] = completeData['DateTime'].dt.year
    completeData['MonthApprev'] = completeData['DateTime'].dt.strftime('%b')

    completeData.rename(columns = {'Day': 'DayofWeek'}, inplace=True)

    return rawData, completeData


def download_dataset(filename):
    """Download full dataset of Cambridge Boradway Totem

    Args:
        filename (str): file to save dataset to. Full folder and filename with extension.
    """
    url = 'https://data.cambridgema.gov/api/views/q8v9-mcfg/rows.csv'

    response = requests.get(url)

    with open(filename, 'wb') as file:
        file.write(response.content)


# %% Get Records


def daily_totals(history):
    """Calculate total daily rider volume for each day

    Args:
        history (_type_): _description_

    Returns:
        dailyTotals: _description_
    """
    # Total counted for each day
    dailyTotals = history.groupby('Date')['Total'].sum().to_frame()

    dailyTotals['Date'] = dailyTotals.index.values
    dailyTotals = dailyTotals.astype({'Date': 'datetime64'})

    dailyTotals['Year'] = dailyTotals['Date'].dt.year
    dailyTotals['Month'] = dailyTotals['Date'].dt.month
    dailyTotals['MonthName'] = dailyTotals['Date'].dt.month_name()
    dailyTotals['DayofWeek'] = dailyTotals['Date'].dt.day_name()
    dailyTotals['MonthApprev'] = dailyTotals['Date'].dt.strftime('%b')

    return dailyTotals


def record_daily_month(dailyTotals):
    """Calculate highest daily rider volume for each month

    Args:
        dailyTotals (_type_): _description_

    Returns:
        monthTotals: _description_
    """
    # Highest day for each month
    monthTotals = dailyTotals.groupby('Month')['Total'].max()

    return monthTotals


def create_records(history):
    """Calculate records based on daily totals

    Args:
        history (_type_): _description_

    Returns:
        dailyTotals: _description_
        recordsDict:
    """
    # Create a dict with all records
    dailyTotals = daily_totals(history)

    dailyRecord = dailyTotals['Total'].max()

    monthlyRecords = record_daily_month(dailyTotals)

    recordsDict = {'dailyRecord': dailyRecord,
                   'monthlyRecords': monthlyRecords}

    return dailyTotals, recordsDict


# %% Main


def main():
    """
    Directly update Totem data from Cambridge.

    - Download full dataset
    - Create daily totals for each day
    - Create daily records for each month.
    """

    if input('Download full dataset? (y)  ') == 'y':
        logger.info('Downloading dataset...')
        download_dataset(datasetCSVlocation)
        logger.info('Download complete')

    logger.info('Loading dataset...')
    rawData, completeData = load_dataset(datasetCSVlocation)

    dailyTotals, records = create_records(completeData)

    rawData.to_pickle(dataFolder + '/broadway-raw.pkl', protocol=3)
    completeData.to_pickle(dataFolder + '/broadway-complete.pkl', protocol=3)
    dailyTotals.to_pickle(dataFolder + '/broadway-daily_totals.pkl', protocol=3)
    utils.pickle_dict(records, 'data/broadway-records')

    logger.info('History and records saved')

# %% Run Script


if __name__ == '__main__':

    main()
