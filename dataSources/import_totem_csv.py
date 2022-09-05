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
    history = pd.read_csv(totemdataset)

    # Clean up data
    history['DateTime'] = pd.to_datetime(history['DateTime'])

    history['Month'] = history['DateTime'].dt.month_name()
    history['Time'] = history['DateTime'].dt.time
    history['Date'] = history['DateTime'].dt.date
    history['Year'] = history['DateTime'].dt.year

    return history


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
    history = load_dataset(datasetCSVlocation)

    dailyTotals, records = create_records(history)

    dailyTotals.to_pickle(dataFolder + '/broadway_daily_totals.pkl', protocol=3)
    utils.pickle_dict(records, 'broadway_records')

    logger.info('History and records saved')

# %% Run Script


if __name__ == '__main__':

    main()
