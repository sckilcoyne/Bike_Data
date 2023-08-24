# %% Initialize
import json
import datetime
from pathlib import Path
import pandas as pd
import numpy as np

import dataSources.cambridge_odp as codp
import dataSources.nmds as nmds
import utils.data_analysis as da

cols_standard = ['StationID', 'StationName', 'Mode', 'DateTime', 'Count']

countersNMDS = json.load(open("dataSources/nmds_test.json", encoding="utf8"))
countersCODP = json.load(open("dataSources/codp_counters.json", encoding="utf8"))

# %% Download Data - NMDS

# for c in countersNMDS:
c = countersNMDS[0]

# Load full and daily datasets of counter, create empty df if it doesn't exsist
filename_full = f'data/{c[0]}_full.pkl'
filename_daily = f'data/{c[0]}_daily.pkl'

cdf_full = pd.DataFrame(columns=cols_standard)
cdf_full = cdf_full.astype({'DateTime': 'datetime64'})

cdf_daily = pd.DataFrame(columns=cols_standard)
cdf_daily = cdf_daily.astype({'DateTime': 'datetime64'})

# Create numpy array of dates with data
datadates = nmds.get_dates(c[0])

datelist = set(sorted(set(datadates))[:-3])
datelist = set([datetime.datetime.strptime(d, "%m/%d/%Y").date() for d in datelist])

# Download data from missing dates
newcdf_full = nmds.main(c, datelist)
if newcdf_full is None:
    print('No new data downloaded from NMDS')
else:
    # Update the daily count dataframe
    newcdf_daily = da.daily_counts(newcdf_full)
    print('Calculated daily counts for new data')

    # Add new data to existing data
    cdf_full = pd.concat([cdf_full, newcdf_full], ignore_index=True)
    cdf_daily = pd.concat([cdf_daily, newcdf_daily], ignore_index=True)
    print('Appened new data to existing data')

    # Save data to file
    cdf_full.to_pickle(filename_full, protocol=3)
    cdf_daily.to_pickle(filename_daily, protocol=3)
    print('Saved data files')

# %% Download Data - CODP
# Can only download 1000 entries at a time using API, just download csv from site instead

# c = countersCODP[0]

# filename_full = f'data/{c[0]}_full.pkl'
# filename_daily = f'data/{c[0]}_daily.pkl'

# cdf_full = pd.read_csv('data/Eco-Totem_Broadway_Bicycle_Count.csv')

# cdf_full['StationID'] = 'q8v9-mcfg'
# cdf_full['StationName'] = 'Broadway in #CambMA (Eco-Totem)'
# cdf_full['Mode'] = 'Bike'
# cdf_full['Count'] = cdf_full['Total']

# cdf_full = cdf_full.drop(columns=['Day', 'Date', 'Time', 'Total'])

# cdf_full = cdf_full.astype({'Westbound': int,
#                             'Eastbound': int,
#                             'Count': int,
#                             'DateTime': 'datetime64[ns]'})

# cdf_full = cdf_full[cols_standard + ['Westbound', 'Eastbound']]


# # Update the daily count dataframe
# cdf_daily = da.daily_counts(cdf_full)
# print('Calculated daily counts for new data')

# # Save data to file
# cdf_full.to_pickle(filename_full, protocol=3)
# cdf_daily.to_pickle(filename_daily, protocol=3)
# print('Saved data files')
