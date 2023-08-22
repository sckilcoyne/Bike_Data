# %% Initialize
import json
import datetime
from pathlib import Path
import pandas as pd

import dataSources.cambridge_odp as codp
import dataSources.nmds as nmds
import utils.data_analysis as da

cols_standard = ['StationID', 'StationName', 'Mode', 'DateTime', 'Count']

countersNMDS = json.load(open("dataSources/nmds_counters.json", encoding="utf8"))
countersCODP = json.load(open("dataSources/codp_counters.json", encoding="utf8"))

# %% Download Data

postlist = []
# for c in countersNMDS:
c = countersNMDS[3]

# Load full and daily datasets of counter, create empty df if it doesn't exsist
filename_full = f'data/{c[0]}_full.pkl'
filename_daily = f'data/{c[0]}_daily.pkl'
if Path(filename_full).is_file():
    cdf_full = pd.read_pickle(filename_full)
else:
    cdf_full = pd.DataFrame(columns=cols_standard)
    cdf_full = cdf_full.astype({'DateTime': 'datetime64'})
if Path(filename_daily).is_file():
    cdf_daily = pd.read_pickle(filename_daily)
else:
    cdf_daily = pd.DataFrame(columns=cols_standard)
    cdf_daily = cdf_daily.astype({'DateTime': 'datetime64'})

# Create numpy array of dates with data
datadates = cdf_full['DateTime'].dt.date
# datadates = datadates.unique()

# Create a list of dates in the past week without data
datelist = list(nmds.get_dates(c[0]))
datelist = [datetime.datetime.strptime(d, "%m/%d/%Y") for d in datelist]
datelist = datelist[:7]

# Download data from missing dates
newcdf_full = nmds.main(c, datelist)

# Update the daily count dataframe
newcdf_daily = da.daily_counts(newcdf_full)

# Add new data to exsisting data
cdf_full = pd.concat([cdf_full, newcdf_full], ignore_index=True)
cdf_daily = pd.concat([cdf_daily, newcdf_daily], ignore_index=True)

# Create post list from new data
postlist = da.new_posts(postlist, cdf_daily, newcdf_daily, c)

# Save data to file
cdf_full.to_pickle(filename_full, protocol=3)
cdf_daily.to_pickle(filename_daily, protocol=3)
