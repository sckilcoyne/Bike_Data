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

# %% Download Data - NMDS

# postlist = []
# # for c in countersNMDS:
# c = countersNMDS[0]

# # Load full and daily datasets of counter, create empty df if it doesn't exsist
# filename_full = f'data/{c[0]}_full.pkl'
# filename_daily = f'data/{c[0]}_daily.pkl'
# if Path(filename_full).is_file():
#     cdf_full = pd.read_pickle(filename_full)
# else:
#     cdf_full = pd.DataFrame(columns=cols_standard)
#     cdf_full = cdf_full.astype({'DateTime': 'datetime64'})
# if Path(filename_daily).is_file():
#     cdf_daily = pd.read_pickle(filename_daily)
# else:
#     cdf_daily = pd.DataFrame(columns=cols_standard)
#     cdf_daily = cdf_daily.astype({'DateTime': 'datetime64'})

# # Create numpy array of dates with data
# datadates = cdf_full['DateTime'].dt.date
# # datadates = datadates.unique()

# # Create a list of dates in the past week without data

# # datelist = list(nmds.get_dates(c[0]))
# # datelist = [datetime.datetime.strptime(d, "%m/%d/%Y") for d in datelist]
# # datelist = datelist[:7]

# today = datetime.datetime.today().date()
# pastweek = [today - datetime.timedelta(days=x+1) for x in range(7)]
# datelist = set(pastweek) - set(datadates)


# # Download data from missing dates
# newcdf_full = nmds.main(c, datelist)
# if newcdf_full is None:
#     print('No new data downloaded from NMDS')
# else:
#     # Update the daily count dataframe
#     newcdf_daily = da.daily_counts(newcdf_full)
#     print('Calculated daily counts for new data')

#     # Add new data to existing data
#     cdf_full = pd.concat([cdf_full, newcdf_full], ignore_index=True)
#     cdf_daily = pd.concat([cdf_daily, newcdf_daily], ignore_index=True)
#     print('Appened new data to existing data')

#     # Create post list from new data
#     postlist = da.new_posts(postlist, cdf_daily, newcdf_daily, c)
#     print('Created post list')
#     print(postlist)

#     # Save data to file
#     cdf_full.to_pickle(filename_full, protocol=3)
#     cdf_daily.to_pickle(filename_daily, protocol=3)
#     print('Saved data files')

# %% Download Data - CODP

postlist = []
c = countersCODP[0]

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

# datelist = list(nmds.get_dates(c[0]))
# datelist = [datetime.datetime.strptime(d, "%m/%d/%Y") for d in datelist]
# datelist = datelist[:7]

today = datetime.datetime.today().date()
pastweek = [today - datetime.timedelta(days=x+1) for x in range(7)]
datelist = set(pastweek) - set(datadates)


# Download data from missing dates
newcdf_full = codp.main(c, datelist)

if newcdf_full is None:
    print('No new data downloaded')
else:
    # Update the daily count dataframe
    newcdf_daily = da.daily_counts(newcdf_full)
    print('Calculated daily counts for new data')

    # Add new data to existing data
    cdf_full = pd.concat([cdf_full, newcdf_full], ignore_index=True)
    cdf_daily = pd.concat([cdf_daily, newcdf_daily], ignore_index=True)
    print('Appened new data to existing data')

    # Create post list from new data
    postlist = da.new_posts(postlist, cdf_daily, newcdf_daily, c)
    print('Created post list')
    print(postlist)

    # Save data to file
    cdf_full.to_pickle(filename_full, protocol=3)
    cdf_daily.to_pickle(filename_daily, protocol=3)
    print('Saved data files')
