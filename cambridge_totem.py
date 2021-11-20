# -*- coding: utf-8 -*-
"""

"""

# %% Initialize

import pandas as pd
from sodapy import Socrata
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt

yesterday = date.today() - timedelta(days=5)
print(yesterday)

# %% API Info
# https://data.cambridgema.gov/resource/q8v9-mcfg.json

appToken = ''  # https://dev.socrata.com/docs/app-tokens.html

apiBroadway = 'q8v9-mcfg'
cambData = 'data.cambridgema.gov'

# %% Query Dataset


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
                     where='date=\'' + str(yesterday) + '\'', limit=100)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

# %%
results_df = results_df.astype(
    {'entries': int, 'exits': int, 'total': int, })  # 'datetime': 'datetime64',

results_df['time'] = pd.to_datetime(results_df['time'], format=' %H:%M:%S')
# datetime.strptime(test['time'],'%H:%M:%S').time()

# %% Plot Data

dailyTotal = results_df['total'].sum()

results_df.plot('time', 'total')
