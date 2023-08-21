import json
import pandas as pd
import numpy as np
import datetime

# %% Import json files
counters_nmds = json.load(open("dataSources/nmds_counters.json", encoding="utf8"))

counters_codp = json.load(open("dataSources/codp_counters.json", encoding="utf8"))

bot_settings = json.load(open("bot_settings.json", encoding="utf8"))

print(counters_nmds[0])
print(counters_codp[0])
# print(bot_settings[0])

# %% Load dataframe

df = pd.read_pickle('data/4001-complete.pkl')

# %% Data testing

dt = df['DateTime'].dt.date

dtu = dt.unique()

# %%

today = datetime.datetime.today().date()

date_list = [today - datetime.timedelta(days=x+1) for x in range(7)]


testlist = [today - datetime.timedelta(days=x+3) for x in range(14)]
