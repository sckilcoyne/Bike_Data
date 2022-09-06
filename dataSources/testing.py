import os
import pickle
import pandas as pd
from scipy import stats

path = os.getcwd()
currentFolder = os.path.basename(path)
print(currentFolder)

if currentFolder == 'dataSources':
    parent = os.path.dirname(path)
    dataFolder = parent + '/data'
else:
    dataFolder = path + '/data'

# infile = open(dataFolder + '/broadway_records.pkl', 'rb')
# broadwayRecords = pickle.load(infile)
# # print(records)
# infile.close()

# print(broadwayRecords)

broadwayDailyTotals = pd.read_pickle(
        dataFolder + '/broadway_daily_totals.pkl')

print(broadwayDailyTotals.tail(10))

broadwayDailyTotals = broadwayDailyTotals.iloc[:-5]

broadwayDailyTotals.to_pickle(
        dataFolder + '/broadway_daily_totals.pkl')


month = 'September'
day = 'Monday'

selection = broadwayDailyTotals[
    (broadwayDailyTotals['MonthName'] == month) &
    (broadwayDailyTotals['DayofWeek'] == day)]

# print(selection)
# print(selection['Total'])

print(stats.percentileofscore(selection['Total'], 841))

# grouped = broadwayDailyTotals.groupby(['MonthName', 'DayofWeek'])

# # print(grouped.groups.keys())
# for group in grouped:
#     print(group)
#     print()

# # print(groups['(\'April\', \'Friday\')'])

