'''
Universal Data Analysis

'''

# %% Initialize
# import calendar
import logging
import json
import numpy as np
import pandas as pd
from scipy import stats
from dataSources import cambridge_odp as odp
from dataSources import nmds

# pylint: disable=invalid-name

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)

# %% Data Sets

def daily_counts(newData):
    '''Calculate daily totals from hourly/15-min intervals from the source data
    '''
    dirList = ['Total', 'Northbound', 'Southbound', 'Eastbound', 'Westbound']
    directions = list(set(dirList) & set(list(newData)))
    # print(f'daily_counts: {directions}')

    # Total counted for each day
    # for d in directions:
    # newData[d] = newData[d].astype('float')
    updateDaily = newData.groupby('Date')[directions].sum()#.to_frame()

    updateDaily['Date'] = updateDaily.index.values
    updateDaily = updateDaily.astype({'Date': 'datetime64'})

    updateDaily['Year'] = updateDaily['Date'].dt.year
    updateDaily['Month'] = updateDaily['Date'].dt.month
    updateDaily['MonthName'] = updateDaily['Date'].dt.month_name()
    updateDaily['DayofWeek'] = updateDaily['Date'].dt.day_name()
    updateDaily['MonthApprev'] = updateDaily['Date'].dt.strftime('%b')

    return updateDaily

# %% Format Tweets
def tweet_note(dailyTotals, date):
    '''
    Calculate percentile for every day
    Try for day of week in month
    If not enough data, do weekday/weekend, then seasons
    '''

    total = newData['Total'].sum()

    monthName = newData['MonthName'][0]
    dayofWeek = newData['DayofWeek'][0]

    # Check for most detailed (or best) performance metric (above threshold of occurances)
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekends = ['Sunday', 'Monday']
    if dayofWeek in weekdays:
        weekPart = (weekdays, 'weekdays')
    else:
        weekPart = (weekends, 'weekends')

    months = [['September', 'October', 'November'],
                ['December', 'Janurary', 'February'],
                ['March', 'April', 'May'],
                ['June', 'July', 'August']]
    seasons = pd.DataFrame(np.array(months).T,
                            columns=['fall', 'winter', 'spring', 'summer'])
    season = list(seasons.where(seasons == monthName).dropna(how='all', axis=1))[0]
    seasonMonths = list(seasons[season])

    filters = [([monthName], [dayofWeek], f'{dayofWeek}s in {monthName}'),
                ([monthName], weekPart[0], f'{weekPart[1]} in {monthName}'),
                (seasonMonths, weekPart[0], f'{weekPart[1]} in {season}'),
            ]

    percentileStr = ''
    for f in filters:
        selection = dailyTotals[
            (dailyTotals['MonthName'].isin(f[0])) & 
            (dailyTotals['DayofWeek'].isin(f[1]))]

        if selection.shape[0] > 4:
            percentile = stats.percentileofscore(selection['Total'], total)
            percentileVal = f'{percentile:.0f}'
            logger.info('%s percentile of trips for %s', percentileVal, f[2])

            # Choose the most detailed percentile above posting threshold
            if percentile == 100:
                percentileStr = f'\n\nNew record for {f[2]}!'
                break
            if percentile > 50:
                percentileStr = f'\n\n{percentileVal} percentile of {f[2]}'
                break

    tweetNote = ''
    if percentileStr is not None:
        tweetNote = f'\n\n{percentileStr}'

    return tweetNote

def format_tweet(dailyCounts, stationInfo, date):
    '''
    Write the full tweet for a new day of data
    '''
    stationName = stationInfo[1]

    bikeCount = dailyCounts['Count'].loc[dailyCounts['Date'] == date]

    dateString = pd.to_datetime(date, format=r'%m/%d/%Y').strftime('%a %b %d')
    tweetNote = tweet_note(dailyCounts, date)
    tweet = f'{stationName}\n{bikeCount} riders on {dateString}{tweetNote}'
    return tweet


def new_posts():
    '''
    Create a list of new posts to make
    '''
    tweetList = []
    for d in dateList:
        tweetList.append(format_tweet(dailyCounts, stationInfo, d))

    return tweetList