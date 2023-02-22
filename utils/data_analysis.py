'''
Universal Data Analysis

(Moving functions used in a dataset to be used in all datasets)
'''

# %% Initialize
import calendar
import logging

import numpy as np
import pandas as pd
from scipy import stats

# pylint: disable=invalid-name

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)

# %% Data Sets

def daily_counts(newData):
    '''Calculate daily totals
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

# %% Data Calculations
def tweet_note(newData, dailyTotals, records=None):
    '''
    Calculate percentile for every day
    Try for day of week in month
    If not enough data, do weekday/weekend, then seasons
    '''

    total = newData['Total'].sum()

    monthName = newData['MonthName'][0]
    dayofWeek = newData['DayofWeek'][0]

    # Check record book
    dailyRecordStr = None
    if records is not None:
        dailyRecord = records['dailyRecord']
        monthlyRecord = records['monthlyRecords'][newData['Month']]

        # Daily record for given month
        if total > monthlyRecord:
            dailyRecordStr = 'New daily record for month of ' + monthName + ' of ' + total + \
                ' riders on ' + newData['Date'] + \
                '! (Previous record for month was ' + monthlyRecord + '.)'
            logger.info(dailyRecordStr)
            records['monthlyRecords'][newData['Month']] = total
        else:
            logger.info('...did not break the daily record for this month')

        # All time daily count record
        if total > dailyRecord:
            dailyRecordStr = 'New all-time daily record of ' + total + ' riders on ' + \
                newData['Date'] + '! (Previous record ' + dailyRecord + '.)'
            logger.info(dailyRecordStr)
            records['dailyRecord'] = total
        else:
            logger.info('...did not break the all-time daily record')
    else:
        logger.info('No record book for counter')

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
    # selection = dailyTotals[
    #     (dailyTotals['MonthName'] == monthName) &
    #     (dailyTotals['DayofWeek'] == dayofWeek)]
        if selection.shape[0] > 4:
            percentile = stats.percentileofscore(selection['Total'], total)
            percentileVal = f'{percentile:.0f}'
            logger.info('%s percentile of trips for %s', percentileVal, f[2])

            # Choose the most detailed percentile above posting threshold
            if percentile > 50:
                percentileStr = f'\n\n{percentileVal} percentile of {f[2]}'
                break

    tweetNote = ''
    if dailyRecordStr is not None:
        tweetNote = f'\n\n{dailyRecordStr}'
    elif percentileStr is not None:
        tweetNote = f'\n\n{percentileStr}'

    return tweetNote

# %% Format Tweets
def format_tweet(stationName, newCount, dailyTotals, records=None):
    '''
    Write the full tweet for a new day of data
    '''
    # countDate =
    bikeCount = newCount['Total'].sum()
    # dateString = pd.to_datetime(countDate, format=r'%m/%d/%Y')
    # dateString = dateString.strftime('%a %b %d')
    dateString = pd.to_datetime(newCount['Date'][0], format=r'%m/%d/%Y').strftime('%a %b %d')
    tweetNote = tweet_note(newCount, dailyTotals, records)
    tweet = f'{stationName}\n{bikeCount} riders on {dateString}{tweetNote}'
    return tweet
    