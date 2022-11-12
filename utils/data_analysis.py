'''
Universal Data Analysis

(Moving functions used in a dataset to be used in all datasets)
'''

# %% Initialize
import pandas as pd
import numpy as np

# pylint: disable=invalid-name

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
def daily_percentile():
    '''
    Calculate percentile for every day
    Try for day of week in month
    If not enough data, do weekday/weekend, then seasons
    '''
    
    return None

# %% Data Comparisons
def tweet_note():
    '''
    Check if notable record achieved (w/min comparisons)
    Check if high percentile
    '''
    tweetNote = f'\n\n'
    return tweetNote

# %% Format Tweets
def format_tweet(stationName, date, data):
    '''
    Write the full tweet for a new day of data
    '''
    bikeCount = ''
    dateString = pd.to_datetime(date, format=r'%m/%d/%Y')
    dateString = dateString.strftime('%a %b %d')
    tweetNote = tweet_note()
    tweet = f'{stationName}\n{bikeCount} riders on {dateString}{tweetNote}'
    return tweet
    