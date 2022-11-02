'''
Universal Data Analysis

'''

# %% Initialize
import pandas as pd
import numpy as np

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
    