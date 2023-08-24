'''
Data Analysis and Post generation for all data sources

'''

# %% Initialize
# import calendar
import logging
# import json
import numpy as np
import pandas as pd
from scipy import stats

# pylint: disable=invalid-name

cols_standard = ['StationID', 'StationName', 'Mode', 'DateTime', 'Count']
# StationID: Counter station code
# StationName: Station name used in posts
# Mode: Mode count is for e.g. bike/ped
# DateTime: datetime of count
# Count: Total count for the DateTime of the Mode
#   Counts for specific directions can be extra columns

MODES = ['Bike', 'bike', 'bicycle', 'Bicycle', 'BIKE', 'BICYCLE',
         'Bikes', 'bikes', 'bicycles', 'Bicycles', 'BIKES', 'BICYCLES']

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)

# %% Data Sets


def daily_counts(newData):
    '''Calculate daily totals from hourly/15-min intervals from the source data
    '''
    # Figure out which count data columns exsist
    newDataCols = newData.columns
    extraCols = set(cols_standard) - set(newDataCols)
    directionCols = ['Count'] + list(extraCols)

    df = newData.copy()
    df = df.astype({'DateTime': 'datetime64[ns]'})
    df['Date'] = df['DateTime'].dt.date

    newDaily = df.groupby(['StationID', 'StationName', 'Mode', 'Date'])[
        directionCols].sum().reset_index()

    newDaily = newDaily.rename(columns={"Date": "DateTime"})
    newDaily = newDaily.astype({'DateTime': 'datetime64[ns]'})

    return newDaily

# %% Format Posts


def post_note(dailyTotals, date):
    '''
    Calculate percentile for every day
    Try for day of week in month
    If not enough data, do weekday/weekend, then seasons
    '''

    count = dailyTotals['Count'].loc[(dailyTotals['DateTime'] == date) & (
        (dailyTotals.Mode.isin(MODES)))].sum()

    monthName = date.strftime("%B")
    dayofWeek = date.strftime("%A")

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
        selection = dailyTotals[(dailyTotals.Mode.isin(MODES)) & (
            dailyTotals.DateTime.dt.month_name().isin(f[0])) & (
            dailyTotals.DateTime.dt.day_name().isin(f[1]))]

        if selection.shape[0] > 4:
            percentile = stats.percentileofscore(selection['Count'], count)
            percentileVal = f'{percentile:.0f}'
            logger.info('%s percentile of trips for %s', percentileVal, f[2])

            # Choose the most detailed percentile above posting threshold
            if percentile == 100:
                percentileStr = f'New record for {f[2]}!'
                break
            if percentile > 50:
                percentileStr = f'{percentileVal} percentile of {f[2]}'
                break

    postNote = ''
    if percentileStr is not None:
        postNote = f'\n\n{percentileStr}'

    return postNote


def format_post(dailyCounts, stationInfo, dateinfo):
    '''
    Write the full post for a new day of data
    '''
    stationName = stationInfo[1]
    date = dateinfo['DateTime']

    bikeCount = dateinfo['Count']

    dateString = pd.to_datetime(date, format=r'%m/%d/%Y').strftime('%a %b %d')
    postNote = post_note(dailyCounts, date)
    post = f'{stationName}\n{bikeCount} riders on {dateString}{postNote}'
    return post


def new_posts(postlist, alldf, newdf, stationInfo):
    '''
    Create a list of new posts to make
    '''
    newposts = []
    for _, newdate in newdf.iterrows():
        if newdate.Mode in MODES:
            newposts.append(format_post(alldf, stationInfo, newdate))

    postlist.extend(newposts)

    # postlist = list(postlist)
    return postlist
