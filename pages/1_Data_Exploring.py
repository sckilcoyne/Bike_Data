# %% Initialize
import os
import sys
import pickle
import streamlit as st
import pandas as pd
import numpy as np

import plotly.express as px
from plotly import graph_objs as go

from google.oauth2 import service_account
from google.cloud import storage

print('\n---Page 1: Data Exploring---\n')
# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilSt as ust

# pylint: disable=invalid-name, pointless-string-statement

# Get GCS credentials
GCP_SA = 'gcp_service_account'
bucket_name = st.secrets[GCP_SA]['GCS_BUCKET_NAME']
# print(f'{bucket_name=}')

# %% Create Google Cloud API client.
credentials = service_account.Credentials.from_service_account_info(st.secrets[GCP_SA])
client = storage.Client(credentials=credentials)

# %% Set up streamlit
st.set_page_config(page_title='Boston Bike Data',
                   page_icon=':bike:',)
st.title('Boston Bike Data\n[Twitter](https://twitter.com/BostonBikeData) [Mastodon](https://better.boston/@BostonBikeData)')

# tabFig, tabTest = st.tabs(['Data', 'Test'])

# %% Import Data

@st.cache_data(ttl=60*60*24)
def read_pickle(fileName):
    """Read file from Google Cloud Storage

    Args:
        fileName (str): Google Cloud Storage file name and path

    Returns:
        Content loaded from Pickle file
    """
    bucket = client.bucket(bucket_name)
    content = bucket.blob(f'data/{fileName}').download_as_string()
    data = pickle.loads(content)
    return data

@st.cache_data(hash_funcs={pd.DataFrame: lambda _: None}, ttl=60*60*12)
def import_prep_data(fileName):

    print(f'Downloading {fileName}')
    # Import Data
    dailyTotals = read_pickle(f'{fileName}_daily.pkl')
    completeData = read_pickle(f'{fileName}_full.pkl')

    # st.dataframe(dailyTotals.head())
    # st.dataframe(completeData.head())

    # Daily Totals
    dailyTotals.index = pd.to_datetime(dailyTotals.DateTime)

    # Get path directions
    dirList = ['Northbound', 'Southbound', 'Eastbound', 'Westbound']
    print(f'\n{list(completeData)=}')
    directions = list(set(dirList) & set(list(completeData)))
    print(f'\n{directions=}')

    dailyTotals['Month'] = dailyTotals['DateTime'].dt.month
    dailyTotals['MonthName'] = dailyTotals['DateTime'].dt.strftime('%b')
    dailyTotals['DayofWeek'] = dailyTotals['DateTime'].dt.day_name()
    dailyTotals['Year'] = dailyTotals['DateTime'].dt.year

    # st.write(dailyTotals.head(5))
    dailyTotals.sort_values(by=['DayofWeek', 'Month'], ascending=True, inplace=True)

    # print(f'\n\nimport_prep_data: {dailyTotals.columns=}')

    dayGroups = dailyTotals.groupby(['DayofWeek','Month'])
    dailyTotals['PercentilesTotal'] = dayGroups['Count'].transform('rank', pct=True)
    dailyTotals['Percentiles100Total'] = dailyTotals['PercentilesTotal'] * 100
    # for direction in directions:
    #     dailyTotals[f'Percentiles{direction}'] = dayGroups[direction].transform('rank', pct=True)
    #     dailyTotals[f'Percentiles100{direction}'] = dailyTotals[f'Percentiles{direction}'] * 100

    dailyTotals.sort_index(ascending=True, inplace=True)

    # Full dataset
    completeData['Date'] = completeData['DateTime'].dt.date
    completeData['Hour'] = completeData['DateTime'].dt.hour
    completeData['Year'] = completeData['DateTime'].dt.year
    completeData['Month'] = completeData['DateTime'].dt.strftime('%b')
    completeData['DayofWeek'] = completeData['DateTime'].dt.day_name()
    print(f'{completeData.columns=}')

    # Create Hourly dataset
    hourlyTotals = []
    columns=['Date', 'Year', 'Month', 'DayofWeek', 'Hour', 'Count']
    columns.extend(directions)

    for _, group in completeData.groupby(['Date', 'Hour']):
        hourTotal = group['Count'].sum()

        row = [group.Date.iloc[0], group.Year.iloc[0],
                group.Month.iloc[0], group.DayofWeek.iloc[0],
                group.Hour.iloc[0], hourTotal]

        # st.write(directions)
        for direction in directions:
            # st.write(direction)
            hourDir = group[direction].sum()
            row.append(hourDir)
            # print(f'Append {direction}')

        # st.write(row)
        # print(f'{row=}')
        hourlyTotals.append(row)

    # print(f'{columns=}')
    hourlyTotals = pd.DataFrame(hourlyTotals, columns=columns)

    hourlyTotals['day_percentCount'] = hourlyTotals['Count'] / \
                                        hourlyTotals.groupby('Date')['Count'].transform('sum')
    for direction in directions:
        hourlyTotals[f'day_percent{direction}'] = hourlyTotals[direction] / \
                                                    hourlyTotals.groupby('Date')[direction].transform('sum')

    return dailyTotals, hourlyTotals, completeData

# %% Plotting
def plot_daily_per(stationName, dailyTotals, countDirection='Count'):
    """Plot daily percentile (per day of week and month) for each day over time
    """
    fig = go.Figure()
    # plotType = st.radio('Plot Type', ['Normalized', 'Counts'], horizontal = True,)

    fig.add_trace(go.Scatter(
        x = dailyTotals.index,
        y = dailyTotals[f'Percentiles100{countDirection}'],
        name = 'Percentile',
        mode = 'markers',
        visible = 'legendonly',
        xhoverformat="%d%b%Y",
        hovertemplate= '%{x}' + '<br>%{y:d} percentile'))
    fig.add_trace(go.Scatter(
        x = dailyTotals.index,
        y = dailyTotals[f'Percentiles100{countDirection}'].rolling(28, min_periods=21).mean(),
        name = '28 Day Rolling Avg',
        xhoverformat="%d%b%Y",
        hovertemplate= '%{x}' + '<br>%{y:d} percentile',
        connectgaps=False,
    ))

    fig.update_layout(
        title=f'Daily Ridership Volume [{stationName}]<br><sup>Normalized to day of week and month</sup>',
        xaxis=dict(
            title="Date",
            rangeslider=dict(
                visible=True
            ),
            type="date"
        ),
        yaxis_title='Normalized Ridership Volume',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='left',
            x=0.01,
        ),
    )

    return fig

def plot_monthly_vol(stationName, dailyTotals, countDirection='Total'):
    """Plot box plots for each month
    """
    monthGroups = dailyTotals.groupby(['Month'])

    if countDirection == 'Total':
        countDirection = 'Count'

    fig = go.Figure()
    for name, group in monthGroups:
        # print(f'{name=}')
        monthname = group['MonthName'][0]
        # print(f'{monthname=}')
        trace = go.Box()
        trace.name = monthname
        trace.y = group[countDirection]
        fig.add_trace(trace)
    fig.update_layout(
        title=f'Monthly Ridership Volume Distibutions [{stationName}]',
        xaxis_title='Month',
        yaxis_title='Ridership Volume',
        showlegend=False)

    return fig

def plot_daily_vol(stationName, dailyTotals, countDirection='Total'):
    """Plot box plots for each day of week for each month
    """
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekends = ['Saturday', 'Sunday']

    if countDirection == 'Total':
        countDirection = 'Count'

    fig = go.Figure()

    weekdayTotals = dailyTotals[dailyTotals['DayofWeek'].isin(weekdays)]
    fig.add_trace(go.Box(
        x = [weekdayTotals['MonthName'], weekdayTotals['DayofWeek']],
        y = weekdayTotals[countDirection],
        name='Weekdays'
        ))

    weekendTotals = dailyTotals[dailyTotals['DayofWeek'].isin(weekends)]
    fig.add_trace(go.Box(
        x = [weekendTotals['MonthName'], weekendTotals['DayofWeek']],
        y = weekendTotals[countDirection],
        name='Weekends'
        ))

    fig.update_layout(
        title=f'Daily Ridership Volume Distibutions [{stationName}]',
        xaxis_title='Day of Week',
        yaxis_title='Ridership Volume',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='left',
            x=0.01
    ))

    return fig

def plot_hourly_per(stationName, hourlyData, countDirection='Total'):
    """Plot ridership volume distributions by hour
    """

    if countDirection == 'Total':
        countDirection = 'Count'

    fig = go.Figure()
    # print(f'\n\n\nplot_hourly_per: {countDirection=}')
    # print(f'plot_hourly_per: {list(hourlyData)=}')

    for name, group in hourlyData.groupby(['Hour']):
        # print(f'{name=}')
        if isinstance(name, tuple): # Some reason local and hosted streamlit think name is a different type
            name = name[0]
        trace = go.Box()
        trace.name = name
        trace.y = group[f'day_percent{countDirection}']
        fig.add_trace(trace)

    fig.update_layout(
        title=f'Hourly Ridership Volume [{stationName}]',
        xaxis_title='Hour',
        yaxis_title='Daily Ridership Volume Percent',
        showlegend=False,
        yaxis=dict(tickformat=".0%")
    )

    # st.dataframe(hourlyData)
    return fig

def plot_all_daily(dataSources):
    '''Plot all normalized volumes on single view
    '''
    fig = go.Figure()

    stationDaily = pd.DataFrame()

    for counter in dataSources:
        dailyTotals, completeData, hourlyTotals = import_prep_data(dataSources[counter]['FileName'])
        stationDaily[counter] = dailyTotals['Percentiles100Total']
        fig.add_trace(go.Scatter(
            x = dailyTotals.index,
            y = dailyTotals['Percentiles100Total'].rolling(28, min_periods=21).mean(),
            name = counter,
            mode='markers',
            marker_size=2,
            xhoverformat="%d%b%Y",
            hovertemplate= '%{y:d} percentile',
        ))

    stationDaily['mean'] = stationDaily.mean(axis=1)
    # st.dataframe(stationDaily)

    fig.add_trace(go.Scatter(
        x = stationDaily.index,
        y = stationDaily['mean'].rolling(28, min_periods=21).mean(),
        name = 'Mean',
        xhoverformat="%d%b%Y",
        hovertemplate= '%{y:d} percentile',
    ))

    fig.update_layout(
        title='Daily Ridership Volume (28 day rolling average)',
        xaxis=dict(
            title="Date",
            rangeslider=dict(
                visible=True
            ),
            type="date"
        ),
        yaxis_title='Normalized Ridership Volume<br><sup>(by day and month)</sup>',
        hovermode="x unified",
        # legend=dict(
        #     orientation='h',
        #     yanchor='bottom',
        #     y=1.02,
        #     xanchor='left',
        #     x=0.01,
        # ),
    )

    return fig

# %% Data/Fig Layout

def main():
    '''Create GUI for data display

    add map:
        https://docs.streamlit.io/library/api-reference/charts/st.map
        https://plotly.com/python/scattermapbox/
    '''
    dataSources = ust.dataSources

    singles, everything = st.tabs(['Indiviual Counters', 'Compare Counters'])

    with singles:
        dataName = st.selectbox('Data Source', dataSources.keys())
        dataSource = dataSources[dataName]

        dailyTotals, hourlyTotals, completeData = import_prep_data(dataSource['FileName'])
        # dailyTotals = import_prep_data(dataSource['FileName'])

        # print(f'{dataSources=}')

        # direction = st.radio('Count Direction', dataSource['Directions'],
        #                         horizontal = True,)
        direction = 'Total'

        # st.dataframe(dailyTotals)
        # st.write(dailyTotals.head(5))

        figDailyPer = plot_daily_per(dataName, dailyTotals, direction)
        st.plotly_chart(figDailyPer)

        figHourly = plot_hourly_per(dataName, hourlyTotals, direction)
        st.plotly_chart(figHourly)

        figMonthlyVol = plot_monthly_vol(dataName, dailyTotals, direction)
        st.plotly_chart(figMonthlyVol)

        figDailyVol = plot_daily_vol(dataName, dailyTotals, direction)
        st.plotly_chart(figDailyVol)

    with everything:
        st.write('Work in progress')
    #     fig_all_daily = plot_all_daily(dataSources)
    #     st.plotly_chart(fig_all_daily)

# %% Streamlit Script
main()
