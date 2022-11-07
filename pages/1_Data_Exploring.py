# %% Initialize
import os
import pickle
import streamlit as st
import pandas as pd
import numpy as np

import plotly.express as px
from plotly import graph_objs as go

from google.oauth2 import service_account
from google.cloud import storage

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
st.title('[Boston Bike Data](https://twitter.com/BostonBikeData)')

# tabFig, tabTest = st.tabs(['Data', 'Test'])

# %% Import Data

# Uses st.experimental_memo to only rerun when the query changes or after timeout.
@st.experimental_memo(ttl=60*60*24)
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

@st.cache
def import_prep_data(fileName):
    """_summary_

    Args:
        dataFolder (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Import Data
    dailyTotals = read_pickle(f'{fileName}-daily_totals.pkl')
    completeData = read_pickle(f'{fileName}-complete.pkl')

    # Daily Totals
    dailyTotals.index = pd.to_datetime(dailyTotals.index)

    # Get path directions
    dirList = ['Northbound', 'Southbound', 'Eastbound', 'Westbound']
    # print(f'\n\n\n\n{list(completeData)=}')
    directions = list(set(dirList) & set(list(completeData)))
    # print(f'{directions=}')

    # st.write(dailyTotals.head(5))

    dailyTotals['MonthApprev'] = dailyTotals['Date'].dt.strftime('%b')

    dailyTotals.sort_values(by=['DayofWeek', 'Month'], ascending=True, inplace=True)

    print(f'\n\nimport_prep_data: {dailyTotals.columns=}')

    dayGroups = dailyTotals.groupby(['DayofWeek','Month'])
    dailyTotals['PercentilesTotal'] = dayGroups['Total'].transform('rank', pct=True)
    dailyTotals['Percentiles100Total'] = dailyTotals['PercentilesTotal'] * 100
    for direction in directions:
        dailyTotals[f'Percentiles{direction}'] = dayGroups[direction].transform('rank', pct=True)
        dailyTotals[f'Percentiles100{direction}'] = dailyTotals[f'Percentiles{direction}'] * 100

    dailyTotals.sort_index(ascending=True, inplace=True)

    # Full dataset
    completeData['Hour'] = completeData['DateTime'].dt.hour
    # print(completeData.columns)

    # Create Hourly dataset
    hourlyTotals = []
    columns=['Date', 'Year', 'Month', 'Day', 'Hour', 'Total']
    columns.extend(directions)

    for _, group in completeData.groupby(['Date', 'Hour']):
        hourTotal = group['Total'].sum()

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

    print(f'{columns=}')
    hourlyTotals = pd.DataFrame(hourlyTotals, columns=columns)

    hourlyTotals['day_percentTotal'] = hourlyTotals['Total'] / hourlyTotals.groupby('Date')['Total'].transform('sum')
    for direction in directions:
        hourlyTotals[f'day_percent{direction}'] = hourlyTotals[direction] / hourlyTotals.groupby('Date')[direction].transform('sum')

    return dailyTotals, completeData, hourlyTotals

# %% Plotting
def plot_daily_per(dailyTotals, countDirection='Total'):
    """Plot daily percentile (per day of week and month) for each day over time

    Args:
        dailyTotals (_type_): _description_

    Returns:
        _type_: _description_
    """    
    fig = go.Figure()

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
        y = dailyTotals[f'Percentiles100{countDirection}'].rolling(28).mean(),
        name = '28 Day Rolling Avg',
        xhoverformat="%d%b%Y",
        hovertemplate= '%{x}' + '<br>%{y:d} percentile'
    ))

    fig.update_layout(
        title="Daily Ridership Volume<br><sup>Normalized to day of week and month</sup>",
        xaxis=dict(
            title="Date",
            # rangeselector=dict(
            #     buttons=list([
            #         dict(count=1,
            #             label="1m",
            #             step="month",
            #             stepmode="backward"),
            #         dict(count=6,
            #             label="6m",
            #             step="month",
            #             stepmode="backward"),
            #         dict(count=1,
            #             label="YTD",
            #             step="year",
            #             stepmode="todate"),
            #         dict(count=1,
            #             label="1y",
            #             step="year",
            #             stepmode="backward"),
            #         dict(step="all")
            #     ])
            # ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        ),
        yaxis_title="Normalized Ridership Volume",
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='left',
            x=0.01,
        ),
    )

    return fig

def plot_monthly_vol(dailyTotals, countDirection='Total'):
    """Plot box plots for each month

    Args:
        dailyTotals (_type_): _description_
    """    
    monthGroups = dailyTotals.groupby(['Month'])

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
        title='Monthly Ridership Volume Distibutions',
        xaxis_title='Month',
        yaxis_title='Ridership Volume',
        showlegend=False)

    return fig

def plot_daily_vol(dailyTotals, countDirection='Total'):
    """Plot box plots for each day of week for each month

    Args:
        dailyTotals (_type_): _description_
    """
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekends = ['Saturday', 'Sunday']

    fig = go.Figure()

    weekdayTotals = dailyTotals[dailyTotals['DayofWeek'].isin(weekdays)]
    fig.add_trace(go.Box(
        x = [weekdayTotals['MonthApprev'], weekdayTotals['DayofWeek']],
        y = weekdayTotals[countDirection],
        name='Weekdays'
        ))

    weekendTotals = dailyTotals[dailyTotals['DayofWeek'].isin(weekends)]
    fig.add_trace(go.Box(
        x = [weekendTotals['MonthApprev'], weekendTotals['DayofWeek']],
        y = weekendTotals[countDirection],
        name='Weekends'
        ))

    fig.update_layout(
        title='Daily Ridership Volume Distibutions',
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

def plot_hourly_per(hourlyData, countDirection='Total'):
    """Plot ridership volume distributions by hour

    Args:
        hourlyData (_type_): _description_

    Returns:
        _type_: _description_
    """    
    fig = go.Figure()
    # print(f'\n\n\nplot_hourly_per: {countDirection=}')
    # print(f'plot_hourly_per: {list(hourlyData)=}')

    for name, group in hourlyData.groupby(['Hour']):
        # print(f'{name=}')
        trace = go.Box()
        trace.name = name
        trace.y = group[f'day_percent{countDirection}']
        fig.add_trace(trace)

    fig.update_layout(
        title='Hourly Ridership Volume',
        xaxis_title='Hour',
        yaxis_title='Daily Ridership Volume Percent',
        showlegend=False,
        yaxis=dict(tickformat=".0%")
    )

    # st.dataframe(hourlyData)
    return fig

# %% Data/Fig Layout

def main():
    '''Create GUI for data display
    '''
    # add map:
    # https://docs.streamlit.io/library/api-reference/charts/st.map
    # https://plotly.com/python/scattermapbox/
    dataSources =  {'Broadway - Cambridge': {
                        'FileName': 'broadway',
                        'Directions': ['Total', 'Westbound', 'Eastbound'],
                        },
                    'Minuteman - Arlington': {
                        'FileName': '4005',
                        'Directions': ['Total', 'Northbound', 'Southbound'],
                        },
                    'Minuteman - Lexington':{
                        'FileName': '4001',
                        'Directions': ['Total', 'Northbound', 'Southbound'],
                        },
                    'Northen Strand - Malden':{
                        'FileName': '4006',
                        'Directions': ['Total', 'Westbound', 'Eastbound'],
                        },
                    'Fellsway NB - Medford':{
                        'FileName': '4004_NB',
                        'Directions': ['Total'], # Only show total for 1 direction
                        },
                    'Fellsway SB - Medford':{
                        'FileName': '4004_SB',
                        'Directions': ['Total'], # Only show total for 1 direction
                        },
                    }

    dataSource = st.selectbox('Data Source',dataSources.keys())
    dataSource = dataSources[dataSource]

    dailyTotals, completeData, hourlyTotals = import_prep_data(dataSource['FileName'])

    direction = st.radio('Count Direction', dataSource['Directions'],
                            horizontal = True,)
    
    # st.dataframe(dailyTotals)
    # st.write(dailyTotals.head(5))

    figDailyPer = plot_daily_per(dailyTotals, direction)
    st.plotly_chart(figDailyPer)

    figHourly = plot_hourly_per(hourlyTotals, direction)
    st.plotly_chart(figHourly)

    figMonthlyVol = plot_monthly_vol(dailyTotals, direction)
    st.plotly_chart(figMonthlyVol)

    figDailyVol = plot_daily_vol(dailyTotals, direction)
    st.plotly_chart(figDailyVol)

# %% Streamlit Script
main()
