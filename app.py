
# %% Initialize
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from plotly import graph_objs as go

# pylint: disable=invalid-name, pointless-string-statement

# %% Set up streamlit
st.set_page_config(page_title='Boston Bike Data',
                   page_icon=':bike:',)
st.title('[Boston Bike Data](https://twitter.com/BostonBikeData)')

tabFig, tabOther, tabInfo = st.tabs(['Data', 'Others\' Cool Projects', 'Info'])

# %% Import Data

path = os.getcwd()
currentFolder = os.path.basename(path)

if currentFolder == 'dataSources':
    parent = os.path.dirname(path)
    dataFolder = parent + '/data'
else:
    dataFolder = path + '/data'


@st.cache
def importBroadwayData(folder):
    """_summary_

    Args:
        dataFolder (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Broadway Daily Totals
    broadwayDaily = pd.read_pickle(
        folder + '/broadway_daily_totals.pkl')

    broadwayDaily.index = pd.to_datetime(broadwayDaily.index)

    broadwayDaily['Day'] = broadwayDaily['Date'].dt.dayofweek
    broadwayDaily['MonthApprev'] = broadwayDaily['Date'].dt.strftime('%b')

    broadwayDaily.sort_values(by=['Day', 'Month'], ascending=True, inplace=True)

    # print(broadwayDailyTotals.columns)

    dayGroups = broadwayDaily.groupby(['DayofWeek','Month'])
    broadwayDaily['Percentiles'] = dayGroups['Total'].transform('rank', pct=True)
    broadwayDaily['Percentiles100'] = broadwayDaily['Percentiles'] * 100

    broadwayDaily.sort_index(ascending=True, inplace=True)

    # Broadway full dataset
    broadwayComplete = pd.read_pickle(folder + '/broadway_complete.pkl')
    broadwayComplete['Hour'] = broadwayComplete['DateTime'].dt.hour
    print(broadwayComplete.columns)

    # Create Hourly dataset
    broadwayHourly = []
    for name, group in broadwayComplete.groupby(['Date', 'Hour']):
        hourTotal = group['Total'].sum()
        hourWest = group['Westbound'].sum()
        hourEast = group['Eastbound'].sum()
        broadwayHourly.append([group.Date.iloc[0], group.Year.iloc[0], 
                           group.Month.iloc[0], group.Day.iloc[0], 
                           group.Hour.iloc[0], hourTotal, hourWest, hourEast])

    broadwayHourly = pd.DataFrame(broadwayHourly, 
            columns=['Date', 'Year', 'Month', 'Day', 'Hour', 'Total', 'West', 'East'])

    broadwayHourly['day_percentTotal'] = broadwayHourly['Total'] / broadwayHourly.groupby('Date')['Total'].transform('sum')
    broadwayHourly['day_percentWest'] = broadwayHourly['West'] / broadwayHourly.groupby('Date')['West'].transform('sum')
    broadwayHourly['day_percentEast'] = broadwayHourly['East'] / broadwayHourly.groupby('Date')['East'].transform('sum')


    return broadwayDaily, broadwayComplete, broadwayHourly

broadwayDailyTotals, broadwayFull, broadwayHourlyTotals = importBroadwayData(dataFolder)

# %% Plotting
def plot_daily_per(dailyTotals):
    """Plot daily percentile (per day of week and month) for each day over time

    Args:
        dailyTotals (_type_): _description_

    Returns:
        _type_: _description_
    """    
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x = dailyTotals.index, 
        y = dailyTotals['Percentiles100'],
        name = 'Day in Month Percentile',
        mode = 'markers',
        visible = 'legendonly',
        xhoverformat="%d%b%Y",
        hovertemplate= '%{x}' + '<br>%{y:d} percentile'))
    fig.add_trace(go.Scatter(
        x = dailyTotals.index, 
        y = dailyTotals['Percentiles100'].rolling(28).mean(),
        name = '28 Day Rolling<br>Normalized Percentile',
        xhoverformat="%d%b%Y",
        hovertemplate= '%{x}' + '<br>%{y:d} percentile'
    ))

    fig.update_layout(
        title="Daily Ridership Volume<br><sup>Normalized to day of week and month</sup>",
        xaxis_title="Date",
        yaxis_title="Normalized Ridership Volume",
    )

    return fig

def plot_monthly_vol(dailyTotals):
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
        trace.y = group['Total']
        fig.add_trace(trace)
    fig.update_layout(
        title='Monthly Ridership Volume Distibutions',
        xaxis_title='Month',
        yaxis_title='Ridership Volume',
        showlegend=False)

    return fig

def plot_daily_vol(dailyTotals):
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
        y = weekdayTotals['Total'],
        name='Weekdays'
        ))

    weekendTotals = dailyTotals[dailyTotals['DayofWeek'].isin(weekends)]
    fig.add_trace(go.Box(
        x = [weekendTotals['MonthApprev'], weekendTotals['DayofWeek']],
        y = weekendTotals['Total'],
        name='Weekends'
        ))

    fig.update_layout(
        title='Daily Ridership Volume Distibutions',
        xaxis_title='Day of Week',
        yaxis_title='Ridership Volume',
        legend=dict(
            yanchor='top',
            y=0.99,
            xanchor='left',
            x=0.01
    ))

    return fig

def plot_hourly_per(hourlyData, direction='Total'):
    """Plot ridership volume distributions by hour

    Args:
        hourlyData (_type_): _description_

    Returns:
        _type_: _description_
    """    
    fig = go.Figure()

    for name, group in hourlyData.groupby(['Hour']):
        # print(f'{name=}')
        trace = go.Box()
        trace.name = name
        trace.y = group['day_percent' + direction]
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



# %% Data/Fig Tab Layout 
with tabFig:
    st.header('Eco-Totem on Broadway in Cambridge')

    direction = st.radio('Count Direction',('Total', 'West', 'East'),
                            horizontal = True)
    figHourly = plot_hourly_per(broadwayHourlyTotals, direction)
    st.plotly_chart(figHourly)

    figDailyPer = plot_daily_per(broadwayDailyTotals)
    st.plotly_chart(figDailyPer)

    figMonthlyVol = plot_monthly_vol(broadwayDailyTotals)
    st.plotly_chart(figMonthlyVol)

    figDailyVol = plot_daily_vol(broadwayDailyTotals)
    st.plotly_chart(figDailyVol)


# %% Info
with tabInfo:
    '''
    ### Local Bike Orgs to support
    * [Mass Bike](https://www.massbike.org/) [(Twitter)](https://twitter.com/MassBike)
    * [Boston Cyclist Union]() [(Twitter)](https://twitter.com/bostonbikeunion)

    * [Cambridge Bike Safety](https://www.cambridgebikesafety.org/) [(Twitter)](https://twitter.com/cambbikesafety)
    * [Somerville Bike Safety]() [(Twitter)](https://twitter.com/somerbikesafety)
    * [West Rox Bikes](https://www.facebook.com/groups/westroxbikes) [(Twitter)](https://twitter.com/WestRoxBikes)
    * [Soutie Bikes](https://docs.google.com/forms/d/e/1FAIpQLSevN1hQRlAu2pMpcYKpZfY1y7BQqNUVyWzmRgUgL5qnIoVRag/viewform) [(Twitter)](https://twitter.com/SouthieBikes)

    * [Bikes Not Bombs](https://bikesnotbombs.org/) [(Twitter)](https://twitter.com/bikesnotbombs)
    * [Livable Streets](https://www.livablestreets.info/) [(Twitter)](https://twitter.com/StreetsBoston)


    ### Data Sources:
    * [Cambridge Open Data Portal - Eco-Totem Broadway Bicycle Counter](https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg)
    * [MassDOT Non-Motorized Database System](https://mhd.ms2soft.com/TDMS.UI/nmds/dashboard?loc=mhd)
    '''

# %% Cool Projects by others
with tabOther:
    '''
    [@CambridgeCrash](https://twitter.com/CambridgeCrash) - Automatically tweeting reported crashes in Cambridge.

    ### Bluebikes data explorations:    
    [BlueBikes volume per street segment for July 2022](https://crschmidt.net/bluebikes/july2022) by [@crschmidt](https://twitter.com/crschmidt/status/1567888389281665024?s=20&t=1bSy86Pw7Q66Fq9NCTOgNg)
    https://datastudio.google.com/u/0/reporting/650b5e38-07a0-4c95-8f16-f0f9f22b7d98/page/tEnnC
    
    https://www.bostonindicators.org/reports/report-website-pages/covid_indicators-x2/2021/february/biking-brief
    '''
