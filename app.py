
# %% Initialize
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from plotly import graph_objs as go

# %% Import Data

path = os.getcwd()
currentFolder = os.path.basename(path)

if currentFolder == 'dataSources':
    parent = os.path.dirname(path)
    dataFolder = parent + '/data'
else:
    dataFolder = path + '/data'

broadwayDailyTotals = pd.read_pickle(
    dataFolder + '/broadway_daily_totals.pkl')

broadwayDailyTotals.index = pd.to_datetime(broadwayDailyTotals.index)

broadwayDailyTotals['Day'] = broadwayDailyTotals['Date'].dt.dayofweek
broadwayDailyTotals['MonthApprev'] = broadwayDailyTotals['Date'].dt.strftime('%b')

broadwayDailyTotals.sort_values(by=['Day', 'Month'], ascending=True, inplace=True)

print(broadwayDailyTotals.columns)

# %% Plotting

# Plot box plots for each month
monthGroups = broadwayDailyTotals.groupby(['Month'])

fig = go.Figure()
for name, group in monthGroups:
    # print(f'{name=}')
    monthname = group['MonthName'][0]
    # print(f'{monthname=}')
    trace = go.Box()
    trace.name = monthname
    trace.y = group['Total']
    fig.add_trace(trace)
fig.update_layout(showlegend=False)
st.plotly_chart(fig)

# Plot box plots for each day of week for each month

weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
weekends = ['Saturday', 'Sunday']

fig = go.Figure()

weekdayTotals = broadwayDailyTotals[broadwayDailyTotals['DayofWeek'].isin(weekdays)]
fig.add_trace(go.Box(
    x = [weekdayTotals['MonthApprev'], weekdayTotals['DayofWeek']],
    y = weekdayTotals['Total'],
    name='Weekdays'
    ))

weekendTotals = broadwayDailyTotals[broadwayDailyTotals['DayofWeek'].isin(weekends)]
fig.add_trace(go.Box(
    x = [weekendTotals['MonthApprev'], weekendTotals['DayofWeek']],
    y = weekendTotals['Total'],
    name='Weekends'
    ))

fig.update_layout(legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.01
))
st.plotly_chart(fig)

# Plot daily percentile (per day of week and month) for each day over time
dayGroups = broadwayDailyTotals.groupby(['DayofWeek','Month'])

broadwayDailyTotals['Percentiles'] = dayGroups['Total'].transform('rank', pct=True)

broadwayDailyTotals.sort_index(ascending=True, inplace=True)

# print(broadwayDailyTotals.head(5))

fig = go.Figure()

fig.add_trace(go.Scatter(
    x = broadwayDailyTotals.index, 
    y = broadwayDailyTotals['Percentiles']*100,
    name = 'Day in Month Percentile',
    mode = 'markers',
    visible = 'legendonly',))
fig.add_trace(go.Scatter(
    x = broadwayDailyTotals.index, 
    y = broadwayDailyTotals['Percentiles'].rolling(28).mean()*100,
    name = '28 Day Rolling<br>Normalized Percentile',
    xhoverformat="%d%b%Y",
    hovertemplate= '%{x}' + '<br>%{y:.2} percentile'
))


fig.update_layout(
    title="Daily ridership normalized to day of week and month",
    xaxis_title="Date",
    yaxis_title="Normalized Ridership Volume",
)


st.plotly_chart(fig)