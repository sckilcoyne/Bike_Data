'''
https://www.cambridgema.gov/streetsandtransportation/projectsandprograms/gardenstsafetyimprovementproject

https://stackoverflow.com/questions/56017702/how-to-extract-table-from-pdf-in-python
'''
# %% Initialize
# from datetime import datetime, timedelta
import datetime
import numpy as np
import pandas as pd
import glob

# import pypdf
from tabula import read_pdf

# import plotly.graph_objects as go
import matplotlib.pyplot as plt
from matplotlib.path import Path
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
import matplotlib as mpl
from matplotlib.legend_handler import HandlerPathCollection

from scipy import stats

import cartopy.crs as ccrs
from cartopy.io.img_tiles import OSM


# %% Files

folder = 'C:/Users/Scott/Documents/GitHub/Bike_Data/dataSources/garden st/'
ext = '.pdf'

commonHeaders = ['Datetime', 'Location', 'Direction',
                 'Bicycles', 'Cars', 'Total']

# %% Extract Original Data from PDFs


def format1(filename, metaData):
    # Get the number of pages in the file
    # pdf_reader = pypdf.PdfReader(filename)
    # n_pages = len(pdf_reader.pages)

    location = metaData[0]
    print(f'{location=}')
    dates = metaData[1]
    print(f'{dates=}')

    df = pd.DataFrame()

    for page in metaData[3:]:
        print(f'{page=}')
        pageInfo = page.split('_')
        direction = pageInfo[0]
        pageNum = pageInfo[1]

        # For each page the table can be read with the following code
        table_pdf = read_pdf(
            f'{folder}{filename}{ext}',
            guess=True,
            pages=pageNum,
            stream=True,
            encoding="utf-8",
        )

        dfTable = table_pdf[0]
        # Remove bottom rows with summary info
        dropSummary = dfTable.iloc[:, 0].index[dfTable.iloc[:, 0] == 'Total']
        dfTable = dfTable.iloc[0:dropSummary[0], :]

        # Merge top two rows
        dfTable.columns = [str(x)+' '+str(y) for x,
                           y in zip(table_pdf[0].columns[:], dfTable.iloc[0, :])]
        dfTable = dfTable.iloc[1:].reset_index(drop=True)

        # Remove empty columns
        # table_pdf[0] = table_pdf[0].loc[:, ~
        #                                 table_pdf[0].columns.str.startswith('Unnamed')]
        dfTable = dfTable.dropna(axis=1)

        # Rename Time Column
        timeCol = dfTable.columns[0]
        dateData = timeCol[:-4]
        dfTable = dfTable.rename(columns={timeCol: 'Time'})

        # Remove unnamed from columns
        dfTable.columns = dfTable.columns.str.replace(
            r'Unnamed: [0-9]* ', '', regex=True)

        # Add Date Column
        dfTable['Date'] = dateData

        # Add Datetime column
        dfTable['Datetime'] = pd.date_range(
            start=dateData, periods=24, freq='H')

        # Add direction and location
        dfTable['Direction'] = direction
        dfTable['Location'] = location

        df = pd.concat([df, dfTable])

    # Create a simple dataframe with common headers between report types
    dfSimple = df[['Datetime', 'Location', 'Direction',
                   'Bicycles', 'Cars & Trailers', 'Total']].copy()
    dfSimple.columns = commonHeaders

    return dfSimple


def format2(filename, metaData):
    # Get the number of pages in the file
    # pdf_reader = pypdf.PdfReader(filename)
    # n_pages = len(pdf_reader.pages)

    location = metaData[0]
    print(f'{location=}')
    dates = metaData[1].split('_')
    print(f'{dates=}')

    # pageCount = len(metaData[3:])

    df = pd.DataFrame()
    headers = ['Time', 'Bicycles', 'Motorcycle', 'Cars & light Goods',
               'Buses', 'Single Heavy Unit', 'Multi Heavy Unit', 'Total']

    for page in metaData[3:]:
        print(f'{page=}')
        pageInfo = page.split('_')
        direction = pageInfo[0]
        pageNum = pageInfo[1]

        # For each page the table can be read with the following code
        table_pdf = read_pdf(
            f'{folder}{filename}{ext}',
            guess=True,
            pages=pageNum,
            stream=True,
            encoding="utf-8",
            silent=True,
        )

        dfTable = table_pdf[0]

        # Remove top rows with annoying headers
        dfTable = dfTable.iloc[4:]

        # Remove separating column
        # dfTable = dfTable.drop(
        #     ['Unnamed: 3', 'Unnamed: 4', 'Unnamed: 9', 'Unnamed: 13'], axis=1)
        dfTable = dfTable.dropna(axis=1)

        # Move PM table below AM table
        dfAM = dfTable.iloc[:, :8]
        dfPM = dfTable.iloc[:, 8:]
        dfAM.columns = headers
        dfPM.columns = headers
        dfTable = pd.concat([dfAM, dfPM], ignore_index=True)

        # Fix headers
        dfTable.columns = headers

        # Create date then datetime columns
        if len(dates) == 1:
            dfTable['Date'] = dates[0]
        elif len(dates) > 1:
            date = dates[np.mod(
                np.add(int(pageNum), -1), len(dates))]
            print(f'{pageNum=}   |   {date=}')
            dfTable['Date'] = date

        # Add Datetime column
        dfTable['Datetime'] = pd.to_datetime(
            dfTable['Date'] + ' ' + dfTable['Time'], format='%y%m%d %I:%M %p')

        # Add direction and location
        dfTable['Direction'] = direction
        dfTable['Location'] = location

        df = pd.concat([df, dfTable])

    # Create a simple dataframe with common headers between report types
    dfSimple = df[['Datetime', 'Location', 'Direction',
                   'Bicycles', 'Cars & light Goods', 'Total']].copy()
    dfSimple.columns = commonHeaders

    return dfSimple


def collect_data():
    pdfList = glob.glob(f'{folder}*{ext}')
    pdfList = [x[len(folder):-len(ext)] for x in pdfList]

    dfAll = pd.DataFrame()
    for pdf in pdfList:
        metaData = pdf.split('-')
        if metaData[2] == 'T1':
            dfAll = pd.concat([dfAll, format1(pdf, metaData)])
        elif metaData[2] == 'T2':
            dfAll = pd.concat([dfAll, format2(pdf, metaData)])

    # Remove rows without real data
    dfAll = dfAll.replace('*', np.NaN)
    dfAll = dfAll.dropna(axis=0, how='any')

    # Set column types
    dfAll[['Bicycles', 'Cars', 'Total']] = dfAll[[
        'Bicycles', 'Cars', 'Total']].apply(pd.to_numeric)

    dfAll.to_csv(f'{folder}/all_garden_data.csv')
    dfAll.to_pickle(f'{folder}/all_garden_data.pkl')

    return dfAll

# %% Data Analysis


def import_bluebikes():
    dfBB = pd.read_csv(f'{folder}/bluebikes.txt', header=0)
    dfBB['BB_Growth_frac'] = dfBB['Rides'] / dfBB['Rides'][0]
    dfBB['BB_Growth_100'] = dfBB['BB_Growth_frac'] * 100
    dfBB['BB_Growth_0'] = dfBB['BB_Growth_100'] - 100
    dfBB['Date'] = pd.to_datetime(dfBB['Date'])
    dfBB.set_index('Date', drop=False, inplace=True)
    dfBB.index.rename('DateIdx', inplace=True)

    # dfBB['BB_Growth_0'].plot()

    return dfBB


def import_broadway():
    df = pd.read_pickle(f'{folder}/broadway-daily_totals.pkl')

    dfBway = pd.DataFrame(df['Total'][df['Date'] > "2022-10-11"])
    dfBway['Broadway_Growth_frac'] = dfBway['Total'] / dfBway['Total'][0]
    dfBway['Broadway_Growth_100'] = dfBway['Broadway_Growth_frac'] * 100
    dfBway['Broadway_Growth_0'] = dfBway['Broadway_Growth_100'] - 100

    # dfBway.index.names = ['Idx']
    dfBway.reset_index(inplace=True)
    # dfBway['Date'] = dfBway.index
    dfBway['Date'] = pd.to_datetime(dfBway['Date'])
    # dfBway = dfBway.reset_index()
    dfBway.set_index('Date', drop=False, inplace=True)
    dfBway.index.rename('DateIdx', inplace=True)

    return dfBway


def find_rush_hours(df):
    dfHourly = df.groupby([df.Datetime.dt.hour])[[
        'Bicycles', 'Cars', 'Total']].sum()

    dfHourly = dfHourly / dfHourly.sum()

    dfHourly.plot(grid=True)

    print(dfHourly.sort_values('Bicycles', ascending=False).head(4))

    hour1 = dfHourly.sort_values('Bicycles', ascending=False).index[0]
    hour2 = dfHourly.sort_values('Bicycles', ascending=False).index[1]

    dfRush = df[(df.Datetime.dt.hour == hour1) |
                (df.Datetime.dt.hour == hour2)]

    return dfRush


def norm2first(data):
    data['Bike_Growth_frac'] = data['Bicycles'] / data['Bicycles'].iloc[0]
    data['Bike_Growth_100'] = data['Bike_Growth_frac'] * 100
    data['Bike_Growth_0'] = data['Bike_Growth_100'] - 100

    return data


def data_analysis(dfData, dfBB, dfBway):
    # Summary Data

    # Calculate daily totals for each location and mode
    dfDate = dfData.groupby(['Location', dfData.Datetime.dt.date])[
        ['Bicycles', 'Cars', 'Total']].sum()
    dfDate = dfDate.reset_index()
    dfDate['Datetime'] = pd.to_datetime(dfDate['Datetime'])
    dfDate = dfDate.sort_values(by='Datetime').set_index('Datetime', drop=False).rename(columns={'Datetime': 'Date'})
    dfDate.index.rename('DateIdx', inplace=True)

    # Calculate middle day of count groups
    meanCountDate = dfDate.groupby(dfDate.Date.dt.month)['Date'].mean()

    # Calculate count locations per day weighted mean of counts
    dayTotal = dfData.groupby([dfData.Datetime.dt.date])[['Bicycles']].sum()
    countLocs = dfData.groupby([dfData.Datetime.dt.date])[['Location']].nunique()
    dfNormalizedMean = pd.DataFrame()
    dfNormalizedMean['Norm'] = dayTotal['Bicycles'] / countLocs['Location']
    dfNormalizedMean['Norm'] = dfNormalizedMean['Norm'] / \
        dfNormalizedMean['Norm'].iloc[0] * 100 - 100
    dfNormalizedMean['Datetime_col'] = dfNormalizedMean.index
    dfNormalizedMean['Datetime_col'] = pd.to_datetime(dfNormalizedMean['Datetime_col'])
    dfNormalizedMean = pd.concat((meanCountDate,
                                  dfNormalizedMean['Norm'].groupby(dfNormalizedMean.Datetime_col.dt.month).mean()), axis=1)
    dfNormalizedMean.sort_values('Date', inplace=True, ignore_index=True)

    # Calculate mode share
    dfDate['Bike_Car_Share'] = dfDate['Bicycles'] / (dfDate['Cars'] + dfDate['Bicycles']) * 100
    dfDate['Bike_Total_Share'] = dfDate['Bicycles'] / dfDate['Total'] * 100

    # Normalize daily totals to first count day
    dfDate = dfDate.groupby(['Location'], group_keys=False).apply(norm2first)

    # Changes in ridership relative to initial count day
    growthSummary = pd.concat((meanCountDate,
                               dfDate.groupby(dfDate.Date.dt.month)['Bike_Growth_frac'].mean(),
                               dfDate.groupby(dfDate.Date.dt.month)['Bike_Growth_0'].mean(),
                               dfDate.groupby(dfDate.Date.dt.month)['Bike_Growth_100'].mean()),
                              axis=1)
    growthSummary.index.names = ['Idx']

    growthSummary['Quantile_50'] = dfDate.groupby(dfDate.Date.dt.month)[
        'Bike_Growth_0'].quantile(q=0.5)
    growthSummary['Quantile_25'] = dfDate.groupby(dfDate.Date.dt.month)[
        'Bike_Growth_0'].quantile(q=0.25)
    growthSummary['Quantile_75'] = dfDate.groupby(dfDate.Date.dt.month)[
        'Bike_Growth_0'].quantile(q=0.75)
    # growthSummary['growth_geoMean'] = dfDate.groupby(dfDate.Date.dt.month)[
    #     'Bike_Growth_100'].transform(stats.gmean)

    growthSummary.sort_values('Date', inplace=True, ignore_index=True)

    # Bluebikes data
    countdatesBluebikes = dfBB[dfBB['Date'].isin(dfDate['Date'].unique())]

    countdatesBluebikes = pd.concat((meanCountDate,
                                     countdatesBluebikes.groupby(countdatesBluebikes.Date.dt.month)[
                                         'BB_Growth_frac'].mean(),
                                     countdatesBluebikes.groupby(countdatesBluebikes.Date.dt.month)[
                                         'BB_Growth_0'].mean(),
                                     countdatesBluebikes.groupby(countdatesBluebikes.Date.dt.month)['BB_Growth_100'].mean()),
                                    axis=1)
    countdatesBluebikes.index.rename('Idx', inplace=True)
    countdatesBluebikes.sort_values('Date', inplace=True, ignore_index=True)
    growthSummary['Bluebikes_frac'] = countdatesBluebikes['BB_Growth_frac']
    growthSummary['Bluebikes_0'] = countdatesBluebikes['BB_Growth_0']
    growthSummary['Bluebikes_100'] = countdatesBluebikes['BB_Growth_100']

    # Braodway Data
    countdatesBroadway = dfBway[dfBway['Date'].isin(dfDate['Date'].unique())]
    countdatesBroadway = pd.concat((meanCountDate,
                                    countdatesBroadway.groupby(countdatesBroadway.Date.dt.month)[
                                        'Broadway_Growth_frac'].mean(),
                                    countdatesBroadway.groupby(countdatesBroadway.Date.dt.month)[
                                        'Broadway_Growth_0'].mean(),
                                    countdatesBroadway.groupby(countdatesBroadway.Date.dt.month)[
                                        'Broadway_Growth_100'].mean()),
                                   axis=1)
    countdatesBroadway.index.rename('Idx', inplace=True)
    countdatesBroadway.sort_values('Date', inplace=True, ignore_index=True)
    growthSummary['Broadway_frac'] = countdatesBroadway['Broadway_Growth_frac']
    growthSummary['Broadway_0'] = countdatesBroadway['Broadway_Growth_0']
    growthSummary['Broadway_100'] = countdatesBroadway['Broadway_Growth_100']

    # Baseline
    dfBaseline = pd.DataFrame()
    dfBaseline['Baseline_Growth_frac'] = pd.concat([dfBB['BB_Growth_frac'], dfBway['Broadway_Growth_frac']], axis=1).mean(axis=1)
    dfBaseline['Baseline_Growth_100'] = pd.concat([dfBB['BB_Growth_100'], dfBway['Broadway_Growth_100']], axis=1).mean(axis=1)
    dfBaseline['Baseline_Growth_0'] = pd.concat([dfBB['BB_Growth_0'], dfBway['Broadway_Growth_0']], axis=1).mean(axis=1)
    dfBaseline['Baseline_Rolling_0'] = dfBaseline['Baseline_Growth_0'].rolling('28D').mean()
    growthSummary['Baseline_frac'] = growthSummary[['Bluebikes_frac', 'Broadway_frac']].mean(axis=1)
    growthSummary['Baseline_100'] = growthSummary[['Bluebikes_100', 'Broadway_100']].mean(axis=1)
    growthSummary['Baseline_0'] = growthSummary[['Bluebikes_0', 'Broadway_0']].mean(axis=1)

    # Adjusted growths
    growthSummary['Adjusted_Growth_BB_100'] = 100 * growthSummary['Bike_Growth_frac'] / \
        growthSummary['Bluebikes_frac']
    growthSummary['Adjusted_Growth_Bway_100'] = 100 * growthSummary['Bike_Growth_frac'] / \
        growthSummary['Broadway_frac']
    growthSummary['Adjusted_Growth_Baseline_100'] = 100 * growthSummary['Bike_Growth_frac'] / \
        growthSummary['Baseline_frac']
    growthSummary['Adjusted_Growth_BB_0'] = growthSummary['Adjusted_Growth_BB_100'] - 100
    growthSummary['Adjusted_Growth_Bway_0'] = growthSummary['Adjusted_Growth_Bway_100'] - 100
    growthSummary['Adjusted_Growth_Baseline_0'] = growthSummary['Adjusted_Growth_Baseline_100'] - 100

    # growthSummary['Baseline_Adjusted_100'] = 100 * growthSummary['Bike_Growth_frac'] / growthSummary['Baseline_frac']
    # growthSummary['Baseline_Adjusted_0'] = dfNormalizedMean['Baseline_Adjusted_100'] - 100

    # dfDate['Adjusted_Growth'] = (((dfDate['Bike_Growth'] + 100)/100) / ((growthMean['Bluebikes'] + 100)/100) * 100 ) - 100

    # Bike mode share by count group
    shareMean = pd.concat((dfDate.groupby(dfDate.Date.dt.month)['Bike_Car_Share'].mean(
    ), dfDate.groupby(dfDate.Date.dt.month)['Date'].mean()), axis=1)
    shareMean.index.names = ['Idx']
    shareMean.sort_values('Date', inplace=True, ignore_index=True)
    growthSummary['Bike_Car_Share'] = shareMean['Bike_Car_Share']

    # dateMin = mdates.date2num(dfDate.Date.min())
    # dateMax = mdates.date2num(dfDate.Date.max())

    return growthSummary, dfDate, dfNormalizedMean, dfBaseline


# %% Plots
figsize = (15, 9)
dpi = 100

markersize = 15
linewidth = 5
s = 75

def project_marker(ax):
    projectDate = datetime.date(2022,11,17)
    ax.axvline(x=projectDate,
               linewidth=4,
               color='gray')
    
    ax.text(projectDate - datetime.timedelta(days=1), 0.5,
            'Bike lane installation complete',
            # rotation=90,
            horizontalalignment='right',
            verticalalignment='center',
            rotation='vertical',
            transform=ax.get_xaxis_text1_transform(0)[0])

    return ax

def watermark(ax):
    ax.text(0.05, 0.01, '@BostonBikeData | BCU Labs', transform=ax.transAxes)

    return ax

def plot_mode_share(df, dfDate):
    # Plot mode share over time
    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    plt.plot(df.index, df['Bike_Car_Share'],
             marker='o', markersize=markersize,
             markerfacecolor='none', markeredgecolor='purple',
             linestyle='None')
    plt.plot(df.index, df['Bike_Car_Share'].rolling('56D', center=True).mean(),
             color='purple', linewidth=linewidth,
             label='Month Average')

    # # Plot individual count markers
    # for loc in dfDate.groupby('Location'):
    #     ax.scatter(loc[1]['Date'], loc[1]
    #                ['Bike_Car_Share'], label=loc[0], marker='.', s=s)

    # Add line for project completion
    ax = project_marker(ax)

    # Annotate
    shareMax = df['Bike_Car_Share'].max()
    shareInit = df[df['Date'] ==
                   df['Date'].min()]['Bike_Car_Share'].iloc[0]
    sharePoint = shareMax - shareInit
    shareIncrease = shareMax / shareInit + 1
    print(f'Bike Mode Share percentage point increase: {sharePoint:0.1f}')
    print(f'Bike Mode Share increased by {shareIncrease:0.1f} times')

    note1 = 'Since project implementation, bike mode share across'
    note2 = 'all count locations in the surrounding neighborhood'
    note3 = f'has increased by as much as {shareIncrease:.1f} times'
    note4 = f'and {sharePoint:0.1f} percentage points'
    ax.text(
        0.5, 0.95, f'{note1}\n{note2}\n{note3}\n{note4}',
        transform=ax.transAxes, ha='center', va='top',
        bbox=dict(boxstyle="round,pad=0.3",
                  fc="lightblue", ec="steelblue", lw=2)
    )

    # Plot Bluebikes data
    # axTwin = ax.twinx()
    # axTwin.plot(dfBluebikes['Date'], dfBluebikes['BB_Growth_0'],)
    #          # label='Cambridge BlueBikes Rides', color='blue')
    # axTwin.set_ylim(-100,100)

    plt.legend()
    plt.xlabel('Count Date')
    plt.ylabel('Bike Mode Share')
    plt.suptitle(
        'Bike/Car Mode Share on streets near Garden Street Safety Improvement Project')
    # plt.title('Garden St. project implemented in Novemeber 2022')
    ax = watermark(ax)
    plt.show()
    fig.savefig(f'{folder}/bike_modeshare.png', bbox_inches='tight')


def plot_bike_growth(df, dfDate, dfBB, dfBway, dfTotal, dfBaseline):
    # Plot bike growth over time
    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    plt.plot(df.index, df['Bike_Growth_0'],
             marker='o', markersize=markersize,
             markerfacecolor='none', markeredgecolor='red',
             linestyle='None')
    plt.plot(df.index, df['Bike_Growth_0'].rolling('56D', center=True).mean(),
             color='red', linewidth=linewidth,
             label='Month Average')

    # # Plot Bluebikes data
    # plt.plot(dfBB['Date'], dfBB['BB_Growth_0'],
    #          label='Cambridge BlueBikes Rides', color='blue')

    # # Plot Braodway data
    # plt.plot(dfBway['Date'], dfBway['Broadway_Growth_0'],
    #          label='Broadway Cambridge', color='darkorange')
    
    # Plot Baseline data
    plt.plot(dfBaseline.index, dfBaseline['Baseline_Rolling_0'],
             label='Cambridge Bike Ridership Baseline', color='gray')

    # # Plot each day's count data
    # for loc in dfDate.groupby('Location'):
    #     plt.scatter(loc[1]['Date'], loc[1]
    #                 ['Bike_Growth_0'], label=loc[0], marker='.', s=s)
        
    # Add line for project completion
    ax = project_marker(ax)

    # Annotate
    note1 = 'Since project implementation, bike volumes at'
    note2 = 'each count location in the surrounding neighborhood'
    note3 = f'have increased by as much as {df.Bike_Growth_0.max()/100 + 1:.1f} times'
    ax.text(
        0.5, 0.9, f'{note1}\n{note2}\n{note3}',
        transform=ax.transAxes, ha='center', va='top',
        bbox=dict(boxstyle="round,pad=0.3",
                  fc="lightblue", ec="steelblue", lw=2)
    )

    # Plot limits
    dateLims = []
    dateLims.append(dfTotal['Date'].iloc[0] - datetime.timedelta(days=5))
    dateLims.append(dfTotal['Date'].iloc[-1] + datetime.timedelta(days=5))
    plt.xlim(dateLims)

    plt.legend()
    plt.xlabel('Count Date')
    plt.ylabel('Bike Growth')
    # plt.ylim([-100, 850])
    plt.ylim([-100, 650])
    plt.suptitle(
        'Bike volume growth relative to first count on streets near Garden Street Safety Improvement Project')
    # plt.title('Garden St. project implemented in Novemeber 2022')
    ax = watermark(ax)
    plt.show()
    fig.savefig(f'{folder}/bike_growth.png', bbox_inches='tight')


def plot_bike_growth_experimental(df, dfDate, dfBB, dfBway, dfTotal, dfBaseline):
    # Plot bike growth over time
    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    plt.plot(df['Datetime'], df['Bike_Growth_0'],
             color='red', marker='o', markersize=markersize, 
             markerfacecolor='none', markeredgecolor='r',
             linewidth=linewidth,
             label='Month Average')

    # Plot median growth numbers
    plt.plot(df['Datetime'], df['Quantile_50'],
             color='purple', marker='o', markersize=markersize, 
             markerfacecolor='none', markeredgecolor='purple',
             linewidth=linewidth,
             label='50th Percentile for Count Series')
    plt.fill_between(df['Datetime'], df['Quantile_25'],
                     df['Quantile_75'], facecolor='purple', alpha=0.5)

    # # Plot Bluebikes data
    # plt.plot(dfBB['Date'], dfBB['BB_Growth_0'],
    #          label='Cambridge BlueBikes Rides', color='blue')

    # # Plot Braodway data
    # plt.plot(dfBway['Date'], dfBway['Broadway_Growth_0'],
    #          label='Cambridge BlueBikes Rides', color='darkorange')
    
    # Plot Baseline data
    plt.plot(dfBaseline.index, dfBaseline['Baseline_Rolling_0'],
             label='Cambridge Bike Ridership Baseline', color='gray')

    # Plot each day's count data
    for loc in dfDate.groupby('Location'):
        plt.scatter(loc[1]['Datetime'], loc[1]
                    ['Bike_Growth_0'], label=loc[0], marker='.', s=s)

    # Add line for project completion
    ax = project_marker(ax)

    # Annotate
    note1 = 'Since project implementation, bike volumes at each count location'
    note2 = f'have increased on average by as much as {df.Bike_Growth_0.max()/100 + 1:.1f} times'
    ax.text(
        0.5, 0.9, f'{note1}\n{note2}',
        transform=ax.transAxes, ha='center',
        bbox=dict(boxstyle="round,pad=0.3",
                  fc="lightblue", ec="steelblue", lw=2)
    )

    # Plot limits
    dateLims = []
    dateLims.append(dfTotal['Datetime'].iloc[0] - datetime.timedelta(days=5))
    dateLims.append(dfTotal['Datetime'].iloc[-1] + datetime.timedelta(days=5))
    plt.xlim(dateLims)

    plt.legend()
    plt.xlabel('Count Date')
    plt.ylabel('Bike Growth')
    plt.ylim([-100, 850])
    plt.suptitle(
        'Bike volume relative to first count on streets near Garden Street Safety Improvement Project')
    # plt.title('Garden St. project implemented in Novemeber 2022')
    ax = watermark(ax)
    plt.show()
    fig.savefig(f'{folder}/bike_growth_experimental.png', bbox_inches='tight')


def plot_bike_volume(df, dfDate, dfBB, dfBway, dfTotal, dfBaseline):
    # Plot bike volume over time
    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    # plt.plot(dfTotal['Date'], dfTotal['Norm'],
    #          color='g', marker='o', markersize=markersize, 
    #          markerfacecolor='none', markeredgecolor='g',
    #          linewidth=linewidth,
    #          label='Bike Volume Across all Count Locations')
    plt.plot(dfTotal['Date'], dfTotal['Norm'],
             marker='o', markersize=markersize,
             markerfacecolor='none', markeredgecolor='g',
             linestyle='None')
    plt.plot(dfTotal['Date'], dfTotal['Norm'].rolling('56D', center=True).mean(),
             color='g', linewidth=linewidth,
             label='Bike Volume Across all Count Locations')

    # # Plot Bluebikes data
    # plt.plot(dfBB['Date'], dfBB['BB_Growth_0'],
    #          label='Broadway Bike Counter', color='blue')

    # # Plot Braodway data
    # plt.plot(dfBway['Date'], dfBway['Broadway_Growth_0'],
    #          label='Cambridge BlueBikes Rides', color='darkorange')
    
    # Plot Baseline data
    plt.plot(dfBaseline.index, dfBaseline['Baseline_Rolling_0'],
             label='Cambridge Bike Ridership Baseline', color='gray')
    
    # Add line for project completion
    ax = project_marker(ax)

    # Notes on max improvements
    note1 = 'Since project implementation, bike volume across'
    note2 = 'all count locations in the surrounding neighborhood'
    note3 = f'has increased by as much as {dfTotal.Norm.max()/100 + 1:.1f} times'
    ax.text(
        0.01, 0.95, f'{note1}\n{note2}\n{note3}',
        transform=ax.transAxes, ha='left', va='top',
        bbox=dict(boxstyle="round,pad=0.3",
                  fc="lightblue", ec="steelblue", lw=2)
    )

    # Plot limits
    dateLims = []
    dateLims.append(dfTotal['Date'].iloc[0] - datetime.timedelta(days=5))
    dateLims.append(dfTotal['Date'].iloc[-1] + datetime.timedelta(days=5))
    plt.xlim(dateLims)

    plt.legend()
    plt.xlabel('Count Date')
    plt.ylabel('Relative Bike Volumes')
    plt.ylim([-100, 200])
    plt.suptitle(
        'Cumalative bike volume across all count locations on streets near Garden Street Safety Improvement Project')
    # plt.title('Garden St. project implemented in Novemeber 2022')
    ax = watermark(ax)
    plt.show()
    fig.savefig(f'{folder}/bike_volume.png', bbox_inches='tight')


def plot_volume_adjusted(df, dfDate, dfBB, dfBway, dfTotal, dfBaseline):
    # Plot bike growth over time, relative to Bluebikes
    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    # plt.plot(df['Date'], df['Bike_Growth_0'],
    #          color='green', #linestyle='--',
    #          marker='o', markersize=markersize, 
    #          markerfacecolor='none', markeredgecolor='green',
    #          linewidth=linewidth,
    #          label='Month Average')
    plt.plot(df.index, df['Bike_Growth_0'],
             marker='o', markersize=markersize,
             markerfacecolor='none', markeredgecolor='green',
             linestyle='None')
    plt.plot(df.index, df['Bike_Growth_0'].rolling('56D', center=True).mean(),
             color='green', linewidth=linewidth,
             label='Month Average')
    
    # Plot Adjusted Values
    # plt.plot(df['Date'], df['Adjusted_Growth_BB_0'],
    #          color='blue', marker='o', markersize=markersize, 
    #          markerfacecolor='none', markeredgecolor='b',
    #          label='Month Average adjusted for Bluebikes Ridership')

    # plt.plot(df['Date'], df['Adjusted_Growth_Bway_0'],
    #          color='darkorange', marker='o', markersize=markersize, 
    #          markerfacecolor='none', markeredgecolor='darkorange',
    #          label='Month Average adjusted for bike volume on Broadway')

    # plt.plot(df['Date'], df['Adjusted_Growth_Baseline_0'],
    #          color='red', linestyle='--',
    #          marker='o', markersize=markersize, 
    #          markerfacecolor='none', markeredgecolor='red',
    #          label='Month Average adjusted for bike volume in Cambridge')
    plt.plot(df.index, df['Adjusted_Growth_Baseline_0'],
             marker='o', markersize=markersize,
             markerfacecolor='none', markeredgecolor='red',
             linestyle='None')
    plt.plot(df.index, df['Adjusted_Growth_Baseline_0'].rolling('56D', center=True).mean(),
             color='red', linewidth=linewidth,
             label='Month Average adjusted for bike volume in Cambridge')

    # # Plot Bluebikes data
    # plt.plot(dfBB['Date'], dfBB['BB_Growth_0'],
    #          label='Cambridge BlueBikes Rides', color='blue', linestyle='--')

    # # Plot Braodway data
    # plt.plot(dfBway['Date'], dfBway['Broadway_Growth_0'],
    #          label='Broadway, Cambridge', color='darkorange', linestyle='--')
    
    # Plot Baseline data
    plt.plot(dfBaseline.index, dfBaseline['Baseline_Rolling_0'],
             label='Cambridge Bike Ridership Baseline', color='gray')
    
    # Add line for project completion
    ax = project_marker(ax)

    # Notes on max improvements
    noteHeader = 'Since project implementation, bike volume at \neach count location has increased on average by as much as:'
    noteTab = '\n       -'
    noteRaw = f'{noteTab}{df.Bike_Growth_0.max()/100+1:.1f} times counts directly before installation'
    # noteBB = f'{noteTab}{df.Adjusted_Growth_BB_0.max()/100+1:.1f} times relative to Bluebikes useage in Cambridge'
    # noteBway = f'{noteTab}{df.Adjusted_Growth_Bway_0.max()/100+1:.1f} times relative to bike volume on Broadway'
    noteBaseline = f'{noteTab}{df.Adjusted_Growth_Baseline_0.max()/100+1:.1f} times relative to Cambridge baseline bike volume'
    ax.text(
        0.01, 0.8, f'{noteHeader}{noteRaw}{noteBaseline}',
        transform=ax.transAxes, ha='left', va='top',
        bbox=dict(boxstyle="round,pad=0.3",
                  fc="lightblue", ec="steelblue", lw=2)
    )

    # Plot limits
    dateLims = []
    dateLims.append(dfTotal['Date'].iloc[0] - datetime.timedelta(days=5))
    dateLims.append(dfTotal['Date'].iloc[-1] + datetime.timedelta(days=5))
    plt.xlim(dateLims)

    plt.legend(loc=2)
    plt.xlabel('Count Date')
    plt.ylabel('(Relative) Bike Growth')
    plt.ylim([-100, 1100])
    plt.suptitle(
        'Adjusted bike volume relative to first count on streets near Garden Street Safety Improvement Project')
    # plt.title('Garden St. project implemented in Novemeber 2022')
    ax = watermark(ax)
    plt.show()
    fig.savefig(f'{folder}/bike_volume_adjusted.png', bbox_inches='tight')


def plot_bike_growth_map(countLocale, projectPath, dfCount):

    imagery = OSM()

    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = plt.axes(projection=imagery.crs)
    ax.set_extent((-71.134, -71.1215, 42.378, 42.388))  # Left, Right, Bottom, Top

    # Add the imagery to the map.
    ax.add_image(imagery, 17)  # Number is scale, larger gives finer detail

    lat = []
    long = []
    growth = []

    for count in countLocale:
        newGrowth = dfCount['Bike_Growth_100'][dfCount['Location'] == count].max()
        newLat = countLocale[count][0]
        newLong = countLocale[count][1]
        growth.append(newGrowth)
        lat.append(newLat)
        long.append(newLong)

        # print(f'{count}: [{newLat}, {newLong}] = {newGrowth}')

    sc = plt.scatter(long, lat, transform=ccrs.PlateCarree(),
                     marker='o', color='red', s=growth, alpha=0.5,
                     linestyle='', label='Growth relative to initial count')

    plt.plot(projectPath[:, 1], projectPath[:, 0], transform=ccrs.PlateCarree(),
             alpha=0.6, color='gold', linewidth=10)
    
    ax.text(0.1, 0.01, '@BostonBikeData | BCU Labs', transform=ax.transAxes,
            bbox=dict(boxstyle="round,pad=0.3",
                      fc="white", ec="black", lw=1, alpha=0.7))

    plt.legend(loc=1, handler_map={type(sc): HandlerPathCollection(update_func=update_prop)})
    plt.title('Garden St. Improvement Project Area Bike Volume Increase')
    plt.show()
    fig.savefig(f'{folder}/bike_growth_map.png', bbox_inches='tight')


def plot_bike_volume_map(countLocale, projectPath, dfCount):

    imagery = OSM()

    fig = plt.figure(figsize=figsize, dpi=dpi)
    ax = plt.axes(projection=imagery.crs)
    ax.set_extent((-71.134, -71.1215, 42.378, 42.388))  # Left, Right, Bottom, Top

    # Add the imagery to the map.
    ax.add_image(imagery, 17)  # Number is scale, larger gives finer detail

    lat = []
    long = []
    bikeMin = []
    bikeMax = []

    for count in countLocale:
        newMin = dfCount['Bicycles'][dfCount['Location'] == count].iloc[0]
        newMax = dfCount['Bicycles'][dfCount['Location'] == count].max()
        newLat = countLocale[count][0]
        newLong = countLocale[count][1]
        bikeMin.append(newMin)
        bikeMax.append(newMax)
        lat.append(newLat)
        long.append(newLong)

        # print(f'{count}: [{newLat}, {newLong}] = {newGrowth}')

    scale = 10
    bikeMin = [bike * scale for bike in bikeMin]
    bikeMax = [bike * scale for bike in bikeMax]

    sc = plt.scatter(long, lat, transform=ccrs.PlateCarree(),
                     marker='o', color='green', s=bikeMax, alpha=0.5,
                     linestyle='', label='Peak Increase')
    plt.scatter(long, lat, transform=ccrs.PlateCarree(),
                marker='o', color='blue', s=bikeMin, alpha=0.5,
                linestyle='', label='Initial Count')

    plt.plot(projectPath[:, 1], projectPath[:, 0], transform=ccrs.PlateCarree(),
             alpha=0.6, color='gold', linewidth=10, label='Project Extents')
    
    ax.text(0.1, 0.01, '@BostonBikeData | BCU Labs', transform=ax.transAxes,
            bbox=dict(boxstyle="round,pad=0.3",
                      fc="white", ec="black", lw=1, alpha=0.7))

    plt.legend(loc=1, handler_map={type(sc): HandlerPathCollection(update_func=update_prop)})
    plt.title('Garden St. Improvement Project Area Bike Volume')
    plt.show()
    fig.savefig(f'{folder}/bike_raw_volume_map.png', bbox_inches='tight')


def update_prop(handle, orig):
    # https://stackoverflow.com/questions/47115869/how-do-i-change-the-size-of-the-scatter-markers-in-the-legend
    marker_size = 36
    handle.update_from(orig)
    handle.set_sizes([marker_size])

# %% Project Info


countLocations = {'BondSt_GardenSt': [42.380982, -71.126894],
                  'MadisonSt_HollyAve': [42.382005, -71.129418],
                  'NewellSt_UplandRd': [42.385857, -71.128693],
                  'GardenSt_IvySt': [42.385844, -71.132865],
                  'RaymondSt_GrayGardensEast': [42.384869, -71.126051],
                  'LinnaeanSt_GraySt': [42.383680, -71.123213],
                  'ConcordAve_BuckinghamSt': [42.380975, -71.128528],
                  'ShepardSt_WalkerSt': [42.381192, -71.122464],
                  'ChauncySt_GardenSt': [42.379541, -71.122634],
                  'WaldenSt_WoodSt': [42.386967, -71.128794],
                  'WalkerSt_WalkerStPlace': [42.379897, -71.123850],
                  }
projectArea = [[42.383823, -71.129201],
               [42.383351, -71.128158],
               [42.382077, -71.126734],
               [42.380139, -71.125039],
               [42.378332, -71.123687],
               [42.377676, -71.123161],
               ]
projectArea = np.array(projectArea)

# %% Main Script
def main():
    # dfGardenCounts = collect_data()
    dfGardenCounts = pd.read_pickle(f'{folder}/all_garden_data.pkl')

    dfBluebikes = import_bluebikes()
    dfBroadway = import_broadway()

    dfGrowthSummary, dfGrowthCounters, dfSummaryMean, dfBaseline = data_analysis(
        dfGardenCounts, dfBluebikes, dfBroadway)

# %% Make the plots!
    dfGrowthSummary.set_index('Date', inplace=True, drop=False)
    dfGrowthSummary.index.rename('DateIdx')

    dfSummaryMean.set_index('Date', inplace=True, drop=False)
    dfSummaryMean.index.rename('DateIdx')

    plot_mode_share(dfGrowthSummary, dfGrowthCounters)
    plot_bike_growth(dfGrowthSummary, dfGrowthCounters, 
                     dfBluebikes, dfBroadway, 
                     dfSummaryMean, dfBaseline)

    # plot_bike_growth_experimental(dfGrowthSummary, dfGrowthCounters,
    #                               dfBluebikes, dfBroadway, dfSummaryMean)

    plot_bike_volume(dfGrowthSummary, dfGrowthCounters, 
                     dfBluebikes, dfBroadway, 
                     dfSummaryMean, dfBaseline)
    plot_volume_adjusted(dfGrowthSummary, dfGrowthCounters, 
                         dfBluebikes, dfBroadway, 
                         dfSummaryMean, dfBaseline)

    plot_bike_growth_map(countLocations, projectArea, dfGrowthCounters)
    plot_bike_volume_map(countLocations, projectArea, dfGrowthCounters)

if __name__ == '__main__':
    main()
