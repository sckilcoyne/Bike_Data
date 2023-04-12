'''
https://www.cambridgema.gov/streetsandtransportation/projectsandprograms/gardenstsafetyimprovementproject#9fe1189863574e38bc5fba895244666c

https://stackoverflow.com/questions/56017702/how-to-extract-table-from-pdf-in-python
'''
# %% Initialize
# import pypdf
from tabula import read_pdf
import numpy as np
import pandas as pd
import glob
# import plotly.graph_objects as go
import matplotlib.pyplot as plt
from scipy import stats
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
# import matplotlib as mpl

# %% Files

folder = 'C:/Users/Scott/Documents/GitHub/Bike_Data/dataSources/garden st/'
ext = '.pdf'

commonHeaders = ['Datetime', 'Location', 'Direction',
                 'Bicycles', 'Cars', 'Total']

# %% Open Original Data


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

    dfAll.to_csv('all_garden_data.csv')
    dfAll.to_pickle('all_garden_data.pkl')

    return dfAll


def import_bluebikes():
    dfBB = pd.read_csv('bluebikes.txt', header=0)
    dfBB['BB_Growth_100'] = 100 * dfBB['Rides'] / dfBB['Rides'][0]
    dfBB['BB_Growth_0'] = dfBB['BB_Growth_100'] - 100
    dfBB['Date'] = pd.to_datetime(dfBB['Date'])

    # dfBB['BB_Growth_0'].plot()

    return dfBB


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
    # firstdate = df.Datetime[0]
    # firstDateBike = df.Bicycles[0]

    data['Bike_Growth_100'] = data['Bicycles'] / data['Bicycles'].iloc[0] * 100
    # data['Bike_Growth_0'] = data['Bicycles'] / data['Bicycles'].iloc[0] * 100 - 100
    data['Bike_Growth_0'] = data['Bike_Growth_100'] - 100

    return data


def data_analysis(dfData, dfBB):
    # Summary Data

    dfDate = dfData.groupby(['Location', dfData.Datetime.dt.date])[[
        'Bicycles', 'Cars', 'Total']].sum()
    # dfDate = dfDate.reset_index(level=1)
    dfDate = dfDate.reset_index()
    dfDate['Datetime'] = pd.to_datetime(dfDate['Datetime'])

    dfDate['Bike_Car_Share'] = dfDate['Bicycles'] / dfDate['Cars'] * 100
    dfDate['Bike_Total_Share'] = dfDate['Bicycles'] / dfDate['Cars'] * 100
    # dfDate['Bike_Percentile'] = dfDate['Bicycles'] / dfDate['Bicycles'].min()

    dfDate = dfDate.sort_values(by='Datetime')

    dfDate = dfDate.groupby(['Location'], group_keys=False).apply(norm2first)

    meanCountDate = dfDate.groupby(dfDate.Datetime.dt.month)['Datetime'].mean()

    # Changes in ridership relative to initial count day
    growthMean = pd.concat((dfDate.groupby(dfDate.Datetime.dt.month)['Bike_Growth_0'].mean(
    ), meanCountDate), axis=1)
    growthMean.index.names = ['Idx']

    growthMean['growth_median'] = dfDate.groupby(dfDate.Datetime.dt.month)[
        'Bike_Growth_0'].median()
    growthMean['growth_geometricMean'] = dfDate.groupby(dfDate.Datetime.dt.month)[
        'Bike_Growth_100'].transform(stats.gmean)

    growthMean.sort_values('Datetime', inplace=True, ignore_index=True)

    countdatesBluebikes = dfBB[dfBB['Date'].isin(dfDate['Datetime'].unique())]
    countdatesBluebikes = countdatesBluebikes.groupby(
        countdatesBluebikes.Date.dt.month)['BB_Growth_0'].mean()
    countdatesBluebikes = pd.concat(
        (countdatesBluebikes, meanCountDate), axis=1)
    countdatesBluebikes.sort_values(
        'Datetime', inplace=True, ignore_index=True)

    growthMean['Bluebikes_0'] = countdatesBluebikes['BB_Growth_0']
    growthMean['Adjusted_Growth'] = (
        ((growthMean['Bike_Growth_0'] + 100)/100) / ((growthMean['Bluebikes_0'] + 100)/100) * 100) - 100

    # dfDate['Adjusted_Growth'] = (((dfDate['Bike_Growth'] + 100)/100) / ((growthMean['Bluebikes'] + 100)/100) * 100 ) - 100

    # Bike mode share by count group
    shareMean = pd.concat((dfDate.groupby(dfDate.Datetime.dt.month)['Bike_Car_Share'].mean(
    ), dfDate.groupby(dfDate.Datetime.dt.month)['Datetime'].mean()), axis=1)
    shareMean.index.names = ['Idx']
    shareMean.sort_values('Datetime', inplace=True, ignore_index=True)
    growthMean['Bike_Car_Share'] = shareMean['Bike_Car_Share']

    # dateMin = mdates.date2num(dfDate.Datetime.min())
    # dateMax = mdates.date2num(dfDate.Datetime.max())

    return growthMean, dfDate

# %% Plots


def plot_mode_share(df, dfDate):
    # Plot mode share over time
    fig = plt.figure(figsize=(15, 9), dpi=600)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    plt.plot(df['Datetime'], df['Bike_Car_Share'],
             color='red', marker='o', markersize=10, markerfacecolor='none', markeredgecolor='r',
             label='Month Average')

    for loc in dfDate.groupby('Location'):
        ax.scatter(loc[1]['Datetime'], loc[1]
                   ['Bike_Car_Share'], label=loc[0], marker='.')

    # Annotate
    shareMax = df['Bike_Car_Share'].max()
    shareInit = df[df['Datetime'] ==
                   df['Datetime'].min()]['Bike_Car_Share'].iloc[0]
    sharePoint = shareMax - shareInit
    sharePercentage = 100 * shareMax / shareInit
    print(f'Bike Mode Share percentage point increase: {sharePoint:0.1f}')
    print(f'Bike Mode Share percentage increase: {sharePercentage:0.0f}%')
    ax.text(
        0.5, 0.9, f'Bike mode share increased by {sharePercentage:0.0f}%\nand {sharePoint:0.1f} percentage points',
        transform=ax.transAxes, ha='center')

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
    plt.title('Garden St. project implemented in Novemeber 2022')
    ax.text(0.1, 0.01, '@BostonBikeData', transform=ax.transAxes)
    plt.show()
    fig.savefig('bike_modeshare.png')


def plot_bike_growth(df, dfDate):
    # Plot bike growth over time
    fig = plt.figure(figsize=(15, 9), dpi=600)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    plt.plot(df['Datetime'], df['Bike_Growth_0'],
             color='red', marker='o', markersize=10, markerfacecolor='none', markeredgecolor='r',
             label='Month Average')

    # # Plot median growth numbers
    # plt.plot(growthMedian['Datetime'], growthMedian['Bike_Growth'],
    #          color='purple', marker='o', markersize=10, markerfacecolor='none', markeredgecolor='purple',
    #          label='Month Median')

    # Plot Bluebikes data
    plt.plot(df['Datetime'], df['Bluebikes_0'],
             label='Cambridge BlueBikes Rides', color='blue')

    # Plot each day's count data
    for loc in dfDate.groupby('Location'):
        plt.scatter(loc[1]['Datetime'], loc[1]
                    ['Bike_Growth_0'], label=loc[0], marker='.')

    ax.text(
        0.5, 0.9, f'Bike volume increased by {df.Bike_Growth_0.max():.0f}%',
        transform=ax.transAxes, ha='center')

    plt.legend()
    plt.xlabel('Count Date')
    plt.ylabel('Bike Growth')
    plt.ylim([-100, 850])
    plt.suptitle(
        'Bike volume relative to first count on streets near Garden Street Safety Improvement Project')
    plt.title('Garden St. project implemented in Novemeber 2022')
    ax.text(0.1, 0.01, '@BostonBikeData', transform=ax.transAxes)
    plt.show()
    fig.savefig('bike_growth.png')


def plot_relative_growth(df, dfDate):
    # Plot bike growth over time, relative to Bluebikes
    fig = plt.figure(figsize=(15, 9), dpi=600)
    ax = fig.add_subplot(1, 1, 1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    # Plot mean growth numbers
    plt.plot(df['Datetime'], df['Bike_Growth_0'],
             color='red', marker='o', markersize=10, markerfacecolor='none', markeredgecolor='r',
             label='Month Average')
    plt.plot(df['Datetime'], df['Adjusted_Growth'],
             color='green', marker='o', markersize=10, markerfacecolor='none', markeredgecolor='g',
             label='Month Average adjusted for Bluebikes Ridership')

    # Plot Bluebikes data
    plt.plot(df['Datetime'], df['Bluebikes_0'],
             label='Cambridge BlueBikes Rides', color='blue')

    # Plot each day's count data
    for loc in dfDate.groupby('Location'):
        plt.scatter(loc[1]['Datetime'], loc[1]
                    ['Bike_Growth_0'], label=loc[0], marker='.')

    ax.text(
        0.4, 0.95, f'Bike volume increased by {df.Bike_Growth_0.max():.0f}%',
        transform=ax.transAxes, ha='left')
    ax.text(
        0.4, 0.9, f'Bike volume increased by {df.Adjusted_Growth.max():.0f}% relative to Bluebikes useage in Cambridge',
        transform=ax.transAxes, ha='left')

    plt.legend()
    plt.xlabel('Count Date')
    plt.ylabel('Bike Growth relative to BlueBikes useage')
    plt.ylim([-100, 1100])
    plt.suptitle(
        'Bike volume relative to first count on streets near Garden Street Safety Improvement Project, adjusted for BlueBikes ridership')
    plt.title('Garden St. project implemented in Novemeber 2022')
    ax.text(0.1, 0.01, '@BostonBikeData', transform=ax.transAxes)
    plt.show()
    fig.savefig('bike_growth_relativeBB.png')

# %% Run Script


# dfGardenCounts = collect_data()
dfGardenCounts = pd.read_pickle('all_garden_data.pkl')

dfBluebikes = import_bluebikes()

dfGrowth, dfGrowthDate = data_analysis(dfGardenCounts, dfBluebikes)

plot_mode_share(dfGrowth, dfGrowthDate)
plot_bike_growth(dfGrowth, dfGrowthDate)
plot_relative_growth(dfGrowth, dfGrowthDate)
