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
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from scipy import stats
import matplotlib.dates as mdates
import matplotlib.ticker as mtick

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

    return dfAll


# %% Run Script


df = collect_data()

df.to_csv('all_garden_data.csv')

# %% Import Data
dfBluebikes = pd.read_csv('bluebikes.txt', header=0)
dfBluebikes['Bike_Growth'] = 100 * \
    dfBluebikes['Rides'] / dfBluebikes['Rides'][0]
dfBluebikes['Date'] = pd.to_datetime(dfBluebikes['Date'])

# %% Summary Data

dfDate = df.groupby(['Location', df.Datetime.dt.date])[[
    'Bicycles', 'Cars', 'Total']].sum()
dfDate = dfDate.reset_index(level=1)

dfDate['Bike_Car_Share'] = dfDate['Bicycles'] / dfDate['Cars'] * 100
dfDate['Bike_Total_Share'] = dfDate['Bicycles'] / dfDate['Cars'] * 100
# dfDate['Bike_Percentile'] = dfDate['Bicycles'] / dfDate['Bicycles'].min()

dfDate = dfDate.sort_values(by='Datetime')


def norm2first(df):
    # firstdate = df.Datetime[0]
    # firstDateBike = df.Bicycles[0]

    df['Bike_Growth'] = df['Bicycles'] / df['Bicycles'][0] * 100

    return df


dfDate = dfDate.groupby(['Location']).apply(norm2first)

dateMin = mdates.date2num(dfDate.Datetime.min())
dateMax = mdates.date2num(dfDate.Datetime.max())

# Plot mode share over time
fig = plt.figure(figsize=(15, 9), dpi=600)
ax = fig.add_subplot(1, 1, 1)
ax.yaxis.set_major_formatter(mtick.PercentFormatter())

x = mdates.date2num(dfDate['Datetime'])
m, b = np.polyfit(x, dfDate['Bike_Car_Share'], 1)
plt.plot(x, m*x+b)

for loc in dfDate.groupby('Location'):
    plt.scatter(loc[1]['Datetime'], loc[1]
                ['Bike_Car_Share'], label=loc[0], marker='.')

sharePoint = (m*dateMax+b) - (m*dateMin+b)
sharePercentage = 100 * (m*dateMax+b) / (m*dateMin+b)
print(f'Bike Mode Share percentage point increase: {sharePoint:0.1f}')
print(f'Bike Mode Share percentage increase: {sharePercentage:0.0f}%')
ax.text(
    0.5, 0.9, f'Bike mode share increased by {sharePercentage:0.0f}%\nand {sharePoint:0.1f} percentage points',
    transform=ax.transAxes, ha='center')

plt.legend()
plt.xlabel('Count Date')
plt.ylabel('Bike Mode Share')
plt.suptitle(
    'Bike/Car Mode Share on streets near Garden Street Safety Improvement Project')
plt.title('Garden St. project implemented in Novemeber 2022')
ax.text(0.1, 0.01, '@BostonBikeData', transform=ax.transAxes)
plt.show()
fig.savefig('bike_modeshare.png')


# Plot bike growth over time
fig = plt.figure(figsize=(15, 9), dpi=600)
ax = fig.add_subplot(1, 1, 1)
ax.yaxis.set_major_formatter(mtick.PercentFormatter())

x = mdates.date2num(dfDate['Datetime'])
m, b = np.polyfit(x, dfDate['Bike_Growth'], 1)

plt.plot(dfBluebikes['Date'], dfBluebikes['Bike_Growth'],
         label='Cambridge BlueBikes Rides')

for loc in dfDate.groupby('Location'):
    plt.scatter(loc[1]['Datetime'], loc[1]
                ['Bike_Growth'], label=loc[0], marker='.')

growthPercent = (m*dateMax+b) - (m*dateMin+b)
print(f'Bike volume growth: {growthPercent:.0f}%')
ax.text(
    0.5, 0.9, f'Bike volume increased by {growthPercent:.0f}%',
    transform=ax.transAxes, ha='center')

plt.plot(x, m*x+b)
plt.legend()
plt.xlabel('Count Date')
plt.ylabel('Bike Growth')
plt.suptitle(
    'Bike volume relative to first count on streets near Garden Street Safety Improvement Project')
plt.title('Garden St. project implemented in Novemeber 2022')
ax.text(0.1, 0.01, '@BostonBikeData', transform=ax.transAxes)
plt.show()
fig.savefig('bike_growth.png')
