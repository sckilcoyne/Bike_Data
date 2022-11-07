
# %% Initialize
# from tracemalloc import start
# import json
# import pickle
import os
import sys
import logging
from datetime import datetime, timedelta, date

import requests
import pickle
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup

# # pylint: disable=import-error
# from dataSources import ms2soft_stations
# stations_info = ms2soft_stations.stations
# # pylint: enable=import-error

# pylint: disable=missing-function-docstring
# pylint: disable=broad-except, invalid-name

# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilFuncs as utils
import utils.data_analysis as da
# pylint:enable=import-error, wrong-import-position

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)

slash = r'%2F'
pipe = ' | '

def data_folder():
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    logger.debug('cwd: %s', currentFolder)

    if currentFolder == 'dataSources':
        parent = os.path.dirname(path)
        folder = parent + '/data'
    else:
        folder = path + '/data'

    return folder

# %% Handle data files

def load_count_data(stationID):
    # dataFile = f'{dataFolder}/{stationID}.pkl'
    rawFile = f'{dataFolder}/{stationID}-raw.pkl'
    completeFile = f'{dataFolder}/{stationID}-complete.pkl'
    logFile = f'{dataFolder}/{stationID}-log.pkl'

    # Load raw count data
    try:
        rawData = pd.read_pickle(rawFile)
    except Exception:
        logger.info('Could not find %s, create new dataframe', rawFile)
        rawData = pd.DataFrame()


    # Load cleaned count data
    try:
        completeData = pd.read_pickle(completeFile)
    except Exception:
        logger.info('Could not find %s, create new dataframe', completeFile)
        completeData = None

    # Load scraped days log
    try:
        dataLog = pd.read_pickle(logFile)
    except Exception:
        logger.info('Could not find %s, create new dataframe', logFile)
        columns = ['ScrapeTime', 'DateRequest', 'DateScraped', 'StatusCode', 'QC', 'LogInfo']
        dataLog = pd.DataFrame(columns=columns)

    # Create list of dates that are either downloaded or have known data issues (missing/failed QC)
    # downloadedDates = dataLog[(dataLog['QC'] == 'Passed') | (dataLog['QC'] == 'Failed')]
    # downloadedDates = pd.to_datetime(downloadedDates['DateScraped'], format=r'%Y%m%d')

    # Create list of dates that have been attempted, except for last few to check them again
    # qc = (dataLog['QC'] == 'Failed') | (dataLog['QC'] == 'Passed')
    qc = (dataLog['QC'] == 'Passed') # retry failed dates in case something gets fixed
    qcData = dataLog[qc]
    noData = dataLog[~qc]

    qcDates = qcData['DateRequest'].unique()
    noDates = noData['DateRequest'].unique()

    downloadedDates = np.concatenate((qcDates, noDates[:-3])).tolist()

    # downloadedDates = dataLog['DateRequest'].unique()
    # downloadedDates.drop(downloadedDates.tail(7).index, inplace=True)
    # downloadedDates = downloadedDates.to_list()

    return rawData, dataLog, downloadedDates, completeData

def save_count_data(stationInfo, downloadedDates, completeData, rawData, dailyCounts):
    stationID = stationInfo["ID"]

    # Filenames for each data file
    logName = f'{stationID}-log'
    completeName = f'{stationID}-complete'
    rawName = f'{stationID}-raw'
    dailyName = f'{stationID}-daily_totals'

    # Save data files
    downloadedDates.to_pickle(f'data/{logName}.pkl', protocol=3)
    logger.info('Saved %s.pkl', logName)
    if __name__ == '__main__':
        downloadedDates.to_csv(f'testing/{logName}.csv')
        logger.info('Saved %s.csv', logName)

    completeData.to_pickle(f'data/{completeName}.pkl', protocol=3)
    logger.info('Saved %s.pkl', completeName)

    rawData.to_pickle(f'data/{rawName}.pkl', protocol=3)
    logger.info('Saved %s.pkl', rawName)

    dailyCounts.to_pickle(f'data/{dailyName}.pkl', protocol=3)
    logger.info('Saved %s.pkl', dailyName)

# %% Get data from NMDS on ms2soft

def ms2soft_session():
    session = requests.Session()

    # In Firefox developer tools, Network tab: For Get request, RMB > Copy Value > Copy as cURl
    # With still in clipboard, in python > uncurl (pip install uncurl)
    r = session.get('https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index',
            #?loc=mhd&LocationId=4001',
        headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0'
        },
        cookies={},
        auth=(),
    )

    logger.debug('Status code for ms2soft: %s', r.status_code)
    if r.status_code == 200:
        logger.info('%s: ms2soft session started', datetime.now())
        return session
    else:
        raise Exception(f'{datetime.now()}: Session Connection Error: Status Code {r.status_code}')

def ms2soft_download_day(session, startDate, stationId):
    stationInfo = stations_info[stationId]

    endDate = startDate + timedelta(days=1)

    # Format query date for URL
    # '01%2f01%2f2022+00%3a00%3a00'
    startStr = f'{startDate.month}{slash}{startDate.day}{slash}{startDate.year}+00%3a00%3a00'
    endStr = f'{endDate.month}{slash}{endDate.day}{slash}{endDate.year}+00%3a00%3a00'

    # URL direct to iframe with table of hourly data
    # Set browser.link.open_newwindow to 1 for ms2soft network monitoring, then generate report
    # Copy cURL once report is loaded and uncurl to get _id, ReportLocationSetId, LocationSetId
    URL = 'https://mhd.ms2soft.com/TDMS.UI/MvcReportViewer.aspx?' + \
        f'_id={stationInfo["_id"]}&_m=Remote' +\
        r'&_r=%2ftdms.reports%2f%5bNMDS%5d%5bStation%5d%5bDetail%5dSingleDayByStation' +\
        '&_13=False&_39=1004px&AgencyId=96' +\
        f'&StartDate={startStr}&EndDate={endStr}' +\
        f'&ReportLocationSetId={stationInfo["ReportLocationSetId"]}' +\
        f'&LocationSetId={stationInfo["LocationSetId"]}' +\
        '&Header=Massachusetts+Highway+Department' +\
        '&TitleAndCriteria=Counts+by+Station&Footer='

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Alt-Used": "mhd.ms2soft.com",
        "Connection": "keep-alive",
        "DNT": "1",
        "Referer": f"https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId={stationInfo['ID']}",
        "Sec-Fetch-Dest": "iframe",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "TE": "trailers",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0"
    }

    report = session.get(URL, headers=headers, auth=(), timeout=30)

    logger.info('%s status code: %s', startDate, report.status_code)

    logInfo = ''
    if report.status_code != 200:
        logInfo = 'Unable to reach site to download data'

    # ['ScrapeTime', 'DateRequest', 'DateScraped', 'StatusCode', 'QC', 'LogInfo']
    scrapeLog = [datetime.now(), startDate, '', report.status_code, '', logInfo]

    return report, scrapeLog

def save_iframe(iframe, stationID, scrapeDate, fileNote=None):
    iframesoup = BeautifulSoup(iframe.content, 'html.parser')
    iframesouppretty = iframesoup.prettify()

    dateStr = scrapeDate.strftime(r'%Y%m%d')

    if fileNote is None:
        fileNote = '_'
    else:
        fileNote = '_' + fileNote + '_'

    # pylint: disable=consider-using-enumerate
    with open(f'testing/iframe{fileNote}{stationID}_{dateStr}.txt','w', encoding='UTF-8') as out:
        for i in range(0, len(iframesouppretty)):
            try:
                out.write(iframesouppretty[i])
            except Exception:
                logger.info('Error printing iframe to file')
    # pylint: enable=consider-using-enumerate

# %% Data Cleaning
def clean_iframe(iframe_content, stationInfo, scrapeLog):
    try:
        dfs = pd.read_html(iframe_content.content)
    except Exception as e:
        errorNote = f'Failed to read iframe content: {e}'
        logger.info(errorNote)
        scrapeLog[-1] = f'{scrapeLog[-1]}{pipe}{errorNote}'
        save_iframe(iframe_content, stationInfo[0], stationInfo[1], 'read_html_fail')
        return None, scrapeLog

    # Find correct tables from iframe content based on lengths
    dfLens = [x.shape[0] for x in dfs]

    # Table with metadata
    try:
        dfInfo = dfs[dfLens.index(8)]
    except Exception as e:
        errorNote = f'Unable to find metadata table: {e}'
        if __name__ == '__main__':
            save_iframe(iframe_content, stationInfo[0], stationInfo[1], 'no_meta_data_table')
        scrapeLog[-1] = f'{scrapeLog[-1]}{pipe}{errorNote}'
        logger.info(errorNote)
        return None, scrapeLog

    # Table with data
    try:
        dfData = dfs[dfLens.index(31)]
    except Exception as e:
        errorNote = f'Unable to find data table: {e}'
        if __name__ == '__main__':
            save_iframe(iframe_content, stationInfo[0], stationInfo[1], 'no_data_table')
        scrapeLog[-1] = f'{scrapeLog[-1]}{pipe}{errorNote}'
        logger.info(errorNote)
        return None, scrapeLog

    # Get station ID from metadata table
    stationID = np.where(dfInfo.to_numpy() == 'Location Id:')
    stationID = dfInfo.loc[stationID[0][0], stationID[1][0] + 1]

    # Get date from metadata table
    countDate = np.where(dfInfo.to_numpy() == 'Count Date')
    countDate = dfInfo.loc[countDate[0][0], countDate[1][0] + 1]
    dateStr = datetime.strptime(countDate, r'%m/%d/%Y').strftime(r'%Y%m%d')
    # scrapeLog: ['ScrapeTime', 'DateRequest', 'DateScraped', 'StatusCode', 'QC', 'LogInfo']
    scrapeLog[2] = dateStr

    # Check that data passed QC, according to metadata table
    qc_status = np.where(dfInfo.to_numpy() == 'Qc Status:')
    qc_status = dfInfo.loc[qc_status[0][0], qc_status[1][0] + 1]
    if qc_status == 'Accepted':
        scrapeLog[4] = 'Passed'
        logger.debug('QC passed on %s', dateStr)
    else:
        scrapeLog[4] = 'Failed'
        failedQCnote = f'QC failed on {dateStr}'
        scrapeLog[-1] = f'{scrapeLog[-1]}{pipe}{failedQCnote}'
        logger.debug(failedQCnote)
        return None, scrapeLog

    # Save data table before cleaning
    # if __name__ == '__main__':
    #     filename = f'testing/{stationID}_{dateStr}'
    #     dfData.to_pickle(filename + '.pkl', protocol=3)

    # Set header (3 levels of multi-index, only 2 causes problems of dupe indicies)
    dfData.columns = [dfData.iloc[2], dfData.iloc[3], dfData.iloc[4]]
    dfData.columns.names = ['Location', 'Type', 'Direction']

    # Remove extra rows
    dfData = dfData.iloc[5:] # Drop top rows
    dfData = dfData.iloc[:-2]  # Drop subtotal rows

    # Drop useless columns (spacing columns on web page)
    dfData.dropna(axis=1, how='all', inplace=True)

    # Set Index
    dfData['Date'] = countDate
    dfData.index = [dfData.iloc[:,-1], dfData.iloc[:,0]]
    dfData.index.names = ['Date', 'Hour']

    # logger.info(df.columns)
    # logger.info(df[('Bike', 'NB')])

    if __name__ == '__main__':
        filename = f'testing/{stationID}_{dateStr}-clean'
        # dfData.to_csv(filename + '.csv')
        dfData.to_pickle(f'{filename}.pkl', protocol=3)

    scrapeLog[-1] = f'{scrapeLog[-1]}{pipe}iframe cleaned'
    logger.debug('iframe cleaned for %s', countDate)

    return dfData, scrapeLog

def standardize_df(newdf, fulldf=None):
    '''Clean up scraped data into consistent format for easier working
    '''
    NMDSbike = newdf.xs('Bike', axis=1, level=1).droplevel(level=0, axis=1)
    NMDSbike['Date'] = newdf.droplevel(level=[1, 2], axis=1)['Date']
    NMDSbike['Time'] = newdf.droplevel(level=[0, 1], axis=1)['Time']

    NMDSbike['DateTime'] = pd.to_datetime(NMDSbike['Date']+NMDSbike['Time'],
                                        format=r'%m/%d/%Y%H:%M:%S')

    NMDSbike['Year'] = NMDSbike['DateTime'].dt.year
    NMDSbike['Month'] = NMDSbike['DateTime'].dt.month
    NMDSbike['Day'] = NMDSbike['DateTime'].dt.day

    NMDSbike['MonthName'] = NMDSbike['DateTime'].dt.month_name()
    NMDSbike['DayofWeek'] = NMDSbike['DateTime'].dt.day_name()
    NMDSbike['MonthApprev'] = NMDSbike['DateTime'].dt.strftime('%b')

    # Unify direction names
    NMDSbike.columns = NMDSbike.columns.str.replace('NB', 'Northbound')
    NMDSbike.columns = NMDSbike.columns.str.replace('SB', 'Southbound')
    NMDSbike.columns = NMDSbike.columns.str.replace('EB', 'Eastbound')
    NMDSbike.columns = NMDSbike.columns.str.replace('WB', 'Westbound')

    # Set counts to numbers
    dirList = ['Total', 'Northbound', 'Southbound', 'Eastbound', 'Westbound']
    for d in dirList:
        if d in NMDSbike.columns:
            NMDSbike[d] = NMDSbike[d].apply(pd.to_numeric)

    if fulldf is not None:
        fulldf = pd.concat([fulldf, NMDSbike])
    else:
        fulldf = NMDSbike

    return fulldf


# %% Tweeting

def format_tweet(stationID, countData):
    stationName = stations_info[stationID]['TweetName']

    bikeCount = countData.xs('Bike', axis=1, level=1).xs('Total', axis=1, level=1)
    bikeCount = bikeCount.astype(int).sum().values[0]
    # totalCount = countData[('Total','Total')].astype(int).sum()

    dateString = countData.xs('Date', axis=1, level=0)
    dateString = dateString.iloc[0].values[0]
    dateString = pd.to_datetime(dateString, format=r'%m/%d/%Y')
    dateString = dateString.strftime('%a %b %d')

    tweet = f'{stationName}\n{bikeCount} riders on {dateString}'

    return tweet

def download_all_data(session):
    # Download data for each station from the given start date to present
    tweetList = []

    # Station list: array of form [SationID, FirstScrapeDate]
    for station in stations_info:
        scrapeDate = stations_info[station]['FirstDate']

        # Import pickle of previous downloaded dates and data
        # stationData, stationDataLog, downloadedDates = load_count_data(station)
        rawData, stationDataLog, downloadedDates, completeData = load_count_data(station)

        logger.info('%s: Start downloading %s', datetime.now(), station)

        while scrapeDate < date.today():

            if scrapeDate not in downloadedDates:
                logger.info('Downloading %s', scrapeDate)
                downloadedDay, scrapeLog = ms2soft_download_day(session, scrapeDate, station)

                newData = None
                try:
                    # save_iframe(downloadedDay)
                    newData, scrapeLog = clean_iframe(downloadedDay, station, scrapeLog)
                    rawData = pd.concat([rawData, newData])
                    rawData.reset_index(inplace=True, drop=True)

                    # Update log note ['ScrapeTime', 'DateRequest', 'DateScraped', 'StatusCode', 'QC', 'LogInfo']
                    if newData is not None:
                        scrapeLog[-1] = f'{scrapeLog[-1]}{pipe}Date Added to Data'

                    logger.debug('%s appended to dataframe', scrapeDate)

                except Exception as e:
                    # Update log note ['ScrapeTime', 'DateRequest', 'DateScraped', 'StatusCode', 'QC', 'LogInfo']
                    scrapeLog[-1] = f'{scrapeLog[-1]}{pipe}{e}'
                    logger.debug('%s data error: %s',scrapeDate, e)

                finally:
                    scrapeDate += timedelta(days=1)

                if newData is not None:
                    try:
                        newTweet = format_tweet(station, newData)
                        tweetList.append(newTweet)
                        logger.info('TWEET: %s', newTweet)
                    except Exception as e:
                        logger.info('Failed to make tweet: %s', e)

                    completeData = standardize_df(newData, completeData)

                # Add scrape log for date to station scraping log
                stationDataLog.loc[len(stationDataLog)] = scrapeLog

            else:
                logger.debug('No need to download %s', scrapeDate)
                scrapeDate += timedelta(days=1)

        # Create daily counts from complete data
        # [should make it only append new data]
        dailyCounts = da.daily_counts(completeData)

        # stationData.to_pickle(f'data/{station}.pkl', protocol=3)
        # stationDataLog.to_pickle(f'data/{station}-log.pkl', protocol=3)
        save_count_data(stations_info[station], stationDataLog, completeData, rawData, dailyCounts)
        # rawData.to_pickle(f'data/{station}-raw.pkl', protocol=3)
        # completeData.to_pickle(f'data/{station}-complete.pkl', protocol=3)
        # stationDataLog.to_pickle(f'data/{station}-log.pkl', protocol=3)

        # Save in human readble form for debug
        if __name__ == '__main__':
            stationDataLog.to_csv(f'testing/{station}-log.csv')

    logger.info('Completed downloaded all data')
    return tweetList

# %% Load Station info
dataFolder = data_folder()
infile = open(dataFolder + '/ms2soft_stations.pkl', 'rb')
stations_info = pickle.load(infile)
# print(records)
infile.close()

# %% Run as script
def main(): # Prevents accidental globals

    session = ms2soft_session()

    tweetList = download_all_data(session)

    return tweetList

def print_tweets(tweets):
    for tweet in tweets:
        tweetStr = f'TWEET: {tweet}'
        logger.info(tweetStr)

if __name__ == "__main__":
    # pylint: disable=ungrouped-imports
    import logging.config
    logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')
    logger.debug("Logging is configured.")

    # Run scraping
    Tweets = main()

    print_tweets(Tweets)
