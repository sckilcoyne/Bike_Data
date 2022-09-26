
# %% Initialize
import requests
# import json
import pickle
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np

# pylint: disable=import-error
import ms2soft_stations
stations_info = ms2soft_stations.stations
# pylint: enable=import-error

# pylint: disable=missing-function-docstring
# pylint: disable=broad-except, invalid-name



# %% Call ms2soft

def ms2soft_session():
    session = requests.Session()

    # In Firefox developer tools, Network tab: For Get request, Right Click > Copy Value > Copy as cURl
    # With still in clipboard, in python > uncurl (pip install uncurl)
    r = session.get('https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index', #?loc=mhd&LocationId=4001',
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

    # print(f'{r.status_code=}')
    if r.status_code == 200:
        pass
    else:
        raise Exception(f'Session Connection Error: Status Code {r.status_code}')

    return session

def ms2soft_download_day(session, start, stationID):
    end = start + timedelta(days=1)

    stationInfo = stations_info[stationID]

    # Create report generation url
    slash = r'%2F'

    urlBase = 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/GetReportViewer?loc=mhd&'
    dateStart = f'StartDate={start.month}{slash}{start.day}{slash}{start.year}&'
    dateEnd = f'EndDate={end.month}{slash}{end.day}{slash}{end.year}&'
    reportType = 'SelectedReport=%5BNMDS%5D%5BStation%5D%5BDetail%5DSingleDayByStation&'
    reportLocationSetId = f'''ReportLocationSetId={stationInfo['ReportLocationSetId']}&'''
    LocationSetId = f'''LocationSetId={stationInfo['LocationSetId']}'''

    # Run report
    urlRunReport = urlBase + dateStart + dateEnd + reportType + \
                   reportLocationSetId + LocationSetId
    report = session.get(urlRunReport,
        headers={
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Referer': f'''https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId={stationInfo['ID']}''',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'TE': 'trailers',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0',
            'X-Requested-With': 'XMLHttpRequest'
        },
        cookies={
            'MS2Session_TDMS.UI': 'sesdhyfytvz5u4ilkvxdlxeo',
            'MapPanel.settings': '{showOnlySearchResults:false,ArcGISLayerCheckedState:true}'
        },
        auth=(),
    )

    # print(f'{report.status_code=}')
    if report.status_code == 200:
        pass
    else:
        raise Exception(f'Report Generation Connection Error: Status Code {report.status_code}')

    # Open report iframe
    soup = BeautifulSoup(report.text, 'html.parser')
    # print(soup)
    # print(soup.find('iframe')['src'])
    iframe_content = session.get('https://mhd.ms2soft.com' + soup.find('iframe')['src'], timeout=30)
    # slow website, needed to add delay

    return iframe_content

def save_iframe(iframe, stationInfo):
    """_summary_

    Args:
        iframe (_type_): _description_
    """
    iframesoup = BeautifulSoup(iframe.content, 'html.parser')
    iframesouppretty = iframesoup.prettify()
    # print(iframesoup.prettify())

    dfsave = pd.read_html(iframe.content)

    with open('testing/iframe_content.pkl', 'wb') as f:
        pickle.dump(dfsave, f)


    # stationID = dfsave[9].iloc[1][1]
    # date = dfsave[9].iloc[5][1]
    # dateStr = datetime.strptime(date, r'%m/%d/%Y').strftime(r'%Y%m%d')

    stationID = stationInfo[0]
    date = stationInfo[1]
    dateStr = date.strftime(r'%Y%m%d')

    # pylint: disable=consider-using-enumerate
    with open(f'testing/iframe_{stationID}_{dateStr}.txt','w', encoding='UTF-8') as out:
        for i in range(0, len(iframesouppretty)):
            try:
                out.write(iframesouppretty[i])
            except Exception:
                print('Error printing iframe to file')
    # pylint: enable=consider-using-enumerate

# %% Data Cleaning
def clean_iframe(iframe_content, stationInfo):
    dfs = pd.read_html(iframe_content.content)

    # Find correct tables from iframe content based on lengths
    dfLens = [x.shape[0] for x in dfs]
    # Table with metadata
    try:
        dfInfo = dfs[dfLens.index(8)]
    except:
        if __name__ == '__main__':
            save_iframe(iframe_content, stationInfo)
        raise Exception('Unable to find metadata table') from None
    # Table with data
    try:
        dfData = dfs[dfLens.index(31)]
    except:
        if __name__ == '__main__':
            save_iframe(iframe_content, stationInfo)
        raise Exception('Unable to find data table') from None

    # Get station ID from metadata table
    stationID = np.where(dfInfo.to_numpy() == 'Location Id:')
    stationID = dfInfo.loc[stationID[0][0], stationID[1][0] + 1]

    # Get date from metadata table
    countDate = np.where(dfInfo.to_numpy() == 'Count Date')
    countDate = dfInfo.loc[countDate[0][0], countDate[1][0] + 1]
    dateStr = datetime.strptime(countDate, r'%m/%d/%Y').strftime(r'%Y%m%d')

    # Check that data passed QC, according to metadata table
    qc_status = np.where(dfInfo.to_numpy() == 'Qc Status:')
    qc_status = dfInfo.loc[qc_status[0][0], qc_status[1][0] + 1]
    if qc_status == 'Accepted':
        print(f'QC passed on {dateStr}')
    else:
        raise Exception(f'Failed QC on {dateStr}') from None

    # Save data table before cleaning
    # if __name__ == '__main__':
    #     filename = f'testing/{stationID}_{dateStr}'
    #     dfData.to_pickle(filename + '.pkl', protocol=3)

    # Drop useless columns (spacing columns on web page)
    dfData.dropna(axis=1, how='all', inplace=True)

    # Set header
    dfData.columns = [dfData.iloc[3], dfData.iloc[4]]
    dfData.columns.names = ['Type', 'Direction']

    # Remove extra rows
    dfData = dfData.iloc[5:] # Drop top rows
    dfData = dfData.iloc[:-2]  # Drop subtotal rows

    # Set Index
    dfData['Date'] = countDate
    dfData.index = [dfData.iloc[:,-1], dfData.iloc[:,0]]
    dfData.index.names = ['Date', 'Hour']

    # print(df.columns)
    # print(df[('Bike', 'NB')])

    # filename = f'data/{stationID}_{dateStr}'
    # dfData.to_csv(filename + '.csv')
    # dfData.to_pickle(filename + '.pkl', protocol=3)

    return dfData

def download_all_data(session, stations):
    # stationDfs = []

    for station in stations:
        stationID = station[0]
        startDate = station[1]

        print(f'Start downloading {stationID}')

        stationDf = pd.DataFrame()
        while (startDate <= date.today()):
            try:
                downloadedDay = ms2soft_download_day(session, startDate, stationID)
                # save_iframe(downloadedDay)
                newdf = clean_iframe(downloadedDay, station)
                stationDf = pd.concat([stationDf, newdf])
                print()
            except Exception as e:
                print(f'{startDate}: {e}')
            finally:
                startDate += timedelta(days=1)
        # stationDfs.append(stationDf)
        stationDf.to_pickle(f'data/{stationID}.pkl', protocol=3)
    print('Completed downloaded all data')
    # return stationDfs

# %% Run as script
if __name__ == "__main__":
    Jan2020 = date(2020, 1, 1)
    Jan2021 = date(2021, 1, 1)

    stationList = [
        # Lex Minuteman
        # ['4001', date(2020, 1, 1)], \
        # Medford Fellsway
        ['4004_SB', date(2021, 7, 1)], \
        # ['4004_NB', date(2021, 7, 1)], \
        # Cambridge Charles River Dam
        # ['6003_EB', date(2021, 7, 1)],\
        ]

    # station = stations[stationList[0]]
    # startDay = datetime.now() - timedelta(days=1)

    ms2softSession = ms2soft_session()

    download_all_data(ms2softSession, stationList)
