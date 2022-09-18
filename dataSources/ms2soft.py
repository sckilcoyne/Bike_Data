
# %% Initialize
import requests
# import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd

# pylint: disable=missing-function-docstring
# pylint: disable=broad-except, invalid-name

# %% Station IDs
#4001 - Minuteman Commuter Bikeway, Lexington, Middlesex
#3001 - Bruce Freeman Rail Trail, Acton, Middlesex
#4004_SB - Fellwsay (Route 28), Medford, Middlesex
#4004_NB - Fellsway (Route 28), Medford, Middlesex
#4003_SB - Bridge Street, Lowell, Middlesex
#4003_NB - Bridge Street, Lowell, Middlesex
#4003_WB - Veterans of Foreign Wars Highway, Lowell, Middlesex
#4003_EB - Veterans of Foreign Wars Highway, Lowell, Middlesex
#4002_NB - Sgt James Ayube Memorial Drive, Salem, Essex
#4002_SB - Sgt James Ayube Memorial Drive, Salem, Essex
#5001_SB - Brayton Ave, Fall River, Bristol
#5001_WB - Brayton Ave, Fall River, Bristol
#5001_EB - Brayton Ave, Fall River, Bristol
#5002_WB - Centre St, Brockton, Plymouth
#5002_EB - Centre St, Brockton, Plymouth

#6002_WB - Longfellow Bridge, Cambridge, Suffolk
#6002_EB - Longfellow Bridge, Cambridge, Suffolk
#6003_EB - Charles River Dam Rd, Boston, Suffolk
#6001_EB - Charles River Dam Road, Cambridge, Middlesex
#6001_WB - Charles River Dam Road, Cambridge, Middlesex

stations = {
    4001:{
        'ID': 4001,
        'Description': 'Minuteman Commuter Bikeway',
        'Community': 'Lexington',
        'County': 'Middlesex',
        'Info': 'MHD owned: Q-Free Hi-Trac CMU',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Trail or Shared Use Path',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5224,
        'LocationSetId': 2763970,
        },
    3001: {
        'Description': 'Bruce Freeman Rail Trail',
        },
    }

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

def ms2soft_download_day(session, start, end, station):
    # Create report generation url
    slash = r'%2F'

    urlBase = 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/GetReportViewer?loc=mhd&'
    dateStart = f'StartDate={start.month}{slash}{start.day}{slash}{start.year}&'
    dateEnd = f'EndDate={end.month}{slash}{end.day}{slash}{end.year}&'
    reportType = 'SelectedReport=%5BNMDS%5D%5BStation%5D%5BDetail%5DSingleDayByStation&'
    reportLocationSetId = f'''ReportLocationSetId={station['ReportLocationSetId']}&'''
    LocationSetId = f'''LocationSetId={station['LocationSetId']}'''

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
            'Referer': f'''https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId={station['ID']}''',
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

def save_iframe(iframe):
    """_summary_

    Args:
        iframe (_type_): _description_
    """
    iframesoup = BeautifulSoup(iframe.content, 'html.parser')
    iframesouppretty = iframesoup.prettify()
    # print(iframesoup.prettify())

    dfsave = pd.read_html(iframe.content)

    stationID = dfsave[9].iloc[1][1]
    date = dfsave[9].iloc[5][1]
    dateStr = datetime.strptime(date, r'%m/%d/%Y').strftime(r'%Y%m%d')

    # pylint: disable=consider-using-enumerate
    with open(f'testing/iframe_{stationID}_{dateStr}.txt','w', encoding='UTF-8') as out:
        for i in range(0, len(iframesouppretty)):
            try:
                out.write(iframesouppretty[i])
            except Exception:
                print('Error printing iframe to file')
    # pylint: enable=consider-using-enumerate

# %% Data Cleaning
def clean_iframe(iframe_content):
    dfs = pd.read_html(iframe_content.content)

    date = dfs[8].iloc[5][1]
    stationID = dfs[9].iloc[1][1]
    dateStr = datetime.strptime(date, r'%m/%d/%Y').strftime(r'%Y%m%d')

    df = dfs[11]
    filename = f'testing/{stationID}_{dateStr}'
    df.to_pickle(filename + '.pkl', protocol=3)

    cols = [0,1,3,7,10,13,17,21] # Drop useless columns
    df.drop(df.columns[cols], axis=1, inplace=True)
    df.columns = [df.iloc[3], df.iloc[4]]  # Set header
    df = df.iloc[5:] # Drop top rows
    df = df.iloc[:-2]  # Drop subtotal rows
    df['Date'] = date

    # print(df.columns)
    # print(df[('Bike', 'NB')])
    

    filename = f'data/{stationID}_{dateStr}'
    df.to_csv(filename + '.csv')
    df.to_pickle(filename + '.pkl', protocol=3)

# %% Run as script
if __name__ == "__main__":
    startDay = datetime.now() - timedelta(days=1)
    endDay = datetime.now()

    ms2softSession = ms2soft_session()
    downloadedDay = ms2soft_download_day(ms2softSession, startDay, endDay, stations[4001])
    save_iframe(downloadedDay)
    clean_iframe(downloadedDay)
