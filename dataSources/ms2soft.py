
# %% Initialize
import requests
# import json
import pickle
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd

# pylint: disable=missing-function-docstring
# pylint: disable=broad-except, invalid-name

# %% Station IDs
stations = {
    '4001':{
        'ID': '4001',
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
    '3001': {
        'ID': '3001',
        'Description': 'Bruce Freeman Rail Trail',
        'Community': 'Acton',
        'County': 'Middlesex',
        'Info': 'MHD Owned: CMU Device',
        'District': 3,
        'Area Type': 'Rural',
        'Functional Class': 'Trail or Shared Use Path',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5228,
        'LocationSetId': 2764341,
        },
    '4004_SB': {
        'ID': '4004_SB',
        'Description': 'Fellwsay (Route 28)',
        'Community': 'Medford',
        'County': 'Middlesex',
        'Info': 'MHD owned: Q-Free Hi-Trac CMU',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5230,
        'LocationSetId': 2764347,
        },
    '4004_NB': {
        'ID': '4004_NB',
        'Description': 'Fellwsay (Route 28)',
        'Community': 'Medford',
        'County': 'Middlesex',
        'Info': 'MHD owned: Q-Free Hi-Trac CMU',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5231,
        'LocationSetId': 2764350,
        },
    '4003_SB': {
        'ID': '4003_SB',
        'Description': 'Bridge Street',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5232,
        'LocationSetId': 2764353,
        },
    '4003_NB': {
        'ID': '4003_NB',
        'Description': 'Bridge Street',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5233,
        'LocationSetId': 2764356,
        },
    '4003_WB': {
        'ID': '4003_NB',
        'Description': 'Veterans of Foreign Wars Highway',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5234,
        'LocationSetId': 2764359,
        },
    '4003_EB': {
        'ID': '4003_EB',
        'Description': 'Veterans of Foreign Wars Highway',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5235,
        'LocationSetId': 2764362,
        },
    '4002_NB': {
        'ID': '4002_NB',
        'Description': 'Sgt James Ayube Memorial Drive',
        'Community': 'Salem',
        'County': 'Essex',
        'Info': 'MHD owned: Eco Counter',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5236,
        'LocationSetId': 2764364,
        },
    '4002_SB': {
        'ID': '4002_SB',
        'Description': 'Sgt James Ayube Memorial Drive',
        'Community': 'Salem',
        'County': 'Essex',
        'Info': 'MHD owned: Eco Counter',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5237,
        'LocationSetId': 2764366,
        },
    '5001_SB': {
        'ID': '5001_SB',
        'Description': 'Brayton Ave',
        'Community': 'Fall River',
        'County': 'Bristol',
        'Info': 'Crosswalk Ped Counts - MHD Owned Miovision Data',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5238,
        'LocationSetId': 2764372,
        },
    '5001_WB': {
        'ID': '5001_WB',
        'Description': 'Brayton Ave',
        'Community': 'Fall River',
        'County': 'Bristol',
        'Info': 'Quequechan River Rail Trail, Crossing at Brayton Ave - MHD Owned, Miovision integration sIte: TMC Int 8024292',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Minor Arterial',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5239,
        'LocationSetId': 2764376,
        },
    '5001_EB': {
        'ID': '5001_EB',
        'Description': 'Brayton Ave',
        'Community': 'Fall River',
        'County': 'Bristol',
        'Info': 'Quequechan River Rail Trail, Crossing at Brayton Ave - MHD Owned, Miovision integration sIte: TMC Int 8024292',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Minor Arterial',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5240,
        'LocationSetId': 2764378,
        },
    '5002_WB': {
        'ID': '5002_WB',
        'Description': 'Centre St',
        'Community': 'Brockton',
        'County': 'Plymouth',
        'Info': 'MHD Owned: Eco Counter',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5241,
        'LocationSetId': 2764381,
        },
    '5002_EB': {
        'ID': '5002_EB',
        'Description': 'Centre St',
        'Community': 'Brockton',
        'County': 'Plymouth',
        'Info': 'MHD Owned: Eco Counter',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5242,
        'LocationSetId': 2764385,
        },
    '6002_WB': {
        'ID': '6002_WB',
        'Description': 'Longfellow Bridge',
        'Community': 'Cambridge',
        'County': 'Suffolk',
        'Info': '',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5243,
        'LocationSetId': 2764387,
        },
    '6002_EB': {
        'ID': '6002_EB',
        'Description': 'Longfellow Bridge',
        'Community': 'Cambridge',
        'County': 'Suffolk',
        'Info': 'Longfellow Bridge MHD owned:CMU Device',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5245,
        'LocationSetId': 2764393,
        },
    '6003_EB': {
        'ID': '6003_EB',
        'Description': 'Charles River Dam Rd',
        'Community': 'Boston',
        'County': 'Suffolk',
        'Info': 'MHD Owned: Eco Counter',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5246,
        'LocationSetId': 2764396,
        },
    '6001_EB': {
        'ID': '6001_EB',
        'Description': 'Charles River Dam Rd',
        'Community': 'Cambridge',
        'County': 'Middlesex',
        'Info': '',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5247,
        'LocationSetId': 2764399,
        },
    '6001_WB': {
        'ID': '6001_WB',
        'Description': 'Charles River Dam Rd',
        'Community': 'Cambridge',
        'County': 'Middlesex',
        'Info': '',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5248,
        'LocationSetId': 2764401,
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

def ms2soft_download_day(session, start, end, stationInfo):
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

def save_iframe(iframe):
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

    # do a qc status check (Accepted/Rejected)
    date = dfs[9].iloc[5][1]
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
    station = stations['4004_NB']
    startDay = datetime.now() - timedelta(days=1)
    endDay = datetime.now()

    ms2softSession = ms2soft_session()
    downloadedDay = ms2soft_download_day(ms2softSession, startDay, endDay, station)
    save_iframe(downloadedDay)
    clean_iframe(downloadedDay)
