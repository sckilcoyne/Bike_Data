
# %% Initialize
import requests
import json
from bs4 import BeautifulSoup

# Reading data from a SQL-SERVER/Oracle database
import pyodbc

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


# %% Call ms2soft

session_url = 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis?loc=mhd'

url = r'http://sql-2/ReportServer?%2Ftdms.reports%2F%5BNMDS%5D%5BStation%5D%5BDetail%5DSingleDayByStation&amp;' + \
    'Header=Massachusetts%20Highway%20Department&amp;' + \
    r'TitleAndCriteria=Counts%20by%20Station&amp;' + \
    'Footer=&amp;' + \
    'AgencyId=96&amp;' + \
    r'StartDate=01%2F01%2F2022%2000%3A00%3A00&amp;' + \
    r'EndDate=12%2F31%2F2022%2000%3A00%3A00&amp;' + \
    'LocationSetId=2757145&amp;' + \
    'ReportLocationSetId=5212&amp;' + \
    'rs%3AParameterLanguage=&amp;' + \
    'rs%3ACommand=Render&amp;' + \
    'rs%3AFormat=ATOM&amp;' + \
    'rc%3AItemPath=Tablix6.Subreport1.Tablix3.Offset.Pathway.Mode.Direction'

    # ReportLocationSetId appears to be a counter saved in their database

# <div class="viewer" id="ReportViewer">
# 		<iframe Height="1024" Width="1280" class="report-finished" 
            # src="/TDMS.UI/MvcReportViewer.aspx?
            # _id=d8eaf35b-563e-49c6-949c-00eff764315a&amp;
            # _m=Remote&amp;
            # _r=%2ftdms.reports%2f%5bNMDS%5d%5bStation%5d%5bDetail%5dSingleDayByStation&amp;
            # _13=False&amp;
            # _39=1004px&amp;
            # AgencyId=96&amp;
            # StartDate=01%2f01%2f2022+00%3a00%3a00&amp;
            # EndDate=12%2f31%2f2022+00%3a00%3a00&amp;
            # ReportLocationSetId=5219&amp;
            # LocationSetId=2759128&amp;
            # Header=Massachusetts+Highway+Department&amp;
            # TitleAndCriteria=Counts+by+Station&amp;
            # Footer=" style="border: none">
            # </iframe>
# </div>

url2 = r'https://mhd.ms2soft.com//TDMS.UI/MvcReportViewer.aspx?_id=d8eaf35b-563e-49c6-949c-00eff764315a&amp;_m=Remote&amp;_r=%2ftdms.reports%2f%5bNMDS%5d%5bStation%5d%5bDetail%5dSingleDayByStation&amp;_13=False&amp;_39=1004px&amp;AgencyId=96&amp;StartDate=01%2f01%2f2022+00%3a00%3a00&amp;EndDate=12%2f31%2f2022+00%3a00%3a00&amp;ReportLocationSetId=5219&amp;LocationSetId=2759128&amp;Header=Massachusetts+Highway+Department&amp;TitleAndCriteria=Counts+by+Station&amp;Footer='

# conn = pyodbc.connect()

session = requests.Session()

session_response = session.get(session_url)
session_response.raise_for_status()

raw = session.get(url2)

print(raw.content)
soup = BeautifulSoup(raw.content, "html.parser")

data = json.loads(raw.text)


print(data)





csv = 'https://mhd.ms2soft.com/TDMS.UI/Reserved.ReportViewerWebControl.axd?' + \
    'ReportSession=4vrzh12tkni1nkbzffvmuq55' + '&' + \
    'Culture=1033' + '&' + \
    'CultureOverrides=True' + '&' + \
    'UICulture=1033' + '&' + \
    'UICultureOverrides=True' + '&' + \
    'ReportStack=1' + '&' + \
    'ControlID=b7993fc1d005469dbd15c9bc2e758f6b' + '&' + \
    r'RSProxy=http%3a%2f%2fsql-2%2fReportServer' + '&' + \
    'OpType=Export' + '&' + \
    r'FileName=%5bNMDS%5d%5bStation%5d%5bDetail%5dSingleDayByStation' + '&' + \
    'ContentDisposition=OnlyHtmlInline&Format=CSV'