
# %% Initialize
import os
import streamlit as st
# from google.oauth2 import service_account
# from google.cloud import storage
import pandas as pd
import plotly.express as px
from plotly import graph_objs as go

# pylint: disable=invalid-name, pointless-string-statement


# %% Set up streamlit
st.set_page_config(page_title='Boston Bike Data',
                   page_icon=':bike:',)
st.title('[Boston Bike Data](https://twitter.com/BostonBikeData)')


tabInfo, tabGroups = st.tabs(['Info', 'Groups to Support'])


# %% Info
with tabInfo:
    '''
    This is a project to explore and highlight data around biking in the Boston region.
    Primarily, this is being done by posting daily bike counts from counters in the region 
    that have publically published data to the 
    [Boston Bike Data](https://twitter.com/BostonBikeData) twitter account.

    This site is a place to explore the data in more depth. I'll also be using this to 
    experiment with better ways to present the data posted to Twitter.

    ### Data Sources:
    * [Cambridge Open Data Portal - Eco-Totem Broadway Bicycle Counter](https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg)
    * [MassDOT Non-Motorized Database System](https://mhd.ms2soft.com/TDMS.UI/nmds/dashboard?loc=mhd)
    '''

with tabGroups:
    '''
    ### Local Bike Orgs to support
    * [Mass Bike](https://www.massbike.org/) [(Twitter)](https://twitter.com/MassBike)
    * [Boston Cyclist Union]() [(Twitter)](https://twitter.com/bostonbikeunion)

    * [Cambridge Bike Safety](https://www.cambridgebikesafety.org/) [(Twitter)](https://twitter.com/cambbikesafety)
    * [Somerville Bike Safety]() [(Twitter)](https://twitter.com/somerbikesafety)
    * [West Rox Bikes](https://www.facebook.com/groups/westroxbikes) [(Twitter)](https://twitter.com/WestRoxBikes)
    * [Southie Bikes](https://docs.google.com/forms/d/e/1FAIpQLSevN1hQRlAu2pMpcYKpZfY1y7BQqNUVyWzmRgUgL5qnIoVRag/viewform) [(Twitter)](https://twitter.com/SouthieBikes)

    * [Bikes Not Bombs](https://bikesnotbombs.org/) [(Twitter)](https://twitter.com/bikesnotbombs)
    * [Livable Streets](https://www.livablestreets.info/) [(Twitter)](https://twitter.com/StreetsBoston)
    '''
