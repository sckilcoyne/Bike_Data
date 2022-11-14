
# %% Initialize
import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilSt as ust

# pylint: disable=invalid-name, pointless-string-statement


# %% Set up streamlit
st.set_page_config(page_title='Boston Bike Data',
                   page_icon=':bike:',)
st.title('[Boston Bike Data](https://twitter.com/BostonBikeData)')


tabInfo, tabGroups = st.tabs(['Info', 'Groups to Support'])

# %% Plots
def plot_source_map():
    '''Plot the locations of all counters
    '''
    dataSources = pd.DataFrame(ust.dataSources).transpose()
    dataSources.reset_index(inplace=True)
    dataSources = dataSources.rename(columns = {'index':'counterName'})


    # add map:
    #         https://docs.streamlit.io/library/api-reference/charts/st.map
    #         https://plotly.com/python/scattermapbox/
    dataSources['urlLink'] = [f'<a href="{url}/">Click here</a>' for url in dataSources['url']]
    dataSources['size'] = 7

    # st.write(dataSources)
    fig = px.scatter_mapbox(dataSources, lat='Lat', lon='Long',
                            # name='----',
                            # mode= 'markers',
                            hover_name='counterName',
                            hover_data={'urlLink': True,
                                        'size': False,
                            },
                            labels={'urlLink': 'Data Source'},
                            color_discrete_sequence=['blue'],
                            zoom=11,
                            size='size',
                            # height=300,
                            )
    fig.update_layout(mapbox_style="open-street-map",
                        margin={"r":0,"t":0,"l":0,"b":0},
                        # clickmode='select',
                        hoverdistance=100,
                        mapbox=dict(
                            center=go.layout.mapbox.Center(
                                lat=(dataSources['Lat'].max() + dataSources['Lat'].min()) / 2,
                                lon=(dataSources['Long'].max() + dataSources['Long'].min()) / 2 ),
                            ))
    # fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    # fig.update_layout(mapbox_bounds={"west": -71.2, "east": -71, "south": 42.3, "north": 42.5})
    st.plotly_chart(fig)

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
    plot_source_map()

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
