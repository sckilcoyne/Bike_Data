# %% Initialize
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from plotly import graph_objs as go

# pylint: disable=invalid-name, pointless-string-statement


# %% Set up streamlit
st.set_page_config(page_title='Boston Bike Data',
                   page_icon=':bike:',)
st.title('[Boston Bike Data](https://twitter.com/BostonBikeData)')


# %% Cool Projects by others
'''
## Data Projects by Others

### Crashes involving people on bikes
[@CambridgeCrash](https://twitter.com/CambridgeCrash) - Automatically tweeting reported crashes in Cambridge.

### Bluebikes data explorations by [@crschmidt](https://twitter.com/crschmidt/status/1567888389281665024?s=20&t=1bSy86Pw7Q66Fq9NCTOgNg)
[BlueBikes volume per street segment for July 2022](https://crschmidt.net/bluebikes/july2022)    
[BlueBikes system usage](https://datastudio.google.com/u/0/reporting/650b5e38-07a0-4c95-8f16-f0f9f22b7d98/page/tEnnC)

### More
[Boston Indicators - Biking during Covid](https://www.bostonindicators.org/reports/report-website-pages/covid_indicators-x2/2021/february/biking-brief)
'''
