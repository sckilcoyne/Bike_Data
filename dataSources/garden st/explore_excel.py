
# %% Initialize
import pandas as pd
import glob

# %% Import Data

# gardenFolder = 'dataSources\garden st'

folder = 'C:/Users/Scott/Documents/GitHub/Bike_Data/dataSources/garden st/'
ext = '.xlsx'

excelList = glob.glob(f'{folder}*{ext}')
# excelList = [x[len(folder):-len(ext)] for x in excelList]

dfList = []

for file in excelList:
    df = pd.read_excel(file)
    print(file)
    # print(df.iloc[[1,6,7],0])
    
    dfList.append(df)