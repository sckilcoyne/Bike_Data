'''

https://stackoverflow.com/questions/56017702/how-to-extract-table-from-pdf-in-python
'''
# %% Initialize
import pypdf
from tabula import read_pdf
import numpy as np
import pandas as pd

# %% Files

folder = 'C:/Users/Scott/Documents/GitHub/Bike_Data/data/garden st/'
ext = '.pdf'

pdf_file = f'{folder}gardenstreetativydecembercounts{ext}'

pdf_2022 = {'garden101222atrcounts': {
    'NB': [1, 3],
    'SB': [2, 4]},
    'gardenstreetativydecembercounts': {
    'NB': [1, 3],
    'SB': [2, 4]},
}

pdf_2023 = {'gardenivy2023jan1112': {
    'direction1': [1, 3],
    'direction2': [2, 4]},
    'gardenivy2023feb1516': {
    'direction1': [1, 3],
    'direction2': [2, 4]},
}

# %% Open Original Data


def format1(filename, direction, pageNumbers):
    # Get the number of pages in the file
    # pdf_reader = pypdf.PdfReader(filename)
    # n_pages = len(pdf_reader.pages)

    # For each page the table can be read with the following code
    table_pdf = read_pdf(
        filename,
        guess=True,
        pages=pageNumbers,
        stream=True,
        encoding="utf-8",
    )

    df = pd.DataFrame()
    print(f'{pageNumbers=}')
    for i, _ in enumerate(pageNumbers):
        print(f'{i=}')
        dfTable = table_pdf[i]
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
        dfTable.columns = dfTable.columns.str.replace(r'Unnamed: [0-9]* ', '')

        # Add Date Column
        dfTable['Date'] = dateData

        # Add Datetime column
        dfTable['Datetime'] = pd.date_range(
            start=dateData, periods=24, freq='H')

    df = pd.concat([df, dfTable])

    return df


df2022 = pd.DataFrame()
for pdf in pdf_2022:
    # print(pdf)
    for direction in pdf_2022[pdf]:
        # print(direction)
        # print(pdf_2022[pdf][direction])
        pdf_file = f'{folder}{pdf}{ext}'
        dfAdd = format1(pdf_file, direction, pdf_2022[pdf][direction])
        df2022 = pd.concat([df2022, dfAdd])
