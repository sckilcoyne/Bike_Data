# -*- coding: utf-8 -*-
"""
Created on Fri Dec 17 20:56:57 2021

@author: Scott
"""
# %% Initialize
import pickle
import os


# %% Functions
def pickle_dict(pklDict, fileName):
    """Dict to be saved to disc as pickle file.

    Args:
        pklDict (dict): Dict to save as pickle file.
        fileName (str): Where to save file.
    """
    # Save a Dict to a pickle file

    # f = open('..\data\\' + fileName + '.pkl', 'wb')
    f = open(fileName + '.pkl', 'wb')

    # write the python object (dict) to pickle file
    pickle.dump(pklDict, f, protocol=3)

    # close file
    f.close()


def load_pickled_dict(fileName, folder):
    """Load dict saved in pickle file.

    Args:
        fileName (str)
        folder (str)

    Returns:
        dict: Dict loaded from pickle file.
    """

    filePath = folder + '\\' + fileName + '.pkl'
    infile = open(filePath, 'rb')
    openedDict = pickle.load(infile)
    # print(records)
    infile.close()

    return openedDict


def get_data_folder():
    """Find 'data' folder in project

    Returns:
        str: 'data' folder path
    """
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    if currentFolder == 'utils':
        parent = os.path.dirname(path)
        dataFolder = parent + '\data'
    else:
        dataFolder = currentFolder + '\data'

    return dataFolder
