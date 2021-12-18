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
    # Save a Dict to a pickle file

    f = open('..\data\\' + fileName + '.pkl', 'wb')

    # write the python object (dict) to pickle file
    pickle.dump(pklDict, f, protocol=3)

    # close file
    f.close()


def load_pickled_dict(fileName, folder):

    filePath = folder + '\\' + fileName + '.pkl'
    infile = open(filePath, 'rb')
    openedDict = pickle.load(infile)
    # print(records)
    infile.close()

    return openedDict


def get_data_folder():
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    if currentFolder == 'utils':
        parent = os.path.dirname(path)
        dataFolder = parent + '\data'
    else:
        dataFolder = currentFolder + '\data'

    return dataFolder
