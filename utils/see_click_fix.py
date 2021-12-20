# -*- coding: utf-8 -*-
"""

http://dev.seeclickfix.com


Created on Mon Dec 13 22:15:08 2021

@author: Scott
"""
import requests
import pandas as pd
# import pickle
import utils.utilFuncs as utils

scfURL = 'https://seeclickfix.com/api/v2/'

baseOpen311URL = 'https://seeclickfix.com/open311/v2/requests.json?organization_id='


# %% Get Place Names and IDs
def get_scf_cities():
    bostonPlaces = 'places?address=Boston,+MA&per_page=100'

    places = requests.get(scfURL + bostonPlaces)

    placeData = places.json()['places']

    # Create list of cities near Boston
    cityDict = {}
    for place in placeData:
        if place['place_type'] == 'City':
            cityDict[place['name']] = {'id': place['id'],
                                       'url': place['url'],
                                       'url_name': place['url_name']}

    utils.pickle_dict(cityDict, 'scf_cities')


def load_scf_cities():
    dataFolder = utils.get_data_folder()

    cities = utils.load_pickled_dict('scf_cities', dataFolder)

    return cities


def get_open311_cities():
    # Create a list of all cities/agencies ids on SCF Open311
    agencyDict = {}

    for i in range(0, 10000):
        response = requests.get(baseOpen311URL + str(i))

        try:
            agency = response.json()[0]['agency_responsible']
            print(agency + ': ' + str(i))

            agencyDict[agency] = {'id': i}
        except:

            continue

    utils.pickle_dict(agencyDict, 'open311_agencies')


scf_cities = utils.load_pickled_dict('scf_cities', utils.get_data_folder())


# %% Get issues
'''
id: 193  Agency: Boston 311
id: 554  Agency: City of Cambridge
id: 387  Agency: Watertown, MA
'''

cityList = {'Boston': 193,
            'Cambridge': 554,
            'Watertown': 387,
            }


def get_open311_issues(cityID, pages):
    issues = []

    for page in range(0, pages-1):
        issues = issues + requests.get(baseOpen311URL + str(cityID) +
                                       '&page=' + str(page)).json()

    issuesDf = pd.DataFrame(issues)
    return issuesDf


# %% Script Testing
if __name__ == '__main__':
    # get_scf_cities()

    # scfCities = load_scf_cities()

    # get_open311_cities()

    issues = get_open311_issues(554, 100)

    print()
