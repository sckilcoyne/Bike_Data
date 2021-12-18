# -*- coding: utf-8 -*-
"""

http://dev.seeclickfix.com


Created on Mon Dec 13 22:15:08 2021

@author: Scott
"""
import requests
# import pickle
import utils.utilFuncs as utils

scfURL = 'https://seeclickfix.com/api/v2/'


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
    baseURL = 'https://seeclickfix.com/open311/v2/requests.json?organization_id='

    agencyDict = {}

    for i in range(0, 10000):
        response = requests.get(baseURL + str(i))

        try:
            agency = response.json()[0]['agency_responsible']
            print(agency + ': ' + str(i))

            agencyDict[agency] = {'id': i}
        except:

            continue

    utils.pickle_dict(agencyDict, 'open311_agencies')


'''
id: 193  Agency: Boston 311
id: 554  Agency: City of Cambridge
id: 387  Agency: Watertown, MA

id: 173  Agency: Saugus, MA
id: 193  Agency: Boston 311
id: 196  Agency: Swampscott, MA
id: 334  Agency: Chicopee, MA
id: 335  Agency: Town of Wakefield
id: 337  Agency: Malden, MA
id: 342  Agency: Taunton, MA
id: 351  Agency: City of Fitchburg, MA
id: 352  Agency: Framingham, MA
id: 361  Agency: Melrose, MA
id: 365  Agency: Needham, MA
id: 368  Agency: Randolph, MA
id: 375  Agency: New Bedford, MA
id: 385  Agency: Whitman, MA
id: 387  Agency: Watertown, MA
id: 388  Agency: Braintree, MA
id: 396  Agency: Sudbury, MA
id: 399  Agency: Worcester, MA
id: 400  Agency: Holyoke, MA
id: 402  Agency: Reading, MA
id: 403  Agency: Marlborough, MA
id: 405  Agency: Westwood, MA
id: 406  Agency: Brockton, MA
id: 407  Agency: City of Salem
id: 448  Agency: Chelmsford, MA
id: 539  Agency: Norwell, MA
id: 541  Agency: Cohasset, MA
id: 542  Agency: Amherst, MA
id: 544  Agency: Town of Natick, MA
id: 547  Agency: Gloucester, MA
id: 548  Agency: North Attleboro, MA
id: 549  Agency: Medford, MA
id: 550  Agency: Attleboro, MA
id: 551  Agency: Westfield, MA
id: 552  Agency: Greenfield, MA
id: 554  Agency: City of Cambridge

'''


# %% Get issues
cityList = ['Boston', 'Cambridge', 'Somerville']


# %% Script Testing
if __name__ == '__main__':
    # get_scf_cities()

    # scfCities = load_scf_cities()

    get_open311_cities()
