'''
Database of MassDOT Non-Motorized Database System Stations

To get [_id, ReportLocationSetId, LocationSetId]:
    In about:config
        Set browser.link.open_newwindow to 1

    Open Firefox developer tools

    On https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd
        Search for correct station number
        'Run Report' for 'Single Dat By Station'

    In Firefox Dev Tools Network tab:
        For Get request, RMB > Copy Value > Copy as cURl

    In VS Code:
        With cURL still in clipboard, in python > uncurl (pip install uncurl)
        Find parameters in output and save to repsective station info below    
'''
# %% Initialize
import sys
import os

from datetime import date #datetime, timedelta

# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilFuncs as utils
# pylint:enable=import-error, wrong-import-position

# %% Station IDs
stations = {
    '4001':{ # Lex Minuteman, 2020
        'ID': '4001',
        'TweetName': 'Minuteman Bikeway (Lexington)',
        'Description': 'Minuteman Commuter Bikeway',
        'Community': 'Lexington',
        'County': 'Middlesex',
        'Info': 'MHD owned: Q-Free Hi-Trac CMU',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Trail or Shared Use Path',
        'Qc Group': 'Basic QC',
        '_id': 'b923623a-dd05-4c2f-9c28-630d88c8dc9a',
        'ReportLocationSetId': 5341,
        'LocationSetId': 2789244,
        'FirstDate': date(2020, 8, 15),
        },
    '4004_SB': { # Medford Fellsway, 2021
        'ID': '4004_SB',
        'TweetName': 'Fellwsay Rt28 SB over Mystic River',
        'Description': 'Fellwsay (Route 28)',
        'Community': 'Medford',
        'County': 'Middlesex',
        'Info': 'MHD owned: Q-Free Hi-Trac CMU',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        '_id': 'beac5471-54d6-4d5e-a7ae-7153fd7b3957',
        'ReportLocationSetId': 5342,
        'LocationSetId': 2789381,
        'FirstDate': date(2021, 7, 14),
        },
    '4004_NB': { # Medford Fellsway, 2021
        'ID': '4004_NB',
        'TweetName': 'Fellwsay Rt28 NB over Mystic River',
        'Description': 'Fellwsay (Route 28)',
        'Community': 'Medford',
        'County': 'Middlesex',
        'Info': 'MHD owned: Q-Free Hi-Trac CMU',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        '_id': 'c1eecb32-0ac8-4779-8ceb-f54bdfb3dd23',
        'ReportLocationSetId': 5377,
        'LocationSetId': 2806069,
        'FirstDate': date(2021, 7, 13),
        },
    '4005': { # Arlington Minuteman, 2022
        'ID': '4005',
        'TweetName': 'Minuteman Bikeway (Arlington)',
        'Description': 'Minuteman Rail Trail',
        'Community': 'Arlington',
        'County': 'Middlesex',
        'Info': 'Arlington Minuteman Trail - Eco Counter MULTI, Mhd Owned',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Trail or Shared Use Path',
        'Qc Group': '',
        '_id': 'f8333dda-35bf-4f9c-aa92-4bcfb304fa1d',
        'ReportLocationSetId': '5400',
        'LocationSetId': '2814347',
        'FirstDate': date(2022, 9, 15),
        },
    '4006': { # Malden Northen Strand, 2022
        'ID': '4006',
        'TweetName': 'Northern Strand Trail (Malden)',
        'Description': 'Northern Strand Trail',
        'Community': 'Malden',
        'County': 'Middlesex',
        'Info': 'Malden Northern Strand Trail - Eco Counter MULTI - Mhd owned',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Trail or Shared Use Path',
        'Qc Group': '',
        '_id': 'aa676b65-2d1f-4a23-b02b-d70370a93e01',
        'ReportLocationSetId': '5398',
        'LocationSetId': '2814270',
        'FirstDate': date(2022, 9, 15),
        },
    }

# %% Save as pickle
utils.pickle_dict(stations, 'data/ms2soft_stations')

# %% Other Stations

stations_unused = {
        '3001': { # Bruse Freeman, 2021
        'ID': '3001',
        'Description': 'Bruce Freeman Rail Trail',
        'Community': 'Acton',
        'County': 'Middlesex',
        'Info': 'MHD Owned: CMU Device',
        'District': 3,
        'Area Type': 'Rural',
        'Functional Class': 'Trail or Shared Use Path',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5228,
        'LocationSetId': 2764341,
        },
        '4002_NB': { # Salem, 2021
        'ID': '4002_NB',
        'Description': 'Sgt James Ayube Memorial Drive',
        'Community': 'Salem',
        'County': 'Essex',
        'Info': 'MHD owned: Eco Counter',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5236,
        'LocationSetId': 2764364,
        },
    '4002_SB': { # Salem, 2021
        'ID': '4002_SB',
        'Description': 'Sgt James Ayube Memorial Drive',
        'Community': 'Salem',
        'County': 'Essex',
        'Info': 'MHD owned: Eco Counter',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5237,
        'LocationSetId': 2764366,
        },
    '4003_SB': { # Lowell, 2021
        'ID': '4003_SB',
        'Description': 'Bridge Street',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5232,
        'LocationSetId': 2764353,
        },
    '4003_NB': { # Lowell, 2021
        'ID': '4003_NB',
        'Description': 'Bridge Street',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5233,
        'LocationSetId': 2764356,
        },
    '4003_WB': { # Lowell, 2021
        'ID': '4003_NB',
        'Description': 'Veterans of Foreign Wars Highway',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5234,
        'LocationSetId': 2764359,
        },
    '4003_EB': { # Lowell, 2021
        'ID': '4003_EB',
        'Description': 'Veterans of Foreign Wars Highway',
        'Community': 'Lowell',
        'County': 'Middlesex',
        'Info': 'MHD owned: Miovision Data',
        'District': 4,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Default Group',
        'ReportLocationSetId': 5235,
        'LocationSetId': 2764362,
        },
        '5001_SB': { # Fall River, 2021
        'ID': '5001_SB',
        'Description': 'Brayton Ave',
        'Community': 'Fall River',
        'County': 'Bristol',
        'Info': 'Crosswalk Ped Counts - MHD Owned Miovision Data',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5238,
        'LocationSetId': 2764372,
        },
    '5001_WB': { # Fall River, 2021
        'ID': '5001_WB',
        'Description': 'Brayton Ave',
        'Community': 'Fall River',
        'County': 'Bristol',
        'Info': 'Quequechan River Rail Trail, Crossing at Brayton Ave - MHD Owned, Miovision integration sIte: TMC Int 8024292',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Minor Arterial',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5239,
        'LocationSetId': 2764376,
        },
    '5001_EB': { # Fall River, 2021
        'ID': '5001_EB',
        'Description': 'Brayton Ave',
        'Community': 'Fall River',
        'County': 'Bristol',
        'Info': 'Quequechan River Rail Trail, Crossing at Brayton Ave - MHD Owned, Miovision integration sIte: TMC Int 8024292',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Minor Arterial',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5240,
        'LocationSetId': 2764378,
        },
    '5002_WB': { # Brockton, 2021
        'ID': '5002_WB',
        'Description': 'Centre St',
        'Community': 'Brockton',
        'County': 'Plymouth',
        'Info': 'MHD Owned: Eco Counter',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5241,
        'LocationSetId': 2764381,
        },
    '5002_EB': { # Brockton, 2021
        'ID': '5002_EB',
        'Description': 'Centre St',
        'Community': 'Brockton',
        'County': 'Plymouth',
        'Info': 'MHD Owned: Eco Counter',
        'District': 5,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5242,
        'LocationSetId': 2764385,
        },
    '6002_WB': { # Cambridge Longfellow, n/a
        'ID': '6002_WB',
        'Description': 'Longfellow Bridge',
        'Community': 'Cambridge',
        'County': 'Suffolk',
        'Info': '',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5243,
        'LocationSetId': 2764387,
        },
    '6002_EB': { # Cambridge Longfellow, 2021/ped only
        'ID': '6002_EB',
        'Description': 'Longfellow Bridge',
        'Community': 'Cambridge',
        'County': 'Suffolk',
        'Info': 'Longfellow Bridge MHD owned:CMU Device',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': 'Principal Arterial – Other',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5245,
        'LocationSetId': 2764393,
        },
        '6003_EB': { # Cambridge Charles River Dam, 2021
        'ID': '6003_EB',
        'Description': 'Charles River Dam Rd',
        'Community': 'Boston',
        'County': 'Suffolk',
        'Info': 'MHD Owned: Eco Counter',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5246,
        'LocationSetId': 2764396,
        },
}

stations_removed = {
    '6001_EB': {
        'ID': '6001_EB',
        'Description': 'Charles River Dam Rd',
        'Community': 'Cambridge',
        'County': 'Middlesex',
        'Info': '',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5247,
        'LocationSetId': 2764399,
        },
    '6001_WB': {
        'ID': '6001_WB',
        'Description': 'Charles River Dam Rd',
        'Community': 'Cambridge',
        'County': 'Middlesex',
        'Info': '',
        'District': 6,
        'Area Type': 'Urban',
        'Functional Class': '',
        'Qc Group': 'Basic QC',
        'ReportLocationSetId': 5248,
        'LocationSetId': 2764401,
        },
}
