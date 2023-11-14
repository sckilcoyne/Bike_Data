'''Share Common Data between pages

'''

# %% Initialize

# %% Station Info
# pylint: disable=line-too-long

dataSources =  {'Broadway - Cambridge': {
                    'FileName': 'q8v9-mcfg',
                    'Directions': ['Total', 'Westbound', 'Eastbound'],
                    'Lat': 42.36335,
                    'Long': -71.085896,
                    'url': 'https://data.cambridgema.gov/Transportation-Planning/Eco-Totem-Broadway-Bicycle-Count/q8v9-mcfg',
                    },
                'Minuteman - Arlington': {
                    'FileName': '4005',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.41443,
                    'Long': -71.15233,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=4005',
                    },
                'Minuteman - Lexington':{
                    'FileName': '4001',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.4612961,
                    'Long': -71.23842,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=4001',
                    },
                'Northen Strand - Malden':{
                    'FileName': '4006',
                    'Directions': ['Total', 'Westbound', 'Eastbound'],
                    'Lat': 42.42938,
                    'Long': -71.05714,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=4006',
                    },
                'Mass Ave Bridge NB':{
                    'FileName': '6004_NB',
                    'Directions': ['Total'], # Only show total for 1 direction
                    'Lat': 42.35459000,
                    'Long': -71.09126000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=6004_NB',
                    },
                'Mass Ave Bridge SB':{
                    'FileName': '6004_SB',
                    'Directions': ['Total'], # Only show total for 1 direction
                    'Lat': 42.35446550,
                    'Long': -71.09138490,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=6004_SB',
                    },
                'BU Bridge NB':{
                    'FileName': '6005_NB',
                    'Directions': ['Total'], # Only show total for 1 direction
                    'Lat': 42.35324000,
                    'Long': -71.11046000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=6005_NB',
                    },
                'BU Bridge SB':{
                    'FileName': '6005_SB',
                    'Directions': ['Total'], # Only show total for 1 direction
                    'Lat': 42.35325000,
                    'Long': -71.11061000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=6005_SB',
                    },
                'Wellington Bridge (Fellsway Rte 28) NB':{
                    'FileName': '4004_NB',
                    'Directions': ['Total'], # Only show total for 1 direction
                    'Lat': 42.40086750,
                    'Long': -71.08302000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=4004_NB',
                    },
                'Wellington Bridge (Fellsway Rte 28) SB':{
                    'FileName': '4004_SB',
                    'Directions': ['Total'], # Only show total for 1 direction
                    'Lat': 42.40093230,
                    'Long': -71.08337400,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=4004_SB',
                    },
                'SouthwestCorridor, W Newton St':{
                    'FileName': 'ST_DCR_003',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.34428000,
                    'Long': -71.07985000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=ST_DCR_003',
                    },
                'SouthwestCorridor, Prentiss St':{
                    'FileName': 'ST_DCR_006',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.33382000,
                    'Long': -71.09267000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=ST_DCR_006',
                    },
                'SouthwestCorridor, Roxbury Crossing Station':{
                    'FileName': 'ST_DCR_002',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.33107000,
                    'Long': -71.09515000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=ST_DCR_002',
                    },
                'SouthwestCorridor, Jackson Square Station':{
                    'FileName': 'ST_DCR_004',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.32391000,
                    'Long': -71.09954000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=ST_DCR_004',
                    },
                'SouthwestCorridor, Stonybrook Station':{
                    'FileName': 'ST_DCR_005',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.31753000,
                    'Long': -71.10445000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=ST_DCR_005',
                    },
                'SouthwestCorridor, Williams St':{
                    'FileName': 'ST_DCR_001',
                    'Directions': ['Total', 'Northbound', 'Southbound'],
                    'Lat': 42.30756000,
                    'Long': -71.10911000,
                    'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=ST_DCR_001',
                    },
                }