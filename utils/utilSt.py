'''Share Common Data between pages

'''

# %% Initialize

# %% Station Info

dataSources =  {'Broadway - Cambridge': {
                        'FileName': 'broadway',
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
                    'Fellsway NB - Medford':{
                        'FileName': '4004_NB',
                        'Directions': ['Total'], # Only show total for 1 direction
                        'Lat': 42.4008675,
                        'Long': -71.08302,
                        'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=4004_NB',
                        },
                    'Fellsway SB - Medford':{
                        'FileName': '4004_SB',
                        'Directions': ['Total'], # Only show total for 1 direction
                        'Lat': 42.4009323,
                        'Long': -71.083374,
                        'url': 'https://mhd.ms2soft.com/tdms.ui/nmds/analysis/Index?loc=mhd&LocationId=4004_SB',
                        },
                    }