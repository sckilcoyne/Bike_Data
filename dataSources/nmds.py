'''
Download data from MassDOT Non-Motordized Database System (NMDS) through the MS2Soft site.


Origininally written by https://github.com/crschmidt
https://github.com/crschmidt/bikes/tree/main/mass_nmds
'''
# %% Initialize
import sys
import datetime
import time
import json
import logging
import requests
import pandas as pd
import numpy as np

sys.path.append("..") # Add parent directory to search path

cols_standard = ['StationID', 'StationName', 'Mode', 'DateTime', 'Count']
# StationID: Counter station code
# StationName: Station name used in posts
# Mode: Mode count is for e.g. bike/ped
# DateTime: datetime of count
# Count: Total count for the DateTime of the Mode
#   Counts for specific directions can be extra columns

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)

# %% Download Data


def get_all_paths():
    """Generate all possible paths.

    The service requires a pathfilter, but doesn't check the request against the 
    available paths for a particular counter. So we just generate all possible 
    paths to include.
    """
    path_filter = {'directions': [], 'offsets': []}
    directions = ['East', 'West', 'North', 'South',
                  'SouthWest', 'NorthWest', 'SouthEast', 'NorthEast',
                  'Unspecified']
    pathTypes = ["Bike Lane", "Crosswalk", "Roadway", "Sidewalk", "Bike Path", "Trail", "Combined"]
    for i in directions:
        path_filter['directions'].append(i)
        path_filter['offsets'].append({'direction': i, 'pathways': pathTypes})
    return path_filter


def fetch_site(site_id, site_name, date=datetime.date(2023, 1, 1)):
    '''Download data for a specific date and counter location

    Return: Dataframe [Station_ID, Mode, Count, Date, Time(Minute)]
    '''
    url = "https://mhd.ms2soft.com/tdms.ui/nmds/analysis/GetLocationCount"
    # User-Agent filtering is so silly.
    ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
    params = {
        'masterLocalId': site_id,
        'date': date.strftime("%m/%d/%Y"),
    }

    params['pathFiltersStr'] = json.dumps(get_all_paths())

    r = requests.post(url=url, data=params,
                      headers={'User-Agent': ua}, timeout=30)

    data = json.loads(r.content)
    trips = pd.DataFrame(columns=cols_standard)
    for i in data['countData']['IntervalCounts']:
        mode, count, minute = i['Mode'], i['Count'], i['IntervalStartTime']['TotalMinutes']
        t = datetime.time(hour=int(np.floor(minute/60)), minute=np.mod(minute, 60))
        dt = datetime.datetime.combine(date, t)
        trips.loc[len(trips)] = [site_id, site_name, mode, dt, count]
    return trips


def get_dates(counter_id):
    '''Get all dates a counter location has

    Return: Generator of all dates with data
    '''
    logger.info('Get all dates with counts for NMDS station %s', counter_id)
    url = "https://mhd.ms2soft.com/tdms.ui/nmds/analysis/GetLocationAttributes"
    # User-Agent filtering is so silly.
    ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
    params = {
        'masterLocalId': counter_id,
    }
    r = requests.post(url=url, data=params,
                      headers={'User-Agent': ua}, timeout=10)
    d = json.loads(r.content)
    dates = (x['DateFormatted'] for x in d['CountItems'])

    return dates

# %% Test Functions


def run():
    '''Get data for a default counter and date
    '''
    print(fetch_site("4005", datetime.date(2023, 8, 10)))


def run_all(date=datetime.date(2023, 8, 10)):
    '''Download data for all counters in counter file for a given date

    Print collected data, no output
    '''
    d = json.load(open("dailycounters.json", encoding="utf8"))
    for i in d:
        print(i[1], fetch_site(i[0], date))

# %% Main


def main(counterInfo, downloadedDates=None):
    '''Download data from full list of counters for all dates they have data

    Save data for each counter location in a labeled file

    downloadedDates: List of dates already downloaded.
    '''
    counterID = counterInfo[0]
    counterName = counterInfo[1]
    # print(counterID)
    dataDates = get_dates(counterID)

    newdata = pd.DataFrame(columns=cols_standard)

    logger.info('Downloading data for NMDS station %s', counterID)
    if downloadedDates is None:
        logNote = f'No datelist given, downloading all dates with counts for counter {counterID}'
        logger.info(logNote)
        for d in dataDates:
            date = datetime.datetime.strptime(d, "%m/%d/%Y").date()
            data = fetch_site(counterID, counterName, date)

            newdata = pd.concat([newdata, data], ignore_index=True)
            logger.info('Downloaded %s', d)
            time.sleep(1) # Don't download from NMDS too fast
    else:
        downloadedDatesStr = [datetime.datetime.strftime(d, "%m/%d/%Y") for d in downloadedDates]
        for d in dataDates:
            # For each date NMDS has data, check if already downloaded. If not, download the data.
            if d not in downloadedDatesStr:
                date = datetime.datetime.strptime(d, "%m/%d/%Y").date()
                data = fetch_site(counterID, counterName, date)

                newdata = pd.concat([newdata, data], ignore_index=True)
                logger.info('Downloaded %s', d)
                time.sleep(1) # Don't download from NMDS too fast

    if newdata.shape[0] == 0:
        newdata = None

    return newdata


# %% Script
if __name__ == "__main__":
    # pylint: disable=ungrouped-imports
    import logging.config
    logging.config.fileConfig(fname='log.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    logger.setLevel('DEBUG')
    logger.debug("Logging is configured.")

    yesterday = datetime.datetime.now()-datetime.timedelta(days=2)

    counter_list = json.load(open("settings/counters_nmds.json", encoding="utf8"))

    # run_all(yesterday)
    # fetch_all_dates()
    # run()

    dateGenerator = get_dates('4005')
    for x in dateGenerator:
        print(x)

    # for x in list(dateGenerator):
    #     print(x)
