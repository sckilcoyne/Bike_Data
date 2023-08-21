# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 20:21:23 2021

@author: Scott
"""
# %% Initialize
# pylint: disable=invalid-name, broad-except

from datetime import datetime
import time

import os
# import sys
from pathlib import Path
import logging
import logging.config
import json
import pickle
import pandas as pd
# import numpy as np
# pylint: disable=import-error
import utils.configTwitterBot as configTwitter
import utils.config_mastodon as configMastodon
from utils import google_cloud
import utils.data_analysis as da
from dataSources import cambridge_odp as codp
from dataSources import nmds
from dataSources import retweeter
# pylint: enable=import-error


cols_standard = ['StationID', 'StationName', 'Mode', 'DateTime', 'Count']
# StationID: Counter station code
# StationName: Station name used in posts
# Mode: Mode count is for e.g. bike/ped
# DateTime: datetime of count
# Count: Total count for the DateTime of the Mode
#   Counts for specific directions can be extra columns


# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logging.config.fileConfig('log.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

bucket_name = os.getenv('GCS_BUCKET_NAME')
logger.info('bucket_name: %s', bucket_name)

# %% Settings
settings = json.load(open("bot_settings.json", encoding="utf8"))

# Hours to run features
START_POSTING = settings['PostStart_Bot']
START_CODP = settings['PostStart_NMDS']
START_NMDS = settings['PostStart_CODP']
END_POSTING = settings['PostEnd_Bot']

# Limit how many posts to make at a time
BURST_LIMIT = settings['PostBurstLimit']

# Limit how many times per day to retry
RETRY_CODP = settings['RetryLimit_NMDS']
RETRY_NMDS = settings['RetryLimit_CODP']


# %% Bot sleep functions

def sleep_time(wakeTime=START_POSTING):
    """Sleep until given hour

    Args:
        wakeTime (int, optional): Hour to sleep until. Defaults to 8.
    """
    now = datetime.now()

    if now.hour > wakeTime:
        day = now.day + 1
    else:
        day = now.day

    wakeTime = datetime(now.year, now.month, day, wakeTime, 0, 0)

    logger.info('Sleep from now (%s) until %s', now, wakeTime)
    time.sleep((wakeTime - now).seconds)

def nap_time(timeSleep=1*60*60):
    """Sleep for given time.

    Default: 1 hour
    """
    now = datetime.now().strftime('%H:%M:%S')

    logger.info('Sleep for %s hours at %s', timeSleep/3600, now)
    time.sleep(timeSleep)


# %% API Creation Functions
def create_API_clients():
    '''Create API clients to post
    '''
    clientTwitter = create_twitter_client
    clientMastodon = create_mastodon_client

    return clientTwitter, clientMastodon

def create_twitter_client():
    '''Create twitter API
    '''
    try:
        clientTwitter = configTwitter.create_client()
        logger.info('Connected to Twitter')
    except Exception as e:
        logger.info('bot.create_twitter_client raised exception. Continue on...', exc_info=e)
        clientTwitter = None

    return clientTwitter

def create_mastodon_client():
    '''Create Mastodon API
    '''
    try:
        clientMastodon = configMastodon.create_client()
        logger.info('Connected to Mastodon')
    except Exception as e:
        logger.info('bot.create_mastodon_client raised exception. Continue on...', exc_info=e)
        clientMastodon = None

    return clientMastodon

# %% Posting
def make_post(post, clientTwitter, clientMastodon):
    '''Create posts for all services
    '''
    logger.info(post)

    if clientTwitter is None:
        clientTwitter = create_twitter_client
    if clientMastodon is None:
        clientMastodon = create_mastodon_client

    try:
        clientTwitter.create_tweet(text=post)
    except Exception as e:
        logger.info('bot.make_post(Twitter) raised exception. Continue on...', exc_info=e)

    try:
        clientMastodon.status_post(post)
    except Exception as e:
        logger.info('bot.make_post(Mastodon) raised exception. Continue on...', exc_info=e)

    return clientTwitter, clientMastodon

def make_posts(postList, clientTwitter, clientMastodon):
    '''Create posts for a list posts on all services
    '''

    for _ in range(BURST_LIMIT):
        # Only create so many posts at a time, remove created posts from list of posts to be made
        clientTwitter, clientMastodon = make_post(postList[0], clientTwitter, clientMastodon)
        del postList[0]

    # for p in list(postList): # Iterate over copy of list to remove from list along way
        # clientTwitter, clientMastodon = make_post(p, clientTwitter, clientMastodon)
        # postList.remove(p)

    return postList, clientTwitter, clientMastodon


# %% Bot


def main():
    """
    All of the functions for the Posting bot.

    Perform each task then take a nap for a little before repeating.

    Totem: Fetch and Calculate daily riders on Boradway in Cambridge, recorded by the Eco-Totem.
    Retweeter: Automatically retweet CambridgeCrash when the crash involves a cyclist.
    """
    # Create API clients to post
    clientTwitter, clientMastodon = create_API_clients()

    retryCODP = RETRY_CODP
    retryNMDS = RETRY_NMDS

    # Continuously scrape new data and post updates
    while True:

        today = datetime.datetime.today().date()
        pastweek = [today - datetime.timedelta(days=x+1) for x in range(7)]

        filename_postlist = 'data/postlist.pkl'
        if Path(filename_postlist).is_file():
            with open(filename_postlist, 'rb') as f:
                postlist = pickle.load(f)
            # postlist = pd.read_pickle(filename_postlist)
        else:
            postlist = pd.Series()

        # Cambridge Open Data Portal
        if (datetime.now().hour > START_CODP) & (retryCODP > 0):
            retryCODP = retryCODP - 1

            # Load list of counters
            countersCODP = json.load(open("dataSources/codp_counters.json", encoding="utf8"))

            for c in countersCODP:
                try:
                    # Load full and daily datasets of counter, create empty df if it doesn't exsist
                    filename_full = f'data/{c[0]}_full.pkl'
                    filename_daily = f'data/{c[0]}_daily.pkl'
                    if Path(filename_full).is_file():
                        cdf_full = pd.read_pickle(filename_full)
                    else:
                        cdf_full = pd.DataFrame(columns=cols_standard)
                    if Path(filename_daily).is_file():
                        cdf_daily = pd.read_pickle(filename_daily)
                    else:
                        cdf_daily = pd.DataFrame(columns=cols_standard)

                    # Create numpy array of dates with data
                    datadates = cdf_full['DateTime'].dt.date
                    # datadates = datadates.unique()

                    # Create a list of dates in the past week without data
                    datelist = set(pastweek) - set(datadates)

                    # Download data from missing dates
                    newcdf_full = codp.main(c, datelist)

                    # Update the daily count dataframe
                    newcdf_daily = da.daily_counts(newcdf_full)

                    # Add new data to exsisting data
                    cdf_full = pd.concat([cdf_full, newcdf_full], ignore_index=True)
                    cdf_daily = pd.concat([cdf_daily, newcdf_daily], ignore_index=True)

                    # Create post list from new data
                    postlist = da.new_posts(postlist, cdf_daily, newcdf_daily, c)

                    # Save data to file
                    cdf_full.to_pickle(filename_full, protocol=3)
                    cdf_daily.to_pickle(filename_daily, protocol=3)

                except Exception as e:
                    logger.info('bot>codp raised exception. Continue on...', exc_info=e)

        # Mass Nonmotorized Database System
        if (datetime.now().hour > START_NMDS) & (retryNMDS > 0):
            retryNMDS = retryNMDS - 1

            # Load list of counters
            countersNMDS = json.load(open("dataSources/nmds_counters.json", encoding="utf8"))

            for c in countersNMDS:
                try:
                    # Load full and daily datasets of counter, create empty df if it doesn't exsist
                    filename_full = f'data/{c[0]}_full.pkl'
                    filename_daily = f'data/{c[0]}_daily.pkl'
                    if Path(filename_full).is_file():
                        cdf_full = pd.read_pickle(filename_full)
                    else:
                        cdf_full = pd.DataFrame(columns=cols_standard)
                    if Path(filename_daily).is_file():
                        cdf_daily = pd.read_pickle(filename_daily)
                    else:
                        cdf_daily = pd.DataFrame(columns=cols_standard)

                    # Create numpy array of dates with data
                    datadates = cdf_full['DateTime'].dt.date
                    # datadates = datadates.unique()

                    # Create a list of dates in the past week without data
                    datelist = set(pastweek) - set(datadates)

                    # Download data from missing dates
                    newcdf_full = nmds.main(c, datelist)

                    # Update the daily count dataframe
                    newcdf_daily = da.daily_counts(newcdf_full)

                    # Add new data to exsisting data
                    cdf_full = pd.concat([cdf_full, newcdf_full], ignore_index=True)
                    cdf_daily = pd.concat([cdf_daily, newcdf_daily], ignore_index=True)

                    # Create post list from new data
                    postlist = da.new_posts(postlist, cdf_daily, newcdf_daily, c)

                    # Save data to file
                    cdf_full.to_pickle(filename_full, protocol=3)
                    cdf_daily.to_pickle(filename_daily, protocol=3)

                except Exception as e:
                    logger.info('bot>nmds raised exception. Continue on...', exc_info=e)

        # Make postings
        try:
            if (postlist is not None) and (len(postlist) > 0):
                logger.info('New Posts:')
                postList, clientTwitter, clientMastodon = make_posts(postlist,
                                                                     clientTwitter, clientMastodon)

                with open(filename_postlist, 'wb') as f:
                    pickle.dump(postList, f)

        except Exception as e:
            logger.info('bot>make_posts raised exception. Continue on...', exc_info=e)

        # Retweet
        try:
            retweeter.main(clientTwitter)
        except Exception as e:
            logger.info('bot>retweeter.main() raised exception. Continue on...', exc_info=e)


        # Upload all modified files to google cloud
        try:
            google_cloud.main()
        except Exception as e:
            logger.info('bot>google_cloud.main()  raised exception. Continue on...', exc_info=e)

        # Time for a nap
        # Check for new data every hour until end of posting for the day
        if  datetime.now().hour > END_POSTING:
            sleep_time()
            retryCODP = RETRY_CODP
            retryNMDS = RETRY_NMDS
        else:
            nap_time()

if __name__ == "__main__":
    main()
