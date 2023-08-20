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
import logging
import logging.config
import json
# pylint: disable=import-error
import utils.configTwitterBot as configTwitter
import utils.config_mastodon as configMastodon
from utils import google_cloud
from dataSources import cambridge_odp as codp
# from dataSources import ms2soft
from dataSources import nmds
from dataSources import retweeter
# pylint: enable=import-error

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logging.config.fileConfig('log.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

bucket_name = os.getenv('GCS_BUCKET_NAME')
logger.info('bucket_name: %s', bucket_name)

# %% Settings

# Hours to run features
START_POSTING = 8
START_CODP = 12
START_NMDS = 12
END_POSTING = 18

# Limit how many times per day to retry
retryCODPinit = 3
retryNMDSinit = 3


# %% Bot sleep functions

def sleep_till(wakeTime=START_POSTING):
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

def sleep_time(timeSleep=1*60*60):
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

    for p in postList:
        clientTwitter, clientMastodon = make_post(p, clientTwitter, clientMastodon)

    return clientTwitter, clientMastodon


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

    retryCODP = retryCODPinit
    retryNMDS = retryNMDSinit

    # Continuously scrape new data and post updates
    while True:

        # Cambridge Open Data Portal
        if (datetime.now().hour > START_CODP) & (retryCODP > 0):
            retryCODP = retryCODP - 1
            try:
                postList, _, _, _ = codp.main()
                # postList, results_df, updateDaily, recordsNew = totem.main()

                if (postList is not None) and (len(postList) > 0):
                    logger.info('Broadway totem Posts:')
                    clientTwitter, clientMastodon = make_posts(postList, clientTwitter, clientMastodon)

                else:
                    logger.info('No new posts from Broadway totem (bot>main)')
            except Exception as e:
                logger.info('bot>totem.main() raised exception. Continue on...', exc_info=e)
                # pass

        # Mass Nonmotorized Database System (ms2soft)
        if (datetime.now().hour > START_NMDS) & (retryNMDS > 0):
            retryNMDS = retryNMDS - 1
            try:
                postList = nmds.main()

                if (postList is not None) and (len(postList) > 0):
                    logger.info('NMDS-ms2soft Posts:')
                    clientTwitter, clientMastodon = make_posts(postList, clientTwitter, clientMastodon)
                else:
                    logger.info('No new posts from NMDS-ms2soft (bot>main)')
            except Exception as e:
                logger.info('bot>ms2soft.main() raised exception. Continue on...', exc_info=e)


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
            sleep_till()
            retryCODP = retryCODPinit
            retryNMDS = retryNMDSinit
        else:
            sleep_time()

if __name__ == "__main__":
    main()