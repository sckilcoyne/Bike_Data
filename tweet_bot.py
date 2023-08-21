# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 20:21:23 2021

@author: Scott
"""
# %% Initialize
# pylint: disable=invalid-name, broad-except

# import tweepy
from datetime import datetime
import time

import os
# import sys
import logging
import logging.config

# pylint: disable=import-error
# from utils.configTwitterBot import create_client
import utils.configTwitterBot as configTwitter
import utils.config_mastodon as configMastodon
from utils import google_cloud
from dataSources import cambridge_totem as totem
from dataSources import ms2soft
from dataSources import retweeter
# pylint: enable=import-error

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logging.config.fileConfig('log.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

# Cloud storage location
bucket_name = os.getenv('GCS_BUCKET_NAME')
logger.info('bucket_name: %s', bucket_name)

# %% Functions


def sleep_till(timeSleep=8):
    """Sleep until given hour

    Args:
        timeSleep (int, optional): Hour to sleep until. Defaults to 8.
    """
    now = datetime.now()

    if now.hour > 8:
        day = now.day + 1
    else:
        day = now.day

    wakeTime = datetime(now.year, now.month, day, timeSleep, 0, 0)

    logger.info('Sleep from now (%s) until %s', now, wakeTime)
    time.sleep((wakeTime - now).seconds)


def sleep_time(timeSleep=1*60*60):
    """Sleep for given time.

    Default: 1 hour
    """
    now = datetime.now().strftime('%H:%M:%S')

    logger.info('Sleep for %s hours at %s', timeSleep/3600, now)
    time.sleep(timeSleep)


# %% Bot Functions
def create_API_clients():
    '''Create API clients to post
    '''
    logger.info('Creating API Clients')
    clientTwitter = create_twitter_client()
    clientMastodon = create_mastodon_client()

    return clientTwitter, clientMastodon

def create_twitter_client():
    '''Create twitter API
    '''
    logger.info('Attempting to connect to Twitter')
    try:
        clientTwitter = configTwitter.create_client()
        logger.info('Connected to Twitter')
    except Exception as e:
        logger.info('tweet_bot.create_twitter_client raised exception. Continue on...', exc_info=e)
        clientTwitter = None

    return clientTwitter

def create_mastodon_client():
    '''Create Mastodon API
    '''
    logger.info('Attempting to connect to Mastodon')
    try:
        clientMastodon = configMastodon.create_client()
        logger.info('Connected to Mastodon')
    except Exception as e:
        logger.info('tweet_bot.create_mastodon_client raised exception. Continue on...', exc_info=e)
        clientMastodon = None

    return clientMastodon

def make_post(post, clientTwitter, clientMastodon, service):
    '''Create posts for all services
    '''
    logger.info(post)
    logger.info('Service: %s', service)

    nopost_twitter = None
    nopost_mastodon = None

    # Twitter
    if service in ['all', 'twitter']:
        if clientTwitter is None:
            logger.info('Not connected to Twitter, trying to connect')
            clientTwitter = create_twitter_client()
        try:
            clientTwitter.create_tweet(text=post)
            logger.info('Tweeted')
        except Exception as e:
            clientTwitter = None
            nopost_twitter = post
            logger.info('tweet_bot.make_post(Twitter) raised exception. Continue on...',
                            exc_info=e)

    # Mastodon
    if service in ['all', 'mastodon']:
        if clientMastodon is None:
            logger.info('Not connected to Mastodon, trying to connect')
            clientMastodon = create_mastodon_client()
        try:
            clientMastodon.status_post(post)
            logger.info('Tooted')
        except Exception as e:
            clientMastodon = None
            nopost_mastodon = post
            logger.info('tweet_bot.make_post(Mastodon) raised exception. Continue on...',
                            exc_info=e)

    return clientTwitter, clientMastodon, nopost_twitter, nopost_mastodon

def make_posts(postList, clientTwitter, clientMastodon, service='all'):
    '''Create posts for each entry in list of posts

    postList: List with posts to be made
    service: Which post service to send to
        all: Try every posting serivce
        Other options: ['twitter', 'mastodon']
    '''
    retryTwitter = []
    retryMastodon = []

    for post in postList:
        clientTwitter, clientMastodon, nopost_twitter, nopost_mastodon = make_post(
                                                    post, clientTwitter, clientMastodon, service)
        if nopost_twitter is not None:
            retryTwitter.append(nopost_twitter)
        if nopost_mastodon is not None:
            retryMastodon.append(nopost_mastodon)
    
    return clientTwitter, clientMastodon, retryTwitter, retryMastodon

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

    # Make empty lists to retry posting
    retryTwitterR = []
    retryTwitterB = []
    retryTwitterM = []
    retryMastodonR = []
    retryMastodonB = []
    retryMastodonM = []

    # Continuously scrape new data and post updates
    while True:
        now = datetime.now().strftime('%H:%M:%S')
        logger.info('\n------------------------------------------\nLook for new data at %s', now)

        # Retry posting
        retryTwitterR = retryTwitterR + retryTwitterB + retryTwitterM
        retryMastodonR = retryMastodonR + retryMastodonB + retryMastodonM
        clientTwitter, _, retryTwitterR, _ = make_posts(
                                                retryTwitterR, clientTwitter, None, 'twitter')
        _, clientMastodon, _, retryMastodonR = make_posts(
                                                retryMastodonR, None, clientMastodon, 'mastodon')

        # Broadway totem
        try:
            postList, _, _, _, _ = totem.main()
            # postList, results_df, updateDaily, recordsNew = totem.main()

            if (postList is not None) and (len(postList) > 0):
                logger.info('Broadway totem Posts:')
                clientTwitter, clientMastodon, retryTwitterB, retryMastodonB = make_posts(
                                                    postList, clientTwitter, clientMastodon, 'all')

            else:
                logger.info('No new posts from Broadway totem (tweet_bot>main)')
        except Exception as e:
            logger.info('tweet_bot>totem.main() raised exception. Continue on...', exc_info=e)
            # pass

        # Mass Nonmotorized Database System (ms2soft)
        try:
            postList = ms2soft.main()

            if (postList is not None) and (len(postList) > 0):
                logger.info('NMDS-ms2soft Posts:')
                clientTwitter, clientMastodon, retryTwitterM, retryMastodonM = make_posts(
                                                    postList, clientTwitter, clientMastodon, 'all')
            else:
                logger.info('No new posts from NMDS-ms2soft (tweet_bot>main)')
        except Exception as e:
            logger.info('tweet_bot>ms2soft.main() raised exception. Continue on...', exc_info=e)


        # Retweet
        try:
            if clientTwitter is None:
                logger.info('Not connected to Twitter, trying to connect')
                clientTwitter = create_twitter_client()

            retweeter.main(clientTwitter)
        except Exception as e:
            logger.info('tweet_bot>retweeter.main() raised exception. Continue on...', exc_info=e)


        # Upload all modified files to google cloud
        try:
            google_cloud.main()
        except Exception as e:
            logger.info('tweet_bot>google_cloud.main() raised exception. Continue on...',
                            exc_info=e)

        # Time for a nap
        # Check for new data every hour between 8am and 8pm
        if  datetime.now().hour > 20:
            sleep_till()
        else:
            sleep_time()

if __name__ == "__main__":
    main()
