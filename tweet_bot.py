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

bucket_name = os.getenv('GCS_BUCKET_NAME')
logger.info('bucket_name: %s', bucket_name)

# %% Functions


def sleep_till(timeSleep=8):
    """_summary_

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


# %% Bot


def main():
    """
    All of the functions for the Posting bot.

    Perform each task then take a nap for a little before repeating.

    Totem: Fetch and Calculate daily riders on Boradway in Cambridge, recorded by the Eco-Totem.
    Retweeter: Automatically retweet CambridgeCrash when the crash involves a cyclist.
    """
    # Create API clients to post
    clientTwitter = configTwitter.create_client()
    clientMastodon = configMastodon.create_client()

    # Continuously scrape new data and post updates
    while True:

        # Broadway totem
        try:
            postList, _, _, _, _ = totem.main()
            # postList, results_df, updateDaily, recordsNew = totem.main()

            if (postList is not None) and (len(postList) > 0):
                logger.info('Broadway totem Posts:')
                for post in postList:
                    logger.info(post)
                    clientTwitter.create_tweet(text=post)
                    clientMastodon.status_post(post)
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
                for post in postList:
                    logger.info(post)
                    clientTwitter.create_tweet(text=post)
                    clientMastodon.status_post(post)
            else:
                logger.info('No new posts from NMDS-ms2soft (tweet_bot>main)')
        except Exception as e:
            logger.info('tweet_bot>ms2soft.main() raised exception. Continue on...', exc_info=e)


        # Retweet
        try:
            retweeter.main(clientTwitter)
        except Exception as e:
            logger.info('tweet_bot>retweeter.main() raised exception. Continue on...', exc_info=e)


        # Upload all modified files to google cloud
        try:
            google_cloud.main()
        except Exception as e:
            logger.info('tweet_bot>google_cloud.main()  raised exception. Continue on...', exc_info=e)

        # Time for a nap
        # Check for new data every hour between 8am and 8pm
        if  datetime.now().hour > 20:
            sleep_till()
        else:
            sleep_time()

if __name__ == "__main__":
    main()
