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

# import os
# import sys
import logging
import logging.config

# pylint: disable=import-error
from utils.configTwitterBot import create_client
from dataSources import cambridge_totem as totem
# pylint: enable=import-error

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logging.config.fileConfig('log.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

# %% Functions


# def sleep_till(timeSleep):
#     """[summary]

#     Args:
#         timeSleep ([type]): [description]
#     """
#     now = datetime.now()


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
    """[summary]
    """
    # Create client to Twitter API
    client = create_client()

    # Continuously scrape new data and tweet updates
    while True:

        # Broadway totem
        try:
            tweetList, _, _, _ = totem.main()
            # tweetList, results_df, updateDaily, recordsNew = totem.main()

            if tweetList is not None:
                logger.info('Tweets:')
                for tweet in tweetList:
                    logger.info(tweet)
                    client.create_tweet(text=tweet)
            else:
                logger.info('No new tweets from Broadway totem (tweet_bot>main)')
        except Exception as e:
            logger.info('tweet_bot>totem.main() raised exception. Continue on...', exc_info=e)
            # pass

        sleep_time()




if __name__ == "__main__":
    main()
