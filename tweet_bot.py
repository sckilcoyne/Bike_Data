# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 20:21:23 2021

@author: Scott
"""
# %% Initialize
# import tweepy
from datetime import datetime
import time

# import os
import sys
import logging

from utils.configTwitterBot import create_client
from dataSources import cambridge_totem as totem


# logFormat = "%(levelname)s %(asctime)s - %(message)s"
# logFormat = "%(message)s"
# logging.basicConfig(stream=sys.stdout,
#                     level=logging.INFO,
#                     format=logFormat)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
consoleStream = logging.StreamHandler(stream=sys.stdout)
# consoleStream.setFormatter(logFormat)
logger.addHandler(consoleStream)

# %% Functions


def sleep_till(timeSleep):
    """[summary]

    Args:
        timeSleep ([type]): [description]
    """
    now = datetime.now()


def testing_sleep():
    """[summary]
    """
    now = datetime.now().strftime('%H:%M:%S')

    logging.info('Test loop | %s', now)
    time.sleep(20)


# %% Bot Functions
def totem_broadway():
    """[summary]
    """
    try:
        tweetList = totem.main()
        return tweetList
    except:
        logging.info('No tweets')


# %% Bot


def main():
    """[summary]
    """
    client = create_client()

    while True:

        tweetList, results_df, updateDaily, recordsNew = totem_broadway()

        if tweetList is not None:
            print('Tweet list:')
            print(tweetList)

            print('Tweets:')
            for tweet in tweetList:
                logging.info(tweet)
                print(tweet)
                client.create_tweet(text=tweet)
        else:
            logging.info('No tweets')
        

        testing_sleep()


if __name__ == "__main__":
    main()
