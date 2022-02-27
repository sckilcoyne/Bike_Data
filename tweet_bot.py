# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 20:21:23 2021

@author: Scott
"""
# %% Initialize
# import tweepy
from datetime import datetime
import time

import os
import sys
import logging

from utils.configTwitterBot import create_client
from dataSources import cambridge_totem as totem


# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger()

logFormat = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format=logFormat)
logger = logging.getLogger()

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

    logging.info('Test loop | ' + now)
    time.sleep(10)


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

        tweetList = totem_broadway()

        if tweetList is not None:
            for tweet in tweetList:
                logging.info(tweet)
                client.create_tweet(text=tweet)
        else:
            logging.info('No tweets')
        

        testing_sleep()


if __name__ == "__main__":
    main()
