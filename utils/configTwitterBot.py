# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 19:32:44 2021

https://realpython.com/twitter-bot-python-tweepy/#deploying-bots-to-a-server-using-docker

@author: Scott
"""
# %% Intialize
# pylint: disable=invalid-name

# tweepy-bots/bots/config.py
import logging
import os
# import sys
import tweepy

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# %% Creat Client
def create_client():
    '''Create Twitter API client
    '''
    CONSUMER_KEY = os.getenv('TWTR_CONSUMER_KEY')
    CONSUMER_SECRET = os.getenv('TWTR_CONSUMER_SECRET')
    ACCESS_TOKEN = os.getenv('TWTR_ACCESS_TOKEN')
    ACCESS_TOKEN_SECRET = os.getenv('TWTR_ACCESS_TOKEN_SECRET')
    BEARER_TOKEN = os.getenv('TWTR_BEARER_TOKEN')

    if None in [CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, BEARER_TOKEN]:
        logger.error('Failed to properly load Twitter env variables')
        raise ValueError('Env Variable Error')

    logger.info('Twitter consumer key: %s', CONSUMER_KEY)

    client = tweepy.Client(consumer_key=CONSUMER_KEY,
                           consumer_secret=CONSUMER_SECRET,
                           access_token=ACCESS_TOKEN,
                           access_token_secret=ACCESS_TOKEN_SECRET)

    # try:
    #     user = client.get_user(username='bostonbikedata')
    #     logger.info(user)
    # except Exception as e:
    #     logger.error("Error creating Twitter API", exc_info=True)
    #     raise e
    logger.info("Twitter API client created")
    return client

# %% Run as Script
if __name__ == "__main__":
    # pylint: disable=ungrouped-imports
    import logging.config
    # logging.config.fileConfig(os.path.join( os.getcwd(), '..', 'log.conf'))
    logging.config.fileConfig('log.conf')
    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")
    create_client()
