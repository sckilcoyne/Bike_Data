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
    """[summary]

    Raises:
        e: [description]

    Returns:
        [type]: [description]
    """
    consumerKey = os.getenv('CONSUMER_KEY')
    consumerSecret = os.getenv('CONSUMER_SECRET')
    accessToken = os.getenv('ACCESS_TOKEN')
    accessTokenSecret = os.getenv('ACCESS_TOKEN_SECRET')
    bearerToken = os.getenv('BEARER_TOKEN')

    if None in [consumerKey, consumerSecret, accessToken, accessTokenSecret, bearerToken]:
        logger.error('Failed to properly load env variables')
        raise Exception('Env Variable Error')

    client = tweepy.Client(bearer_token=bearerToken,
                           consumer_key=consumerKey,
                           consumer_secret=consumerSecret,
                           access_token=accessToken,
                           access_token_secret=accessTokenSecret)

    try:
        user = client.get_user(username='bostonbikedata')
        logger.info(user)
    except Exception as e:
        logger.error("Error creating API", exc_info=True)
        raise e
    logger.info("API created")
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
