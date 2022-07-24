# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 19:32:44 2021

https://realpython.com/twitter-bot-python-tweepy/#deploying-bots-to-a-server-using-docker

@author: Scott
"""
# tweepy-bots/bots/config.py
import logging
import os
import sys
import tweepy

# logFormat = "%(levelname)s %(asctime)s - %(message)s"
# logFormat = "%(message)s"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
consoleStream = logging.StreamHandler(stream=sys.stdout)
# consoleStream.setFormatter(logFormat)
logger.addHandler(consoleStream)


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
        logging.error('Failed to properly load env variables')
        raise Exception('Env Variable Error')

    client = tweepy.Client(bearer_token=bearerToken,
                           consumer_key=consumerKey,
                           consumer_secret=consumerSecret,
                           access_token=accessToken,
                           access_token_secret=accessTokenSecret)

    try:
        user = client.get_user(username='bostonbikedata')
        logger.info(user)
        # print(user)
    except Exception as e:
        logger.error("Error creating API", exc_info=True)
        raise e
    logger.info("API created")
    return client


if __name__ == "__main__":
    create_client()
