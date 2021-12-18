# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 19:32:44 2021

https://realpython.com/twitter-bot-python-tweepy/#deploying-bots-to-a-server-using-docker

@author: Scott
"""
# tweepy-bots/bots/config.py
import tweepy
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def create_client():
    consumerKey = os.getenv('CONSUMER_KEY')
    consumerSecret = os.getenv('CONSUMER_SECRET')
    accessToken = os.getenv('ACCESS_TOKEN')
    accessTokenSecret = os.getenv('ACCESS_TOKEN_SECRET')
    bearerToken = os.getenv('BEARER_TOKEN')

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
