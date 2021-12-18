# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 20:21:23 2021

@author: Scott
"""
# %% Initialize
import tweepy
import logging

from config import create_client
from utils import cambridge_totem as totem


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# %% Functions


def totem_broadway():
    totem.main()


def main():
    client = create_client()

    totem_broadway()
    # since_id = 1
    # while True:
    #     since_id = check_mentions(client, ["help", "support"], since_id)
    #     logger.info("Waiting...")
    #     time.sleep(60)


if __name__ == "__main__":
    main()
