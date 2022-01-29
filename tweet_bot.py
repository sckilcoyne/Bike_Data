# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 20:21:23 2021

@author: Scott
"""
# %% Initialize
import tweepy
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

    now = datetime.now()


def test_docker():
    while True:
        # now = datetime.now()
        # sleep_till()
        now = datetime.now().strftime('%H:%M:%S')

        logging.info('Test loop | ' + now)
        time.sleep(5)


# %% Bot Functions
def totem_broadway():
    totem.main()

# %% Bot


def main():
    # client = create_client()

    test_docker()

    # totem_broadway()
    # since_id = 1
    # while True:
    #     since_id = check_mentions(client, ["help", "support"], since_id)
    #     logger.info("Waiting...")
    #     time.sleep(60)


if __name__ == "__main__":
    main()
