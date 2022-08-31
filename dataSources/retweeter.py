'''
Automatically retweet CambridgeCrash when the crash involves a cyclist.
'''
# %% Initialize
import sys
import os
import logging
import pickle

# Import custom modules
# pylint: disable=import-error, wrong-import-position
import utils.utilFuncs as utils
# pylint:enable=import-error, wrong-import-position

# pylint: disable=invalid-name, broad-except

# ?Add project folder to be able to import custom modules?
sys.path.insert(0,os.getcwd())

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# %% Accounts to retweet
# pylint: disable=missing-function-docstring
def CambridgeCrash(tweepyClient):
    username = 'CambridgeCrash'
    keyword = "cyclist"

    userInfo = tweepyClient.get_user(username=username)
    userID = userInfo.data.id

    logger.info('username: %s | userid: %s', username, userID)

    tweets = get_tweets(tweepyClient, userID)

    filteredTweets = filter_tweets(tweets, keyword)

    return filteredTweets

# %% Tweet Actions
def get_tweets(tweepyClient, userID):
    tweets = tweepyClient.get_users_tweets(userID)

    # logger.info(tweets)

    return tweets

def filter_tweets(tweets, keyword):
    filteredTweets = []

    for tweet in tweets.data:
        logger.debug(tweet)
        if keyword in tweet.text:
            filteredTweets.append(tweet.id)
            logger.debug(tweet)

    return filteredTweets

def retweet(tweepyClient, tweets):
    try:
        retweeted = loadRetweeted()
    except Exception as e:
        retweeted = []
        logger.error('Failed to load past retweets.', exc_info=e)

    for tweet in tweets:
        if tweet not in retweeted:
            try:
                tweepyClient.retweet(tweet)
                retweeted.append(tweet)
                logger.info('Retweeted %s', tweet)
            except Exception as e:
                logger.error('Failed to retweet %s.', tweet, exc_info=e)

    utils.pickle_dict(retweeted, 'data/retweeted_IDs')
    logger.info('Saved retweets.')


def loadRetweeted():
    path = os.getcwd()
    currentFolder = os.path.basename(path)
    logger.debug('cwd: %s', currentFolder)

    if currentFolder == 'dataSources':
        parent = os.path.dirname(path)
        dataFolder = parent + '/data'
    else:
        dataFolder = path + '/data'

    logger.debug('dataFolder: %s', dataFolder)
    logger.debug('dataFolder contents: %s', os.listdir(path=dataFolder))

    infile = open(dataFolder + '/retweeted_IDs.pkl', 'rb')
    retweetedIDs = pickle.load(infile)
    infile.close()

    return retweetedIDs

# %% Main
def main(tweepyClient):

    # Collect tweets to retweet
    filteredTweets = CambridgeCrash(tweepyClient)
    logger.info('Collected tweets from CambridgeCrash to retweet.')

    # Retweet tweets
    toRetweet = filteredTweets
    retweet(tweepyClient, toRetweet)

    logger.info('Completed retweeting for now')

# %% Run as Script
if __name__ == "__main__":
    # pylint: disable=ungrouped-imports
    import logging.config
    # logging.config.fileConfig(os.path.join( os.getcwd(), '..', 'log.conf'))
    logging.config.fileConfig('log.conf')
    logger = logging.getLogger(__name__)
    logger.debug("Logging is configured.")

    # pylint: disable=import-error
    from utils.configTwitterBot import create_client

    client = create_client()

    main(client)
