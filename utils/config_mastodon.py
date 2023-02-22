'''
Connect to Mastodon to create posts

https://mastodonpy.readthedocs.io/en/stable/#app-registration-and-user-authentication

Before you can use the Mastodon API, you have to register your application (which gets you a client
key and client secret) and then log in (which gets you an access token).

'''
# %% Initialize
import logging
import os

from mastodon import Mastodon

# pylint: disable=invalid-name

# Set up logging
# https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# Connection info
apiBaseURL = 'https://better.boston'
apiInfoFile = 'secrets/masto_clientcred.secret'

# mastoPass = os.getenv('MASTO_PASSWORD')
mastoKey = os.getenv('MAST_CLIENT_KEY')
mastoSecret = os.getenv('MAST_CLIENT_SECRET')
mastoToken = os.getenv('MAST_ACCESS_TOKEN')

# %% Functions


def create_app():
    '''Register app and create api file

    Only needs to be run once
    '''
    Mastodon.create_app(
        'bikedataapp',
        api_base_url=apiBaseURL,
        to_file=apiInfoFile
    )


def create_client():
    '''Create Mastodon API instance
    '''
    # print(f'{mastoKey=}')
    # print(f'{mastoSecret=}')
    # print(f'{mastoToken=}')
    # print(f'{apiBaseURL=}')

    if None in [mastoKey, mastoSecret, mastoToken, apiBaseURL]:
        logger.error('Failed to properly load Mastodon env variables')
        raise Exception('Env Variable Error')

    logger.info('Mastodon key: %s', mastoKey)

    mastodon = Mastodon(
        client_id=mastoKey,
        client_secret=mastoSecret,
        access_token=mastoToken,
        api_base_url=apiBaseURL,
    )

    logger.info('Mastodon API client created:\n %s', mastodon)

    return mastodon

# %% Run Script

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv(dotenv_path='secrets/bikebot-test.env')
    mastoKey = os.getenv('MAST_CLIENT_KEY')
    mastoSecret = os.getenv('MAST_CLIENT_SECRET')
    mastoToken = os.getenv('MAST_ACCESS_TOKEN')

    mastoAPI = create_client()

    mastoAPI.status_post('Testing!', visibility='unlisted')

    # print(mastoAPI)
