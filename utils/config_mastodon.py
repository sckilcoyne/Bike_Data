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
apiInfoFile = 'masto_clientcred.secret'

mastoPass = os.getenv('MASTO_PASSWORD')

# %% Functions

def create_app():
    '''Register app and create api file
    '''
    Mastodon.create_app(
        'pytooterapp',
        api_base_url = apiBaseURL,
        to_file = apiInfoFile
    )

def login():
    '''Log into Mastodon
    '''
    mastodon = Mastodon(
        client_id = apiInfoFile,
        api_base_url = apiBaseURL
    )
    mastodon.log_in(
        'bostonbikedata@gmail.com', mastoPass,
        to_file = apiInfoFile
    )

def create_api():
    '''Create API instance
    '''
    mastodon = Mastodon(
        access_token = 'pytooter_usercred.secret',
        api_base_url = 'https://mastodon.social'
    )

    return mastodon

def create_post(mastodon):
    '''Publish post to Mastodon
    '''
    mastodon.toot('Tooting from Python using #mastodonpy !')
