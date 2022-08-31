# import tweepy

# pylint: disable=import-error
from configTwitterBot import create_client

testTweet = 'This is line 1\nThis is line 2'

def main():
    """[summary]
    """
    client = create_client()

    client.create_tweet(text=testTweet)


if __name__ == "__main__":
    main()
