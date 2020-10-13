'''Reads from twitter's API'''

import sqlalchemy
import os
import tweepy
from dotenv import load_dotenv
import time
import json

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")

TWITTER_AUTH = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
TWITTER_AUTH.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# def select_data(tweet):


class Listener(tweepy.StreamListener):
    '''This class handles the tweet stream'''
    def __init__(self):
        super(Listener, self).__init__()
        self.hose_drinker = {}
        self.counter = 0

    def on_status(self, status):
        print('status text ', status.text)

    def on_error(self, status_code):
        print(status_code)
        return False

    def on_data(self, data):
        tweet = json.loads(data)
        # print('----------NEW TWEET-----------')
        # for key, value in tweet.items():
        #     print('----------------')
        #     print(f'{key}: {value}')
        if self.counter == 2:
            return False
        if 'RT @' not in tweet['text']: 
            if not tweet['truncated']:
                text = tweet['text']
            else:
                text = tweet['extended_tweet']['full_text']
            document = {
                'id': tweet['id'],
                'text': text,
                'userid': tweet['user']['id'],
                'username': tweet['user']['screen_name'],
                'timestamp': tweet['timestamp_ms'],
            }
            self.hose_drinker[self.counter] = document
            self.counter += 1
        return True

stream = Listener()
myStream = tweepy.Stream(auth = TWITTER_AUTH, listener=stream)
myStream.filter(track=['#streaming', '#stream', 'live stream', 
                        'stream', 'streaming'], 
                languages=['en'], )

for value in stream.hose_drinker.values():
    # print('equality to false: ', value['truncated']==False)
    print(value)
    # print('Retweeted: ', value['retweeted'], '\n')