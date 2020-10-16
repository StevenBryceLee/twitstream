'''Reads from twitter's API and writes to google pubsub'''

import os
import tweepy
from dotenv import load_dotenv
import json
from google.cloud import pubsub_v1


# Load and fix Twitter API secrets from environment
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")

# Configure google pubsub publisher
project_name = os.getenv("PROJECT_NAME")
topic_name = os.getenv("PROJECT_TOPIC")

class Listener(tweepy.StreamListener):
    '''
    This class handles the tweet stream and loads selected fields to PubSub
    '''
    def __init__(self, publisher, topic_path):
        super(Listener, self).__init__()
        self.hose_drinker = {}
        self.counter = 0
        self.publisher = publisher
        self.topic_path = topic_path

    def on_status(self, status):
        print('status text: ', status.text)

    def on_error(self, status_code):
        print('Error code: ', status_code)
        return False

    def on_data(self, data):
        tweet = json.loads(data)
        # if self.counter == 2:
        #     return False
        if 'RT @' not in tweet['text']: 
            self.pubsub_push(self.select_data(tweet), self.publisher, self.topic_path)
            self.counter += 1
            # self.hose_drinker[self.counter] = data
            if self.counter % 20 == 0:
                print(self.counter)
        return True

    def select_data(self, tweet):
        '''
        Obtain relevant data from the tweet JSON recieved from the StreamListener
        tweet is a JSON dictionary. 
        See the Twitter JSON documentation for all fields
        '''
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
        return document

    def pubsub_push(self, document, publisher, topic_path):
        '''
        Write a document to pubsub
        document is a dictionary
        publisher is an instance of PublisherClient() from the pubsub package
        topic_path is the topic path method of the previous publisher instance
        '''
        publisher.publish(topic_path, data = json.dumps(document).encode('utf-8'))


# if __name__ == '__main__':
TWITTER_AUTH = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
TWITTER_AUTH.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_name, topic_name)

stream = Listener(publisher, topic_path)
myStream = tweepy.Stream(auth = TWITTER_AUTH, listener=stream)
myStream.filter(track=['#streaming', '#stream', 'live stream', 
                        'stream', 'streaming'], 
                languages=['en'], )

for value in stream.hose_drinker.values():
    # print('equality to false: ', value['truncated']==False)
    print(value)
    # print('Retweeted: ', value['retweeted'], '\n')