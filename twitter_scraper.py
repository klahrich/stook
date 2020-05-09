import yaml
import tweepy
import argparse
import logging
from google.cloud import pubsub_v1
from base_scraper import BaseScraper
from firebase import Firestore, firebase_init
import re


class TwitterScraper(BaseScraper):

    def __init__(self, exchange, stock, firestore, publisher, api, twitter_users, regex_filter_positive):
        super().__init__('twitter', exchange, stock, firestore, publisher)
        self.api = api
        self.twitter_users = twitter_users
        self.regex_filter_positive = re.compile(regex_filter_positive, re.IGNORECASE) if regex_filter_positive is not None else None

    def get_recent_posts(self):
        def get_text(t):
            return t.full_text if hasattr(t, 'full_text') else t.text
               
        user_tweets = [self.api.user_timeline('@' + user, tweet_mode='extended') for user in self.twitter_users]

        res =  [{'text': get_text(t), 'author': t.author.name, 'timestamp': t.created_at.strftime('%Y-%m-%d %H-%M'),
                 'date': t.created_at.strftime('%Y-%m-%d'), 'title': '', 'url': '', 
                 'source': 'twitter', 'exchange': self.exchange, 'stock': self.stock} for tweets in user_tweets for t in tweets]

        res = [r for r in res 
               if (not r['text'].startswith('RE '))  
               and (self.regex_filter_positive is None or self.regex_filter_positive.search(r['text']))]

    def get_post_body(self, post):
        return post['text']


def run(firestore, publisher):
    parser = argparse.ArgumentParser()

    parser.add_argument('--twitter-env', default='twitter-env.yaml')
    parser.add_argument('--twitter-ids', default='twitter-ids.yaml')
    args = parser.parse_args()

    twitter_env_f = args.twitter_env
    twitter_ids_f = args.twitter_ids

    with open(twitter_ids_f, 'r') as f:
        twitter_ids = yaml.load(f, Loader=yaml.FullLoader)

    auth = tweepy.OAuthHandler(os.environ['TWITTER_CONSUMER_KEY'], os.environ['TWITTER_CONSUMER_SECRET'])
    auth.set_access_token(os.environ['TWITTER_TOKEN_KEY'], os.environ['TWITTER_TOKEN_SECRET'])

    api = tweepy.API(auth)

    for exchange, stocks in twitter_ids.items():
        for stock, info in stocks.items():
            logging.info(f'Scraping {exchange}:{stock} from twitter')
            scraper = TwitterScraper(exchange, stock, firestore, publisher, api, info['users'], info.get('regex_filter_positive', None))
            scraper.run()


    
        


