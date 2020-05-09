import time
import logging
from random import uniform
import sys
import os
import requests


PROJECT_ID = "stook-706cc"


class BaseScraper:

    def __init__(self, source, exchange, stock, firestore, publisher, proxy=None, slow=False):
        self.source = source
        self.exchange = exchange
        self.stock =    stock
        self.firestore = firestore
        self.session = None
        self.publisher = publisher
        self.topic_path = topic_path = publisher.topic_path(PROJECT_ID, 'stocks') 
        self.slow = slow
        self.proxy = {'https': proxy}

    def get_html(self, url):
        return requests.get(url, proxies=self.proxy)

    def run(self):
        posts = self.get_recent_posts()

        assert(posts is not None and len(posts) > 0)

        most_recent_post_saved = self.firestore.get_recent_posts(self.source,
                                                                 self.stock,
                                                                 limit=1)

        most_recent_post_saved = list(most_recent_post_saved)

        def already_seen(p):
            cond1 = (p['timestamp'] < most_recent_post_saved[0].to_dict()['timestamp'])
            cond2 = (p['timestamp'] == most_recent_post_saved[0].to_dict()['timestamp'])
            cond3 = (p['author'] == most_recent_post_saved[0].to_dict()['author'])
            cond4 = (p['title'] == most_recent_post_saved[0].to_dict()['title'])
            return cond1 or (cond2 and cond3 and cond4)

        i = 0
        for p in sorted(posts, key=lambda post: post['timestamp']):
            # WARNING: this algo might be fragile
            if ((most_recent_post_saved is not None) and
                (len(most_recent_post_saved) > 0) and
                already_seen(p)):
                continue
            post = dict(**p, body=self.get_post_body(p))
            self.firestore.insert(post)
            i += 1
            self.publisher.publish(self.topic_path.encode('utf-8'),
                                   data=post['body'].encode('utf-8'),
                                   title=post['title'].encode('utf-8'),
                                   timestamp=post['timestamp'].encode('utf-8'),
                                   exchange=self.exchange.encode('utf-8'),
                                   stock=self.stock.encode('utf-8'),
                                   author=post['author'].encode('utf-8'),
                                   source=self.source.encode('utf-8'))

            if self.slow:
                time.sleep(uniform(5, 10))

        if i > 0:
            logging.info(f'{self.source} - {self.stock} - Found {i} new posts.')
        else:
            logging.info(f'{self.source} - {self.stock} - No new posts found.')