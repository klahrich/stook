import base64
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import re
import dateparser
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import logging
import argparse
import os


class StockhouseScraper:

    def __init__(self, exchange, stock, symbol, firestore, publisher):
        self.source = 'stockhouse'
        self.exchange = exchange
        self.stock =    stock
        self.symbol = symbol
        self.firestore = firestore
        self.publisher = publisher
        self.session = None
        self.baseurl = 'https://stockhouse.com/'

    def get_recent_posts(self):

        self.session = requests.Session()

        page = self.session.get(urljoin(self.baseurl, f'/companies/bullboard/{self.stock}'))

        soup = BeautifulSoup(page.text, 'html.parser')

        posts = soup.find_all("div", class_="post-header")

        links = [p.find("a") for p in posts]

        hrefs = [l['href'] for l in links if l['href'].startswith('/companies')]

        titles = [l.find(text=True) for l in links if l['href'].startswith('/companies')]

        post_meta = [d.find('span') for d in soup.find_all('div', class_='post-info-user')]

        authors = [pm.find('a').get_text() for pm in post_meta]

        time = [pm.get_text(strip=True) for pm in post_meta]

        time_stripped = [re.search(r'posted\b(.*\b(am|pm))\b', t, re.IGNORECASE).group(1).strip()
                         for t in time]

        dates = [dateparser.parse(t).strftime('%Y-%m-%d') for t in time_stripped]
        timestamps = [dateparser.parse(t).strftime('%Y-%m-%d %H-%M') for t in time_stripped]

        page_results = [{'title': t, 'author': a, 'timestamp': ts, 'date': d,
                         'url': urljoin(self.baseurl, h),
                         'exchange': self.exchange, 'stock': self.stock, 'source': 'stockhouse'}
                        for t,a,ts,d,h in zip(titles, authors, timestamps, dates, hrefs)]

        return page_results

    def get_post_body(self, url):
        post_page = self.session.get(url)
        soup = BeautifulSoup(post_page.text, 'html.parser')
        post_body = soup.select_one('div.post-body div.post-content').get_text(strip=True)
        return post_body

    def run(self):
        posts = self.get_recent_posts()

        assert(len(posts) > 0)

        most_recent_post_saved = self.firestore.get_recent_posts(self.source,
                                                                 self.stock,
                                                                 limit=1)

        most_recent_post_saved = list(most_recent_post_saved)

        # ATTENTION: posts are assumed to be sorted in descending order of timestamp
        i = 0
        for p in posts:
            # WARNING: this algo might be fragile
            if ((most_recent_post_saved is not None) and
                (len(most_recent_post_saved) > 0) and
                (p['timestamp'] < most_recent_post_saved[0].to_dict()['timestamp'])):
                break
            post = dict(**p, body=self.get_post_body(p['url']))
            self.firestore.insert(post)
            i += 1
            #self.publisher.publish(post)

        if i > 0:
            logging.info(f'Found {i} new posts.')
        else:
            logging.info(f'No new posts found.')


class Firestore:

    def __init__(self):
        # Use a service account
        self.collection = firestore.client().collection('stocks')

    def get_recent_posts(self, source, stock, limit=None):
        latest = self.collection.where(
            u'source', u'==', source
        ).where(
            u'stock', u'==', stock
        ).order_by(
            u'timestamp',
            direction=firestore.Query.DESCENDING
        )

        if limit is not None:
            latest = latest.limit(limit)

        return latest.stream()

    def insert(self, post):
        self.collection.add(post)


def firebase_init():
    cred = credentials.Certificate('/home/ubuntu/stook.json')
    #cred = credentials.Certificate('C:\\Users\\klahrichi\\.ssh\\stook.json')
    firebase_admin.initialize_app(cred)
    #firebase_admin.initialize_app()


def run(event=None, context=None):
    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s', 
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    firebase_init()

    fs = Firestore()

    scraper = StockhouseScraper(exchange='tsx',
                                stock='enb',
                                symbol='t.enb',
                                firestore=fs,
                                publisher=None)

    scraper.run()


if __name__ == '__main__':
    run()
