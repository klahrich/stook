# -*- coding: utf-8 -*-
"""
Created on Sun May  3 21:23:38 2020

@author: klahrichi
"""

from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import re
import dateparser
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class StockhouseScraper:

    def __init__(self, exchange, stock, symbol, firestore, publisher):
        self.exchange = exchange
        self.stock = stock
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

        timestamps = [dateparser.parse(t).strftime('%Y-%m-%d %H-%M') for t in time_stripped]

        page_results = [{'title': t, 'author': a, 'timestamp': ts,
                         'url': urljoin(self.baseurl, h),
                         'exchange': self.exchange, 'stock': self.stock, 'source': 'stockhouse'}
                        for t,a,ts,h in zip(titles, authors, timestamps, hrefs)]

        return page_results

    def get_post_body(self, url):
        post_page = self.session.get(url)
        soup = BeautifulSoup(post_page.text)
        post_body = soup.select_one('div.post-body div.post-content').get_text(strip=True)
        return post_body

    def process_front_page(self):
        posts = self.get_recent_posts()
        most_recent_post_saved = list(self.firestore.get_recent('stockhouse',
                                                                self.stock,
                                                                limit=1))

        for p in posts:
            # WARNING: this algo might be fragile
            if ((most_recent_post_saved is not None) and
                (len(most_recent_post_saved) > 0) and
                (p['timestamp'] < most_recent_post_saved[0]['timestamp'])):
                break
            post = dict(**p, body=self.get_post_body(p['url']))
            self.firestore.insert(post)
            #self.publisher.publish(post)


class Firestore:

    def __init__(self):
        # Use a service account
        self.collection = firestore.client().collection('stocks')

    def get_recent(self, source, stock, limit=None):
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
    cred = credentials.Certificate('C:\\Users\\klahrichi\\.ssh\\stook.json')
    firebase_admin.initialize_app(cred)

#---- TEST 1
fs = Firestore()

sample_post = {
    'source': 'test-source',
    'stock': 'test-stock',
    'exchange': 'test-exchange',
    'title': 'test-title',
    'author': 'test-author',
    'timestamp': '2020-05-06 08-20',
    'body': 'test-body',
    'url': 'test-url'
}

fs.insert(sample_post)

res = list(fs.get_recent(source='test-source', stock='test-stock'))

#---- TEST 2

scraper = StockhouseScraper(exchange='tsx',
                            stock='pwm',
                            symbol='v.pwm',
                            firestore=fs,
                            publisher=None)

scraper.process_front_page()
