import base64
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import re
import dateparser
import logging
import argparse
import os
from google.cloud import pubsub_v1
from base_scraper import BaseScraper
from firebase import Firestore, firebase_init
import time
from random import uniform


class StockhouseScraper(BaseScraper):

    def __init__(self, exchange, stock, symbol, firestore, publisher, proxies):
        super().__init__('stockhouse', exchange, stock, firestore, publisher, proxies, slow=True)
        self.symbol = symbol
        self.baseurl = 'https://stockhouse.com/'

    def get_recent_posts(self):

        self.session = requests.Session()

        page = self.get_html(urljoin(self.baseurl, f'/companies/bullboard/{self.symbol}'))

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

    def get_post_body(self, post):
        page = self.get_html(post['url'])
        soup = BeautifulSoup(page.text, 'html.parser')
        post_body = soup.select_one('div.post-body div.post-content').get_text(strip=True)
        return post_body


def run(firestore, publisher, proxy):

    for exchange, stock, symbol in [('tsx', 'enb', 't.enb'), ('tsx', 'shop', 't.shop'), 
                                    ('cve', 'pwm', 'v.pwm'), ('cve', 'cre', 'v.cre'),
                                    ('tsx', 'well', 't.well'), ('tsx', 'lspd', 't.lspd')]:
        logging.info(f'Scraping {exchange}:{stock} from stockhouse')

        scraper = StockhouseScraper(exchange=exchange,
                                    stock=stock,
                                    symbol=symbol,
                                    firestore=firestore,
                                    publisher=publisher,
                                    proxy=proxy)

        scraper.run()
        time.sleep(uniform(5, 10))

