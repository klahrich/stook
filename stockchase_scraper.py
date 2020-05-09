import base64
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import re
import dateparser
import logging
import argparse
import os
from base_scraper import BaseScraper
from firebase import Firestore, firebase_init
import time
from random import uniform


class StockchaseScraper(BaseScraper):

    def __init__(self, exchange, stock, stockid, symbol, firestore, publisher):
        super().__init__('stockchase', exchange, stock, firestore, publisher, proxies, slow=True)
        self.stockid = stockid
        self.symbol = symbol
        self.baseurl = 'https://stockchase.com/'

    def get_recent_posts(self):

        page = self.get_html(urljoin(self.baseurl, f'/company/view/{self.stockid}/{self.symbol}'))

        soup = BeautifulSoup(page.text, 'html.parser')

        posts = soup.find_all(class_="opinions-row")

        links = [p.find("a") for p in posts]

        hrefs = [l['href'] for l in links if l['href'].startswith('/company')]

        titles = [p.find(class_='opinion-mini__signal-badge').get_text().strip() for p in posts]

        authors = [p.find(class_='expert-name').get_text().strip() for p in posts]

        time = [p.find(class_='opinion-mini__date').get_text().strip() for p in posts]

        dates = [dateparser.parse(t).strftime('%Y-%m-%d') for t in time]
        timestamps = dates

        texts = [p.find(class_='opinion-comment').get_text().strip() for p in posts]

        page_results = [{'text':txt, 'title': t, 'author': a, 'timestamp': ts, 'date': d,
                         'url': urljoin(self.baseurl, h),
                         'exchange': self.exchange, 'stock': self.stock, 'source': 'stockchase'}
                        for txt,t,a,ts,d,h in zip(texts, titles, authors, timestamps, dates, hrefs)]

        return page_results

    def get_post_body(self, post):
        return post['text']


def run(firestore, publisher, proxy):

    for exchange, stock, symbol, stockid in [('tsx', 'enb', 'enb-t', '430'), 
                                            ('tsx', 'shop', 'shop-t', '5093'), 
                                            ('tsx', 'lspd', 'lspd-t', '6014')]:
        logging.info(f'Scraping {exchange}:{stock} from stockhouse')
        
        scraper = StockchaseScraper(exchange=exchange,
                                    stock=stock,
                                    symbol=symbol,
                                    stockid=stockid,
                                    firestore=firestore,
                                    publisher=publisher,
                                    proxy=proxy)

        scraper.run()
        time.sleep(uniform(5, 10))


if __name__ == '__main__':
    firebase_init()

    firestore = Firestore()

    publisher = pubsub_v1.PublisherClient.from_service_account_file('C:\\Users\\klahrichi\\.ssh\\stook.json')

    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s', 
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    run(firestore, publisher)