from twitter_scraper import run as twitter_run
from stockhouse_scraper import run as stockhouse_run
import logging
from firebase import Firestore, firebase_init
from google.cloud import pubsub_v1
import os
import argparse


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--proxy', default=None)

    args = parser.parse_args()
    proxy = args.proxy

    firebase_init()

    firestore = Firestore()

    stook_service_account = os.environ['STOOK_SERVICE_ACCOUNT_FILE']
    publisher = pubsub_v1.PublisherClient.from_service_account_file(stook_service_account)

    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s', 
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    stockhouse_run(firestore, publisher, proxy)
    #twitter_run(firestore, publisher
