from twitter_scraper import run as twitter_run
from stockhouse_scraper import run as stockhouse_run
import logging


if __name__ == '__main__':
    
    firebase_init()

    firestore = Firestore()

    publisher = pubsub_v1.PublisherClient.from_service_account_file('C:\\Users\\klahrichi\\.ssh\\stook.json')

    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s', 
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    stockhouse_run(firestore, publisher)
    twitter_run(firestore, publisher
