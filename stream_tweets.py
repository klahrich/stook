import tweepy
import argparse
import yaml
from google.cloud import pubsub_v1


class MyStreamListener(tweepy.StreamListener):

    def __init__(self, api, publisher, topic_path, follow_screen_names):
        self.api = api
        self.publisher = publisher
        self.topic_path = topic_path
        self.follow_screen_names = follow_screen_names

    def on_status(self, status):
        if hasattr(status, 'full_text'):
            text = status.full_text
        else:
            text = status.text

        data = text.encode('utf-8')
        author = status.author.name
        screen_name = status.author.screen_name
        timestamp = status.created_at.strftime("%d-%b-%Y (%H:%M:%S)")

        if screen_name in self.follow_screen_names:
            print('tweet found:')
            print(data)
            self.publisher.publish(self.topic_path,
                                   data=data,
                                   author=author.encode('utf-8'),
                                   screen_name=screen_name.encode('utf-8'),
                                   timestamp=timestamp.encode('utf-8'),
                                   source='twitter')


# if PLATFORM.upper() == 'GCP':
#     cred = credentials.ApplicationDefault()
#     firebase_admin.initialize_app(cred, {
#         'projectId': 'stook-706cc',
#     })
#     db = firestore.client()
# elif PLATFORM.upper() == 'LOCAL':
#     cred = credentials.Certificate('C:\\Users\\klahrichi\\.ssh\\stook.json')
#     firebase_admin.initialize_app(cred)
#     db = firestore.client()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--env-file', default='env.yaml')
    parser.add_argument('--twitter-file', default='twitter.yaml')
    args = parser.parse_args()

    env_file = args.env_file
    twitter_file = args.twitter_file

    with open(env_file, 'r') as f:
        env = yaml.load(f, Loader=yaml.FullLoader)

    with open(twitter_file, 'r') as f:
        twitter_ids = yaml.load(f, Loader=yaml.FullLoader)

    auth = tweepy.OAuthHandler(env['TWITTER_CONSUMER_KEY'], env['TWITTER_CONSUMER_SECRET'])
    auth.set_access_token(env['TWITTER_TOKEN_KEY'], env['TWITTER_TOKEN_SECRET'])

    api = tweepy.API(auth)

    project_id = "stook-706cc"
    publisher = pubsub_v1.PublisherClient.from_service_account_file('C:\\Users\\klahrichi\\.ssh\\stook.json')

    listeners = []

    for stock_symbol, insiders in twitter_ids.items():
        follow_ids = [str(insider_info['id']) for insider_info in insiders.values()]
        follow_screen_names = [str(screen_name) for screen_name in insiders.keys()]
        topic_name = stock_symbol
        topic_path = publisher.topic_path(project_id, topic_name)
        pubsub_producer = MyStreamListener(api, publisher, topic_path, follow_screen_names)
        twitterStream = tweepy.Stream(auth, listener=pubsub_producer)
        twitterStream.filter(follow=follow_ids)



