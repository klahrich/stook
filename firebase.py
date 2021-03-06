import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os

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
    cred = credentials.Certificate(os.environ['STOOK_SERVICE_ACCOUNT_FILE'])
    firebase_admin.initialize_app(cred)
