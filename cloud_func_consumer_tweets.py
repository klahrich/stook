# requirements
# discord.py>=1.3.3

import base64
import os
from discord import Webhook, RequestsWebhookAdapter

def consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    WEBHOOK_ID = int(os.environ['WEBHOOK_ID'])
    WEBHOOK_TOKEN = os.environ['WEBHOOK_TOKEN']

    webhook = Webhook.partial(WEBHOOK_ID, WEBHOOK_TOKEN, adapter=RequestsWebhookAdapter())

    message = base64.b64decode(event['data']).decode('utf-8')
    attrs = event['attributes']

    author = attrs['author']
    screen_name = attrs['screen_name']
    timestamp = attrs['timestamp']

    if not message.startswith('RT '):
         final_msg = timestamp + ': ' + '**' + author + '**' + ' tweeted:\n' + message
         webhook.send(final_msg)
