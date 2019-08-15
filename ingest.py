from telethon import TelegramClient, events, sync
import ujson as json
import logging
import time

def translate_message_for_es(message, channel_name, retrieved_utc):
    '''Process and prepare a message object for inclusion to Elastic. This method essentially
    handles all the more intricate and nasty translations that need to take place so that each
    message can be indexed within Elasticsearch with minimal issues. Since this translation
    could take up many lines of code, it's best to have it as its own method.'''

    es_record = {}
    es_record['channel_id'] = message.to_id.channel_id
    es_record['message_id'] = message.id
    es_record['id'] = (es_record['channel_id'] << 32) + es_record['message_id']
    es_record['message'] = message.message
    es_record['date'] = int(message.date.timestamp())
    es_record['via_bot_id'] = message.via_bot_id
    es_record['channel_name'] = channel_name
    es_record['grouped_id'] = message.grouped_id
    es_record['post_author'] = message.post_author
    es_record['post'] = message.post
    es_record['silent'] = message.silent
    es_record['retrieved_utc'] = retrieved_utc
    es_record['updated_utc'] = retrieved_utc
    return es_record


def fetch_messages(client, channel, size=100, max_id=None, min_id=None):
    '''Method to fetch messages from a specific channel / group'''

    #logging.debug("Fetching up to {} messages from channel {}".format(size,channel))
    params = [channel,size]
    kwargs = {}

    # The telethon module has issues if a keyword passed is None, so we will add the keyword
    # only if it is not None
    for key in ['max_id', 'min_id']:
        if locals()[key] is not None:
            kwargs[key] = locals()[key]

    data = client.get_messages(*params,**kwargs)

    return data
