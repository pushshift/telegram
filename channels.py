from telethon import TelegramClient, events, sync
import ujson as json
import time
import sys

# example: get_channel_info(client, 'followchris')
# parameters:
# client: the TelegramClient from Telethon
# channel: the channel_id or the channel_username
# output: Dictionary with the channel info. 
# In case that the channel parameter is not valid we return an error dict
def get_channel_info(client, channel):
    try:
        return client.get_entity(channel).to_dict()
    except ValueError as e:
        return {'error': str(e)}