from telethon import TelegramClient, events, sync
import ujson as json
import yaml
import time


with open("config.yaml",'r') as stream:
    config = yaml.safe_load(stream)
    api_id = config['api_id']
    api_hash = config['api_hash']

client = TelegramClient('session', api_id, api_hash)
client.start()
