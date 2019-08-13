from telethon import TelegramClient, events, sync
import ujson as json
import yaml
import time
import ingest


with open("config.yaml",'r') as stream:
    config = yaml.safe_load(stream)
    api_id = config['api_id']
    api_hash = config['api_hash']

telethon_api = TelegramClient('session', api_id, api_hash)
client.start()

messages = ingest.fetch_messages(telethon_api,"washingtonpost", 10, max_id=5)
