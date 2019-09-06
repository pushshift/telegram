import logging
import sys
import time
from collections import defaultdict

import requests
import telethon
import ujson as json
from telethon import TelegramClient

import ingest
from common import config, db
from model import Message, Channel

telethon_api = TelegramClient('session', config["api_id"], config["api_hash"])
telethon_api.start()


def insert_messages_into_es(rows: list, action='index'):
    """This method bulk inserts Telegram messages into Elasticsearch"""
    records = []

    for record in rows:
        index = 'telegram'
        record_id = str(record['id'])
        bulk = defaultdict(dict)
        bulk[action]['_index'] = index
        bulk[action]['_id'] = record_id
        records.extend(list(map(lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False), [bulk, record])))

    headers = {'Accept': 'application/json', 'Content-type': 'application/json; charset=utf-8'}
    url = "http://localhost:9200/_bulk"
    records = '\n'.join(records) + "\n"
    records = records.encode('utf-8')
    response = requests.post(url, data=records, headers=headers)
    content = response.json()
    if content['errors']:
        sys.exit(response.content)  # TODO: Add proper Error Handling later (Raise custom error, etc.)
    if response.status_code != 200:
        sys.exit(response.text)


def ingest_all_messages(channel_name: str):
    BATCH_SIZE = 250
    current_message_id = None
    max_message_id = None
    min_message_id = None
    stop_flag = False
    channel_id = None

    if channel_name.lower() == 'bant4chan':
        current_message_id = 1149000

    while True:
        es_records = []
        logging.debug("Fetching {} ids (in descending order) from {} starting at id {}".format(BATCH_SIZE, channel_name,
                                                                                               current_message_id))
        messages = ingest.fetch_messages(telethon_api, channel_name, BATCH_SIZE, max_id=current_message_id)
        if len(messages) == 0:
            break
        retrieved_utc = int(time.time())
        rows = []

        for m in messages:
            message_id = m.id
            if current_message_id is None or message_id < current_message_id:
                current_message_id = message_id
            if min_message_id is None or message_id < min_message_id:
                min_message_id = message_id
            if max_message_id is None or message_id > max_message_id:
                max_message_id = message_id
            channel_id = m.to_id.channel_id
            record_id = (channel_id << 32) + message_id
            data = m.to_json()
            updated_utc = retrieved_utc
            es_records.append({
                'id': record_id,
                'channel_id': channel_id,
                'message_id': message_id,
                'message': m.message,
                'date': int(m.date.timestamp()),
                'via_bot_id': m.via_bot_id,
                'channel_name': channel_name,
                'grouped_id': m.grouped_id,
                'post_author': m.post_author,
                'post': m.post,
                'silent': m.silent,
                'retrieved_utc': retrieved_utc,
                'updated_utc': retrieved_utc
            })
            rows.append((record_id, message_id, channel_id, retrieved_utc, updated_utc, data))

            db.insert_message(Message(
                record_id=record_id,
                message_id=message_id,
                channel_id=channel_id,
                retrieved_utc=retrieved_utc,
                updated_utc=updated_utc,
                data=data,
            ))
        insert_messages_into_es(es_records)
        rows.clear()
        if stop_flag:
            break
        time.sleep(1)

    db.upsert_channel(Channel(
        channel_id=channel_id,
        channel_name=channel_name,
        retrieved_utc=retrieved_utc,
        min_message_id=min_message_id,
        max_message_id=max_message_id,
        is_active=True,
        is_complete=True,
    ))


# TODO close file
if __name__ == "__main__":
    channels = open("toxic.csv", "r").read().split("\n")
    channels = [channel for channel in channels if channel != '']

    for channel in channels:
        try:
            ingest_all_messages(channel)
        except telethon.errors.rpcerrorlist.UsernameNotOccupiedError:
            continue
