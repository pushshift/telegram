import logging
import time

import telethon
from telethon import TelegramClient

import ingest
from common import config, db, es
from model import Message, Channel

# TODO: Wrap in its own file
telethon_api = TelegramClient('session', config["api_id"], config["api_hash"])
telethon_api.start()


def ingest_all_messages(channel_name: str):
    BATCH_SIZE = 250
    current_message_id = None
    max_message_id = None
    min_message_id = None
    stop_flag = False
    channel_id = None

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
        es.bulk_insert(es_records)
        rows.clear()
        if stop_flag:
            break
        time.sleep(1)  # TODO: rate limit decorator

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
