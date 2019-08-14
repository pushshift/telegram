from telethon import TelegramClient, events, sync, errors
import sys
import ujson as json
import yaml
import time
import psycopg2
import ingest
import logging
import requests
from collections import defaultdict
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)


with open("config.yaml",'r') as stream:
    config = yaml.safe_load(stream)
    api_id = config['api_id']
    api_hash = config['api_hash']
    db_password = config['db_password']

conn = psycopg2.connect("dbname='telegram' user='postgres' host='localhost' password='{}'".format(db_password))
telethon_api = TelegramClient('session', api_id, api_hash)
telethon_api.start()


def insert_messages_into_es(rows: list, action='index'):
    '''This method bulk inserts Telegram messages into Elasticsearch'''
    records = []

    for record in rows:

        index = 'telegram'
        id = str(record['id'])
        bulk = defaultdict(dict)
        bulk[action]['_index'] = index
        bulk[action]['_id'] = id
        records.extend(list(map(lambda x: json.dumps(x,sort_keys=True,ensure_ascii=False), [bulk, record])))

    headers = {'Accept': 'application/json', 'Content-type': 'application/json; charset=utf-8'}
    url = "http://localhost:9200/_bulk"
    records = '\n'.join(records) + "\n"
    records = records.encode('utf-8')
    response = requests.post(url, data=records, headers=headers)
    content = response.json()
    if content['errors']:
        sys.exit(response.content) ###### Add proper Error Handling later (Raise custom error, etc.)
    if response.status_code != 200:
        sys.exit(response.text)


def get_channel(db_handle, channel_id=None, channel_name=None):
    '''Get current channel status by channel id'''

    cur = db_handle.cursor()
    if channel_name is None:
        cur.execute("SELECT * FROM channel_status WHERE channel_id = %s",(channel_id))
    else:
        cur.execute("SELECT * FROM channel WHERE LOWER(name) = %s",(channel_name.lower(),))
    data = cur.fetchone()
    return data


def update_channel(db_handle, channel_id, channel_name, retrieved_utc, min_message_id, max_message_id, is_active=True, is_complete=False):
    '''Upsert function for channel data. This will need to be broken into separate functions later.'''

    cur = db_handle.cursor()
    updated_utc = int(time.time())
    cur.execute('''INSERT INTO channel (id, name, retrieved_utc, updated_utc, min_message_id, max_message_id, is_complete, is_active) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET max_message_id=EXCLUDED.max_message_id, min_message_id=EXCLUDED.min_message_id, 
                updated_utc=EXCLUDED.updated_utc, is_complete=EXCLUDED.is_complete''', (channel_id, 
                                                                                        channel_name,
                                                                                        retrieved_utc,
                                                                                        updated_utc,
                                                                                        min_message_id,
                                                                                        max_message_id,
                                                                                        is_complete, 
                                                                                        is_active))
    db_handle.commit()
    cur.close()


def bulk_insert(db_handle, rows: list):
    '''Method to bulk insert messages into Postgresql'''

    cur = db_handle.cursor()

    if rows:
        sql = "INSERT INTO message (id,message_id, channel_id, retrieved_utc, updated_utc, data) VALUES {} ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data"
        args_str = b','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", x) for x in rows)
        sql = sql.format(args_str.decode("utf-8"))
        cur.execute(sql)
        db_handle.commit()
        cur.close()

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
        logging.debug("Fetching {} ids (in descending order) from {} starting at id {}".format(BATCH_SIZE, channel_name, current_message_id))
        messages = ingest.fetch_messages(telethon_api, channel_name, BATCH_SIZE, max_id=current_message_id)
        if len(messages) == 0:
            break
        retrieved_utc = int(time.time())
        rows = []
        channel_updated = False

        for m in messages:
            message_id = m.id
            if current_message_id is None or message_id < current_message_id:
                current_message_id = message_id
            if min_message_id is None or message_id < min_message_id:
                min_message_id = message_id
            if max_message_id is None or message_id > max_message_id:
                max_message_id = message_id
            channel_id = m.to_id.channel_id
            id = (channel_id << 32) + message_id
            data = m.to_json()
            updated_utc = retrieved_utc
            es_record = {}
            es_record['id'] = id
            es_record['channel_id'] = channel_id
            es_record['message_id'] = message_id
            es_record['message'] = m.message
            es_record['date'] = int(m.date.timestamp())
            es_record['via_bot_id'] = m.via_bot_id
            es_record['channel_name'] = channel_name
            es_record['grouped_id'] = m.grouped_id
            es_record['post_author'] = m.post_author
            es_record['post'] = m.post
            es_record['silent'] = m.silent
            es_record['retrieved_utc'] = retrieved_utc
            es_record['updated_utc'] = retrieved_utc
            es_records.append(es_record)
            rows.append((id, message_id, channel_id, retrieved_utc, updated_utc, data))

            # Add channel to channel table
            #if not channel_updated:
            #    update_channel(conn, channel_id, channel, retrieved_utc)
            #    channel_updated = True
        insert_messages_into_es(es_records)
        bulk_insert(conn, rows)
        rows.clear()
        if stop_flag:
            break
        time.sleep(1)

    update_channel(conn, channel_id, channel_name, retrieved_utc, min_message_id, max_message_id, is_active=True, is_complete=True)


channels = open("toxic.csv","r").read().split("\n")
channels = [channel for channel in channels if channel != '']

for channel in channels:
    try:
        ingest_all_messages(channel)
    except telethon.errors.rpcerrorlist.UsernameNotOccupiedError:
        continue
