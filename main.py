from telethon import TelegramClient, events, sync, errors
import sys
import ujson as json
import yaml
import time
import psycopg2
import ingest
import channels
import logging
import requests
import db
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


def insert_messages_into_pg(db_handle, rows: list):
    '''Method to bulk insert messages into Postgresql'''

    cur = db_handle.cursor()

    if rows:
        sql = """INSERT INTO message (id, message_id, channel_id, channel_name, retrieved_utc, updated_utc, data)
                 VALUES {} ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data"""
        args_str = b','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x) for x in rows)
        sql = sql.format(args_str.decode("utf-8"))
        cur.execute(sql)
        db_handle.commit()
        cur.close()

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

def channel_ingest(channel_name: str, channel_id: int, stop_point: int = None):

    BATCH_SIZE = 250
    current_message_id = None
    max_message_id = None
    min_message_id = None
    stop_flag = False
    total_messages = 0
    seen_ids = set()

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
            if stop_point and message_id <= stop_point:
                stop_flag = True
                break
            if message_id in seen_ids:
                logging.warning("Message id {} was already ingested.".format(message_id))
            seen_ids.add(message_id)
            total_messages += 1
            if current_message_id is None or message_id < current_message_id:
                current_message_id = message_id
            if min_message_id is None or message_id < min_message_id:
                min_message_id = message_id
            if max_message_id is None or message_id > max_message_id:
                max_message_id = message_id
            message_channel_id = m.to_id.channel_id
            if message_channel_id != channel_id:
                error_message = "Message channel id for {} does not match expected value. {} != {}".format(channel_name, message_channel_id, channel_id)
                raise ValueError(error_message)
            id = (message_channel_id << 32) + message_id
            data = m.to_json()
            updated_utc = retrieved_utc
            es_record = ingest.translate_message_for_es(m,channel_name, retrieved_utc)
            es_records.append(es_record)
            rows.append((id, message_id, message_channel_id, channel_name, retrieved_utc, updated_utc, data))

        if es_records:
            insert_messages_into_es(es_records)

        if rows:
            insert_messages_into_pg(conn, rows)

        if stop_flag:
            break
        time.sleep(1)

    logging.debug("A total of {:,} messages was ingested for channel {}".format(total_messages, channel_name))

    if not stop_point:
        db.insert_channel(conn, channel_id, channel_name, retrieved_utc, min_message_id, max_message_id, is_active=True, is_complete=True)
    elif total_messages != 0:
        db.update_channel(conn, channel_id, channel_name, max_message_id)


channel_names = [channel for channel in open("toxic.csv","r").read().split("\n") if channel != '']
channel_names = ['reutersworldchannel']
channel_names = ['washingtonpost']
for channel_name in channel_names:
    channel_data = channels.get_channel_info(telethon_api, channel_name)
    if 'error' in channel_data:
        continue
    channel_data = json.loads(channel_data)
    channel_id = int(channel_data['full_chat']['id'])
    channel_name = channel_data['chats'][0]['username']
    channel_info = db.get_channel_status(conn, channel_id=channel_id)

    # If the channel is not in the DB, let's get the entire history for the channel
    if channel_info is None:
        db.update_channel_data(conn, channel_id, channel_data)
        channel_ingest(channel_name, channel_id)
    else:
        channel_ingest(channel_name, channel_id, channel_info[5])

    #print(channel_info)
    #sys.exit()
    #channels.update_channel_data(conn, channel_id, channel_data)
    #historical_ingest(channel_name, channel_id)
