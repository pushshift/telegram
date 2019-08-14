from telethon import TelegramClient, events, sync
import ujson as json
import yaml
import time
import psycopg2
import ingest
import logging
logging.basicConfig(level=logging.DEBUG)


with open("config.yaml",'r') as stream:
    config = yaml.safe_load(stream)
    api_id = config['api_id']
    api_hash = config['api_hash']
    db_password = config['db_password']

conn = psycopg2.connect("dbname='telegram' user='postgres' host='localhost' password='{}'".format(db_password))
telethon_api = TelegramClient('session', api_id, api_hash)
telethon_api.start()

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

    BATCH_SIZE = 100
    current_message_id = None
    max_message_id = None
    min_message_id = None
    stop_flag = False
    channel_id = None

    while True:

        logging.debug("Fetching ids (in descending order) from {} starting at id {}".format(channel_name,max_message_id))
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
            rows.append((id, message_id, channel_id, retrieved_utc, updated_utc, data))

            # Add channel to channel table
            #if not channel_updated:
            #    update_channel(conn, channel_id, channel, retrieved_utc)
            #    channel_updated = True
        
        bulk_insert(conn, rows)
        rows.clear()
        if stop_flag:
            break
        time.sleep(1)

    update_channel(conn, channel_id, channel_name, retrieved_utc, min_message_id, max_message_id, is_active=True, is_complete=True)


ingest_all_messages("pushshift")