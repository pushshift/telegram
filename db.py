from telethon import TelegramClient, events, sync
import ujson as json
import time
import sys
from telethon import functions, types


def update_channel_data(db_handle, channel_id, data):
    '''Method to update channel data'''

    cur = db_handle.cursor()
    updated_utc = int(time.time())
    if isinstance(data,dict):
        data = json.dumps(data)
    cur.execute("""INSERT INTO channel_data (id,updated_utc,data) VALUES (%s,%s,%s)
              ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data, updated_utc=EXCLUDED.updated_utc""",(channel_id,
                                                                                                      updated_utc,
                                                                                                      data))
    db_handle.commit()
    cur.close()


def update_channel(db_handle, channel_id, channel_name, max_message_id):
    '''Update the channel after doing a rescan'''
    cur = db_handle.cursor()
    updated_utc = int(time.time())
    cur.execute("UPDATE channel SET max_message_id = %s, updated_utc = %s WHERE id = %s",(max_message_id, updated_utc, channel_id))
    db_handle.commit()
    cur.close()

def insert_channel(db_handle, channel_id, channel_name, retrieved_utc, min_message_id, max_message_id, is_active=True, is_complete=False):
    '''Insert new channel data.'''

    cur = db_handle.cursor()
    updated_utc = int(time.time())
    cur.execute('''INSERT INTO channel (id, name, retrieved_utc, updated_utc, min_message_id, max_message_id, is_complete, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''', (channel_id,
                                                      channel_name,
                                                      retrieved_utc,
                                                      updated_utc,
                                                      min_message_id,
                                                      max_message_id,
                                                      is_complete,
                                                      is_active))
    db_handle.commit()
    cur.close()


def get_channel_status(db_handle, channel_id=None, channel_name=None):
    '''Get current channel status by channel id'''

    cur = db_handle.cursor()

    if channel_name is None:
        cur.execute("SELECT * FROM channel WHERE id = %s",(channel_id,))
    else:
        cur.execute("SELECT * FROM channel WHERE LOWER(name) = %s",(channel_name.lower(),))

    data = cur.fetchone()
    cur.close()

    return data

