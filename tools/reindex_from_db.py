"""Reindex from db

This script will reindex data directly from the Postgres database and index the data
into Elasticsearch. This script was designed to be run when making improvements to the
elastic mapping file and then having to reindex the data to take advantage of those
improvements.

"""
import ujson as json
import yaml
import time
import psycopg2
import requests
import sys
sys.path.append("..")
import ingest
import channels
from collections import defaultdict
import logging

logging.basicConfig(level=logging.DEBUG)


with open("../config.yaml",'r') as stream:
    config = yaml.safe_load(stream)
    api_id = config['api_id']
    api_hash = config['api_hash']
    db_password = config['db_password']

conn = psycopg2.connect("dbname='telegram' user='postgres' host='localhost' password='{}'".format(db_password))
cur = conn.cursor()

id = 0

cur.execute("""SELECT id, message_id, channel_id, retrieved_utc, updated_utc, data from message
             WHERE message_id > %s ORDER BY message_id ASC LIMIT 1000""",(id,))

messages = cur.fetchall()

for idx, m in enumerate(messages):

    id = m[0]
    message_id = m[1]
    channel_id = m[2]
    retrieved_utc = m[3]
    updated_utc = m[4]
    data = m[5]
    es_data = ingest.translate_message_for_es(data, channel_name, retrieved_utc)
    print(es_data)
