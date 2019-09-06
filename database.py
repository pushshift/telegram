import json
import time
import traceback
from time import sleep

import psycopg2
from psycopg2.errorcodes import UNIQUE_VIOLATION

from common import logger, config
from model import Message


class PgConn:
    def __init__(self, conn, conn_str):
        self.conn = conn
        self.conn_str = conn_str
        self.cur = conn.cursor()

    def __enter__(self):
        return self

    def exec(self, query_string, args=None):
        if args is None:
            args = []
        while True:
            try:
                if config["sql_debug"]:
                    logger.debug(query_string)
                    logger.debug("With args " + str(args))

                self.cur.execute(query_string, args)
                break
            except psycopg2.Error as e:
                if e.pgcode == UNIQUE_VIOLATION:
                    break
                traceback.print_stack()
                self._handle_err(e, query_string, args)

    def query(self, query_string, args=None):
        while True:
            try:
                if config["sql_debug"]:
                    logger.debug(query_string)
                    logger.debug("With args " + str(args))

                self.cur.execute(query_string, args)
                res = self.cur.fetchall()

                if config["sql_debug"]:
                    logger.debug("result: " + str(res))

                return res
            except psycopg2.Error as e:
                if e.pgcode == UNIQUE_VIOLATION:
                    break
                self._handle_err(e, query_string, args)

    def _handle_err(self, err, query, args):
        logger.warn("Error during query '%s' with args %s: %s %s (%s)" % (query, args, type(err), err, err.pgcode))
        self.conn = psycopg2.connect(self.conn_str)
        self.cur = self.conn.cursor()
        sleep(0.1)

    def __exit__(self, type, value, traceback):
        try:
            self.conn.commit()
            self.cur.close()
        except:
            pass


class Database:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.conn = psycopg2.connect(self.conn_str)

    def _get_conn(self):
        return PgConn(self.conn, self.conn_str)

    def insert_message(self, message: Message):
        with self._get_conn() as conn:
            conn.exec(
                "INSERT INTO message "
                "   (id, message_id, channel_id, retrieved_utc, updated_utc, data) "
                "VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (id) DO "
                "   UPDATE SET "
                "       data=EXCLUDED.data,"
                "       updated_utc=excluded.updated_utc",
                (
                    message.record_id, message.message_id, message.channel_id, message.retrieved_utc,
                    message.updated_utc, message.data
                )
            )

    def upsert_channel(self, channel):
        with self._get_conn() as conn:
            conn.exec(
                "INSERT INTO channel "
                "   (id, name, retrieved_utc, updated_utc, min_message_id, max_message_id, is_complete, is_active) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (id) DO "
                "   UPDATE SET "
                "       max_message_id=EXCLUDED.max_message_id,"
                "       min_message_id=EXCLUDED.min_message_id, "
                "       updated_utc=EXCLUDED.updated_utc,"
                "       is_complete=EXCLUDED.is_complete",
                (
                    channel.channel_id, channel.channel_name, channel.retrieved_utc,
                    channel.updated_utc, channel.min_message_id, channel.max_message_id,
                    channel.is_complete, channel.is_active
                )
            )

    def upsert_channel_data(self, channel_id, data):
        updated_utc = int(time.time())
        if isinstance(data, dict):
            data = json.dumps(data)
        with self._get_conn() as conn:
            conn.exec(
                "INSERT INTO channel_data "
                "   (id,updated_utc,data) "
                "VALUES (%s,%s,%s) "
                "ON CONFLICT (id) DO "
                "   UPDATE SET "
                "       data=EXCLUDED.data,"
                "       updated_utc=EXCLUDED.updated_utc",
                (channel_id, updated_utc, data)
            )

    def get_channel_by_id(self, channel_id):
        with self._get_conn() as conn:
            res = conn.query(
                "SELECT * FROM channel WHERE id = %s",
                (channel_id, )
            )
        return None if not res else res[0]

    def get_channel_by_name(self, channel_name):
        with self._get_conn() as conn:
            res = conn.query(
                "SELECT * FROM channel WHERE LOWER(name) = %s",
                (channel_name.lower(), )
            )
        return None if not res else res[0]

