import asyncio
import json
import re
import time

from flask import blueprints, Response

from common import config
from database import Database
from model import Channel
from telegram_async import AsyncTelegramClient

add_channel_endpoint = blueprints.Blueprint("add_channel", __name__)

CHANNEL_NAME_PATTERN = re.compile(r"^[\w_\-]+$")

telethon_api = AsyncTelegramClient()
db = Database(
    "dbname='telegram' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)
loop = asyncio.get_event_loop()


def matches_channel_name(channel_name):
    return CHANNEL_NAME_PATTERN.match(channel_name) is not None


@add_channel_endpoint.route("/add_channel/<string:channel>")
def add_channel(channel):
    """
    Suggest a channel
    :return: {ok: bool, msg: str}
    """

    if not matches_channel_name(channel):
        return Response(response=json.dumps({
            "ok": False,
            "msg": "invalid channel",
        }), status=400, content_type="application/json")

    j = loop.run_until_complete(telethon_api.get_channel_info(channel))

    if not j:
        return Response(response=json.dumps({
            "ok": False,
            "msg": "this channel does not exist",
        }), status=404, content_type="application/json")

    if "full_chat" not in j or "id" not in j["full_chat"]:
        return Response(response=json.dumps({
            "ok": False,
            "msg": "Telethon API returned invalid channel",
        }), status=500, content_type="application/json")

    existing = db.get_channel_by_id(j["full_chat"]["id"])

    if existing:
        return Response(response=json.dumps({
            "ok": False,
            "msg": "This channel is already being ingested",
        }), status=400, content_type="application/json")

    db.upsert_channel(Channel(
        channel_id=j["full_chat"]["id"],
        channel_name=channel,
        updated_utc=int(time.time()),
        retrieved_utc=int(time.time()),
        min_message_id=0,
        max_message_id=0,
        is_active=True,  # What is that?
        is_complete=False,
    ))

    return Response(response=json.dumps({
        "ok": True,
        "msg": "ok",
    }), status=200, content_type="application/json")

