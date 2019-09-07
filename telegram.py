import json

from telethon.errors import ChatAdminRequiredError
from telethon.sync import TelegramClient
from telethon.tl import functions

from common import logger, config


class SyncTelegramClient:
    def __init__(self):
        self._client = TelegramClient("session", config["api_id"], config["api_hash"])

    def fetch_messages(self, channel, size=100, max_id=None, min_id=None):
        """Method to fetch messages from a specific channel / group"""

        logger.debug("Fetching up to %d messages from channel %s" % (size, channel))
        params = [channel, size]
        kwargs = {}

        # The telethon module has issues if a keyword passed is None, so we will add the keyword
        # only if it is not None
        for key in ['max_id', 'min_id']:
            if locals()[key] is not None:
                kwargs[key] = locals()[key]

        with self._client as client:
            data = client.get_messages(*params, **kwargs)

        return data

    def get_channel_info(self, channel):
        with self._client as client:
            data = client(functions.channels.GetFullChannelRequest(channel=channel)).to_json()
        return json.loads(data)

    def get_channel_users(self, channel, limit=1000):
        """method to get participants from channel (we might not have privileges to get this data)
        getting some errors about permissions"""
        with self._client as client:
            try:
                participants = client.get_participants(channel, limit)
            except ChatAdminRequiredError as e:
                # TODO: ???
                raise e

        return participants
