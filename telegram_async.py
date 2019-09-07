import json

from telethon import TelegramClient as AsyncTelegram
from telethon.tl import functions

from common import logger, config


class AsyncTelegramClient:
    """async (without the telethon.sync hack) client for the flask API"""
    def __init__(self):
        self._client = AsyncTelegram("session", config["api_id"], config["api_hash"])

    async def get_channel_info(self, channel):
        async with self._client as client:
            try:
                data = await client(functions.channels.GetFullChannelRequest(channel=channel))
            except ValueError as e:
                logger.warning(str(e))
                return None

        return json.loads(data.to_json())
