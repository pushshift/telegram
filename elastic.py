import json

import requests

from common import logger


class ES:
    def __init__(self, host, index):
        self._host = host
        self._index = index
        self._headers = {
            "Accept": "application/json",
            "Content-type": "application/json; charset=utf-8",
        }

    def bulk_insert(self, messages, action="index"):
        bulk_str = ""

        if len(messages) == 0:
            return

        for message in messages:
            bulk_str += '{"%s":{"_index":"%s","_id":"%s"}}\n' % (action, self._index, str(message["id"]))
            bulk_str += json.dumps(message, sort_keys=True, ensure_ascii=False) + "\n"

        r = requests.post(self._host + "/_bulk", data=bulk_str.encode("utf-8"), headers=self._headers)
        content = r.json()

        logger.debug("Indexed %d documents <%d>" % (len(messages), r.status_code))

        if r.status_code != 200:
            raise Exception("Elasticsearch error: " + r.text)
        if content["errors"]:
            raise Exception("Elasticsearch error: " + r.text)


def translate_message_for_es(message, channel_name, retrieved_utc):
    """Process and prepare a message object for inclusion to Elastic. This method essentially
    handles all the more intricate and nasty translations that need to take place so that each
    message can be indexed within Elasticsearch with minimal issues. Since this translation
    could take up many lines of code, it's best to have it as its own method."""

    es_record = {
        "channel_id": message.to_id.channel_id,
        "message_id": message.id,
        "message": message.message,
        "date": int(message.date.timestamp()),
        "via_bot_id": message.via_bot_id,
        "channel_name": channel_name,
        "grouped_id": message.grouped_id,
        "post_author": message.post_author,
        "post": message.post,
        "silent": message.silent,
        "retrieved_utc": retrieved_utc,
        "updated_utc": retrieved_utc,
    }

    es_record["id"] = (es_record["channel_id"] << 32) + es_record["message_id"]
    return es_record
