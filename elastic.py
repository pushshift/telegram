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

        for message in messages:
            bulk_str += '{"%s":{"_index":%s,"_id":"%s"}}\n' % (action, self._index, str(message["id"]))
            bulk_str += json.dumps(message, sort_keys=True, ensure_ascii=False) + "\n"

        r = requests.post(self._host + "/_bulk", data=bulk_str, headers=self._headers)
        content = r.json()

        logger.debug(r.text)

        if r.status_code != 200:
            raise Exception("Elasticsearch error: " + r.text)
        if content["errors"]:
            raise Exception("Elasticsearch error: " + r.text)
