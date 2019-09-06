import logging
import sys
from logging import StreamHandler

import yaml

from database import Database
from elastic import ES

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

logger = logging.getLogger("default")

for h in logger.handlers:
    logger.removeHandler(h)
logger.addHandler(StreamHandler(sys.stdout))

SQL_DEBUG = True

with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

db = Database(
    "dbname='telegram' user='postgres' host='localhost' password='%s'"
    % (config["db_password"],)
)

es = ES(
    config["es_host"],
    config["es_index"],
)
