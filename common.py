import logging

import yaml

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

logger = logging.getLogger("default")


with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)
