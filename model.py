from collections import namedtuple

MESSAGE_FIELDS = [
    "record_id", "message_id", "channel_id", "retrieved_utc", "updated_utc", "data"
]
Message = namedtuple("Message", MESSAGE_FIELDS)

CHANNEL_FIELDS = [
    "channel_id", "channel_name", "retrieved_utc", "min_message_id",
    "max_message_id", "is_active", "is_complete"
]
Channel = namedtuple("Channel", CHANNEL_FIELDS)
