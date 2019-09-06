import logging


def fetch_messages(client, channel, size=100, max_id=None, min_id=None):
    """Method to fetch messages from a specific channel / group"""

    logging.debug("Fetching up to {} messages from channel {}".format(size, channel))
    params = [channel, size]
    kwargs = {}

    # The telethon module has issues if a keyword passed is None, so we will add the keyword
    # only if it is not None
    for key in ['max_id', 'min_id']:
        if locals()[key] is not None:
            kwargs[key] = locals()[key]

    data = client.get_messages(*params, **kwargs)

    return data
