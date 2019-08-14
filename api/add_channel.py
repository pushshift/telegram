import json
import re

from flask import blueprints, Response

suggest_channel_endpoint = blueprints.Blueprint("suggest_channel", __name__)

CHANNEL_NAME_PATTERN = re.compile(r"^[\w_\-]+$")


def matches_channel_name(channel_name):
    return CHANNEL_NAME_PATTERN.match(channel_name) is not None


@suggest_channel_endpoint.route("/add_channel/<string:channel>")
def suggest_channel(channel):
    """
    Suggest a channel
    :return: {ok: bool, msg: str}
    """

    if not matches_channel_name(channel):
        return Response(response=json.dumps({
            "ok": False,
            "msg": "invalid channel",
        }), status=400, content_type="application/json")

    # TODO: Check if the channel is already being ingested
    # TODO: Check if channel exists

    return Response(response=json.dumps({
        "ok": True,
        "msg": "ok",
    }), status=200, content_type="application/json")

