import json
import re

from flask import blueprints, request, Response

suggest_channel_endpoint = blueprints.Blueprint("suggest_channel", __name__)

CHANNEL_URL_PATTERN = re.compile(r"^(https?://)?telegram.me/[\w_\-]+$")
CHANNEL_NAME_PATTERN = re.compile(r"^[\w_\-]+$")


def matches_channel_url(channel_url):
    return CHANNEL_URL_PATTERN.match(channel_url) is not None


def matches_channel_name(channel_name):
    return CHANNEL_NAME_PATTERN.match(channel_name) is not None


@suggest_channel_endpoint.route("/suggest_channel", methods=["POST"])
def suggest_channel():
    """
    Suggest a channel
    :return: {ok: bool, msg: str}
    """

    if not request.json:
        return Response(response=json.dumps({
            "ok": False,
            "msg": "invalid request",
        }), status=400, content_type="application/json")

    channel = request.json["channel"]

    if not matches_channel_url(channel) and not matches_channel_name(channel):
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

