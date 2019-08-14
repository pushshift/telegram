import json

from flask import blueprints, Response

status_endpoint = blueprints.Blueprint("status", __name__)


@status_endpoint.route("/status")
def status():

    status = {}  # TODO

    return Response(response=json.dumps(status),
                    mimetype="application/json")
