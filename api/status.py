from flask import blueprints

status_endpoint = blueprints.Blueprint("status", __name__)


@status_endpoint.route("/status")
def status():
    raise NotImplementedError
