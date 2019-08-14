import json

from flask import blueprints, Response

tasks_endpoint = blueprints.Blueprint("tasks", __name__)


@tasks_endpoint.route("/tasks")
def tasks():

    tasks = {}  # TODO

    return Response(response=json.dumps(tasks),
                    mimetype="application/json")
