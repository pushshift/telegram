from flask import blueprints

tasks_endpoint = blueprints.Blueprint("tasks", __name__)


@tasks_endpoint.route("/tasks")
def tasks():
    raise NotImplementedError
