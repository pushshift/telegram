import requests
from flask_testing import LiveServerTestCase

from app import app


class SuggestChannelTest(LiveServerTestCase):

    def request(self, channel):
        return requests.get(self.get_server_url() + "/add_channel/" + channel)

    def create_app(self):
        app.config["TESTING"] = True
        return app

    def test_valid_channel_nameonly(self):
        r = self.request("valid")
        self.assertNotEqual(r.json()["msg"], "invalid channel")

    def test_invalid_channel_nameonly(self):
        r = self.request("$@adw7e89xwa")
        self.assertEqual(r.json()["msg"], "invalid channel")


