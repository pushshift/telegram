import requests
from flask_testing import LiveServerTestCase

from api.app import app


class SuggestChannelTest(LiveServerTestCase):

    def request(self, json):
        return requests.post(self.get_server_url() + "/suggest_channel", json=json)

    def create_app(self):
        app.config["TESTING"] = True
        return app

    def test_invalid_json(self):
        r = self.request(json=None)

        self.assertNotEqual(r.status_code, 200)
        self.assertEqual(r.json()["msg"], "invalid request")

    def test_valid_channel_fullurl(self):
        r = self.request({
            "channel": "http://telegram.me/valid",
        })
        self.assertNotEqual(r.json()["msg"], "invalid channel")

    def test_invalid_channel_fullurl(self):
        r = self.request({
            "channel": "http://google.ca/valid",
        })
        self.assertEqual(r.json()["msg"], "invalid channel")

    def test_invalid_channel_fullurl2(self):
        r = self.request({
            "channel": "http://telegam.me/@$invalid",
        })
        self.assertEqual(r.json()["msg"], "invalid channel")

    def test_valid_channel_noscheme(self):
        r = self.request({
            "channel": "telegram.me/valid",
        })
        self.assertNotEqual(r.json()["msg"], "invalid channel")

    def test_valid_channel_nameonly(self):
        r = self.request({
            "channel": "valid",
        })
        self.assertNotEqual(r.json()["msg"], "invalid channel")

    def test_invalid_channel_nameonly(self):
        r = self.request({
            "channel": "$@adw7e89xwa",
        })
        self.assertEqual(r.json()["msg"], "invalid channel")


