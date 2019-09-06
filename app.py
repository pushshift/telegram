from flask import Flask
import os

from api.add_channel import add_channel_endpoint

app = Flask(__name__)

TELEGRAM_PORT = os.environ.get("TELEGRAM_PORT", 8000)
TELEGRAM_HOST = os.environ.get("TELEGRAM_ADDR", "127.0.0.1")

app.register_blueprint(add_channel_endpoint)


@app.route("/")
def api_index():
    return {
        "name": "Pushshift telegram API",
        "version": "1.0",
        "url": "https://github.com/pushshift/telegram"
    }


if __name__ == "__main__":
    app.run(port=TELEGRAM_PORT, host=TELEGRAM_HOST)

