import logging
import requests
import json

class DiscordHandler(logging.Handler):
    def __init__(self, webhook_url):
        super().__init__()
        self.webhook_url = webhook_url

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if self.webhook_url:
                requests.post(self.webhook_url, json={"content": log_entry})
        except Exception:
            self.handleError(record)
