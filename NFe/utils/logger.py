# utils/logger.py
import logging
import os
import json
from .discord_handler import DiscordHandler


def configurar_logger():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "execucao.log")

    # monta caminho absoluto até config.json (na pasta NFe)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config.json")

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    webhook_url = config.get("DISCORD_WEBHOOK_NFE")

    handlers = [
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]

    if webhook_url:
        discord_handler = DiscordHandler(webhook_url)
        discord_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        handlers.append(discord_handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers
    )

    return logging.getLogger("baixar_nfes_qive")
