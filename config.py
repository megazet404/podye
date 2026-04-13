import json
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def load_config() -> dict:
    config_path = Path("config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "bot_database.db")
CONFIG = load_config()
ALLOWED_USERS = set(CONFIG.get("allowed_users", []))
ALLOWED_CHATS = set(CONFIG.get("allowed_chats", []))
