import json
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Set, List, Union

load_dotenv()

def normalize_chat_id(chat_id: Union[int, str]) -> int:
    """Converts user-friendly chat ID to Bot API format (-100...)."""
    try:
        cid = int(chat_id)
        # If already in API format (negative), return as is
        if cid < 0:
            return cid
        # Convert positive ID to Bot API supergroup/channel format
        return -int(f"100{abs(cid)}")
    except ValueError:
        raise ValueError(f"Invalid chat ID format: {chat_id}")

def load_config() -> dict:
    config_path = Path("config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = json.load(f)

    # Process allowed users (positive IDs, no conversion)
    allowed_users = set(raw_config.get("allowed_users", []))

    # Process allowed chats (convert to Bot API format)
    allowed_chats = set()
    for chat_id in raw_config.get("allowed_chats", []):
        allowed_chats.add(normalize_chat_id(chat_id))

    return {
        "allowed_users": allowed_users,
        "allowed_chats": allowed_chats
    }

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "bot_database.db")
CONFIG = load_config()
ALLOWED_USERS = CONFIG["allowed_users"]
ALLOWED_CHATS = CONFIG["allowed_chats"]
