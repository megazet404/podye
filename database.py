import sqlite3
from pathlib import Path
from typing import Optional, List, Tuple

def init_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA journal_mode = WAL")

    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT NOT NULL,
        last_name TEXT,
        is_bot INTEGER NOT NULL DEFAULT 0,
        language_code TEXT,
        updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS chats (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL,
        title TEXT,
        username TEXT,
        description TEXT,
        updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id INTEGER NOT NULL,
        chat_id INTEGER NOT NULL,
        sender_id INTEGER,
        reply_to_local_id INTEGER,
        forward_sender_id INTEGER,
        forward_message_id INTEGER,
        forward_sender_name TEXT,
        text TEXT,
        entities TEXT,
        media_group_id TEXT,
        date INTEGER NOT NULL,
        edit_date INTEGER,
        UNIQUE (tg_id, chat_id)
    );

    CREATE TABLE IF NOT EXISTS message_media (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER NOT NULL,
        file_id TEXT NOT NULL,
        file_unique_id TEXT NOT NULL,
        file_type TEXT NOT NULL,
        file_size INTEGER,
        mime_type TEXT,
        file_path TEXT,
        width INTEGER,
        height INTEGER
    );

    CREATE TABLE IF NOT EXISTS chat_members (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        status TEXT NOT NULL,
        joined_at INTEGER,
        left_at INTEGER,
        first_activity INTEGER,
        last_activity INTEGER,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (chat_id, user_id)
    );

    CREATE INDEX IF NOT EXISTS idx_messages_chat_date ON messages (chat_id, date DESC);
    CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages (sender_id);
    CREATE INDEX IF NOT EXISTS idx_messages_forward ON messages (forward_sender_id);
    CREATE INDEX IF NOT EXISTS idx_media_message ON message_media (message_id);
    CREATE INDEX IF NOT EXISTS idx_members_chat ON chat_members (chat_id);
    CREATE INDEX IF NOT EXISTS idx_members_activity ON chat_members (chat_id, last_activity DESC);
    CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);
    """)

    conn.commit()
    conn.close()

def upsert_user(conn: sqlite3.Connection, user_data: dict, timestamp: int) -> None:
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO users (id, username, first_name, last_name, is_bot, language_code, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO UPDATE SET
        username = excluded.username,
        first_name = excluded.first_name,
        last_name = excluded.last_name,
        is_bot = excluded.is_bot,
        language_code = excluded.language_code,
        updated_at = excluded.updated_at
    """, (
        user_data.get("id"),
        user_data.get("username"),
        user_data.get("first_name", "Unknown"),
        user_data.get("last_name"),
        1 if user_data.get("is_bot", False) else 0,
        user_data.get("language_code"),
        timestamp
    ))
    conn.commit()

def upsert_chat(conn: sqlite3.Connection, chat_data: dict, timestamp: int) -> None:
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO chats (id, type, title, username, description, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT (id) DO UPDATE SET
        type = excluded.type,
        title = excluded.title,
        username = excluded.username,
        description = excluded.description,
        updated_at = excluded.updated_at
    """, (
        chat_data.get("id"),
        chat_data.get("type"),
        chat_data.get("title"),
        chat_data.get("username"),
        chat_data.get("description"),
        timestamp
    ))
    conn.commit()

def insert_message(conn: sqlite3.Connection, message_data: dict) -> int:
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR IGNORE INTO messages
    (tg_id, chat_id, sender_id, reply_to_local_id, forward_sender_id,
     forward_message_id, forward_sender_name, text, entities, media_group_id, date, edit_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        message_data.get("tg_id"),
        message_data.get("chat_id"),
        message_data.get("sender_id"),
        message_data.get("reply_to_local_id"),
        message_data.get("forward_sender_id"),
        message_data.get("forward_message_id"),
        message_data.get("forward_sender_name"),
        message_data.get("text"),
        message_data.get("entities"),
        message_data.get("media_group_id"),
        message_data.get("date"),
        message_data.get("edit_date")
    ))
    conn.commit()
    return cursor.lastrowid

def insert_media(conn: sqlite3.Connection, message_id: int, media_list: List[dict]) -> None:
    cursor = conn.cursor()
    for media in media_list:
        cursor.execute("""
        INSERT INTO message_media
        (message_id, file_id, file_unique_id, file_type, file_size, mime_type, file_path, width, height)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            message_id,
            media.get("file_id"),
            media.get("file_unique_id"),
            media.get("file_type"),
            media.get("file_size"),
            media.get("mime_type"),
            media.get("file_path"),
            media.get("width"),
            media.get("height")
        ))
    conn.commit()

def update_chat_member_activity(conn: sqlite3.Connection, chat_id: int, user_id: int,
                                 timestamp: int, status: str = "member") -> None:
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO chat_members (chat_id, user_id, status, first_activity, last_activity, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT (chat_id, user_id) DO UPDATE SET
        status = excluded.status,
        last_activity = excluded.last_activity,
        updated_at = excluded.updated_at,
        first_activity = COALESCE(chat_members.first_activity, excluded.first_activity)
    """, (
        chat_id,
        user_id,
        status,
        timestamp,
        timestamp,
        timestamp
    ))
    conn.commit()

def update_chat_member_status(conn: sqlite3.Connection, chat_id: int, user_id: int,
                               status: str, timestamp: int, is_left: bool = False) -> None:
    cursor = conn.cursor()
    if is_left:
        cursor.execute("""
        INSERT INTO chat_members (chat_id, user_id, status, left_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (chat_id, user_id) DO UPDATE SET
            status = excluded.status,
            left_at = excluded.left_at,
            updated_at = excluded.updated_at
        """, (chat_id, user_id, status, timestamp, timestamp))
    else:
        cursor.execute("""
        INSERT INTO chat_members (chat_id, user_id, status, joined_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (chat_id, user_id) DO UPDATE SET
            status = excluded.status,
            joined_at = COALESCE(chat_members.joined_at, excluded.joined_at),
            updated_at = excluded.updated_at
        """, (chat_id, user_id, status, timestamp, timestamp))
    conn.commit()

def get_local_message_id(conn: sqlite3.Connection, tg_id: int, chat_id: int) -> Optional[int]:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM messages WHERE tg_id = ? AND chat_id = ?", (tg_id, chat_id))
    result = cursor.fetchone()
    return result[0] if result else None

def get_db_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn
