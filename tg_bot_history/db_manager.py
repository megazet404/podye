import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

logger = logging.getLogger(__name__)

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
        reply_to_tg_id INTEGER,
        forward_sender_id INTEGER,
        forward_message_id INTEGER,
        forward_sender_name TEXT,
        original_text TEXT,
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
    logger.debug(f"User UPSERT: id={user_data.get('id')}, name={user_data.get('first_name')}")

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
    logger.debug(f"Chat UPSERT: id={chat_data.get('id')}, title={chat_data.get('title')}")

def insert_message(conn: sqlite3.Connection, message_data: dict) -> int:
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR IGNORE INTO messages
    (tg_id, chat_id, sender_id, reply_to_local_id, reply_to_tg_id,
     forward_sender_id, forward_message_id, forward_sender_name,
     original_text, text, entities, media_group_id, date, edit_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        message_data.get("tg_id"),
        message_data.get("chat_id"),
        message_data.get("sender_id"),
        message_data.get("reply_to_local_id"),
        message_data.get("reply_to_tg_id"),
        message_data.get("forward_sender_id"),
        message_data.get("forward_message_id"),
        message_data.get("forward_sender_name"),
        message_data.get("text"),
        message_data.get("text"),
        message_data.get("entities"),
        message_data.get("media_group_id"),
        message_data.get("date"),
        message_data.get("edit_date")
    ))
    conn.commit()
    row_id = cursor.lastrowid
    if row_id:
        logger.debug(f"Message INSERT: tg_id={message_data.get('tg_id')}, local_id={row_id}")
    else:
        logger.debug(f"Message IGNORED (duplicate): tg_id={message_data.get('tg_id')}")
    return row_id

def upsert_message(conn: sqlite3.Connection, message_data: dict) -> int:
    """Inserts a new message or updates an existing one if the new data is more recent."""
    cursor = conn.cursor()
    # original_text is set only once during initial INSERT.
    # text, entities, and edit_date are updated only if the incoming edit_date
    # is greater than or equal to the existing one.
    cursor.execute("""
    INSERT INTO messages
    (tg_id, chat_id, sender_id, reply_to_local_id, reply_to_tg_id,
     forward_sender_id, forward_message_id, forward_sender_name,
     original_text, text, entities, media_group_id, date, edit_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (tg_id, chat_id) DO UPDATE SET
        text = CASE
            WHEN COALESCE(excluded.edit_date, 0) >= COALESCE(messages.edit_date, 0)
            THEN excluded.text ELSE messages.text END,
        entities = CASE
            WHEN COALESCE(excluded.edit_date, 0) >= COALESCE(messages.edit_date, 0)
            THEN excluded.entities ELSE messages.entities END,
        edit_date = CASE
            WHEN COALESCE(excluded.edit_date, 0) >= COALESCE(messages.edit_date, 0)
            THEN excluded.edit_date ELSE messages.edit_date END
    """, (
        message_data.get("tg_id"),
        message_data.get("chat_id"),
        message_data.get("sender_id"),
        message_data.get("reply_to_local_id"),
        message_data.get("reply_to_tg_id"),
        message_data.get("forward_sender_id"),
        message_data.get("forward_message_id"),
        message_data.get("forward_sender_name"),
        message_data.get("text"), # used as original_text on insert
        message_data.get("text"),
        message_data.get("entities"),
        message_data.get("media_group_id"),
        message_data.get("date"),
        message_data.get("edit_date")
    ))
    conn.commit()

    cursor.execute("SELECT id FROM messages WHERE tg_id = ? AND chat_id = ?",
                   (message_data.get("tg_id"), message_data.get("chat_id")))
    return cursor.fetchone()[0]

def insert_media(conn: sqlite3.Connection, message_id: int, media_list: List[dict]) -> None:
    cursor = conn.cursor()
    count = 0
    for media in media_list:
        cursor.execute("""
        INSERT OR IGNORE INTO message_media
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
        count += 1
    conn.commit()
    logger.debug(f"Media INSERT: message_id={message_id}, count={count}")

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
    logger.debug(f"Member Activity UPDATE: chat={chat_id}, user={user_id}")

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
    logger.debug(f"Member Status UPDATE: chat={chat_id}, user={user_id}, status={status}")

def update_message_text(conn: sqlite3.Connection, message_data: dict) -> None:
    cursor = conn.cursor()
    cursor.execute("""
    UPDATE messages
    SET text = ?, edit_date = ?, entities = ?
    WHERE tg_id = ? AND chat_id = ?
    """, (
        message_data.get("text"),
        message_data.get("edit_date"),
        message_data.get("entities"),
        message_data.get("tg_id"),
        message_data.get("chat_id")
    ))
    conn.commit()
    logger.debug(f"DB Update: tg_id {message_data.get('tg_id')} in chat {message_data.get('chat_id')}")

def get_local_message_id(conn: sqlite3.Connection, tg_id: int, chat_id: int) -> Optional[int]:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM messages WHERE tg_id = ? AND chat_id = ?", (tg_id, chat_id))
    result = cursor.fetchone()
    return result[0] if result else None

def get_db_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def fetch_table_data(conn: sqlite3.Connection, table: str, where: Optional[str] = None, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Fetches all rows from a table as dictionaries."""
    cursor = conn.cursor()
    query = f"SELECT * FROM {table}"
    if where:
        query += f" WHERE {where}"
    cursor.execute(query, params or ())
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def get_users_with_chats(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Fetches users and their associated chat memberships with display names."""
    users = fetch_table_data(conn, "users")
    cursor = conn.cursor()
    for user in users:
        cursor.execute("""
            SELECT cm.*, c.title, c.type, c.username as chat_username,
                   u.first_name as chat_fname, u.last_name as chat_lname
            FROM chat_members cm
            JOIN chats c ON cm.chat_id = c.id
            LEFT JOIN users u ON c.id = u.id AND c.type = 'private'
            WHERE cm.user_id = ?
        """, (user['id'],))
        cols = [d[0] for d in cursor.description]
        memberships = [dict(zip(cols, row)) for row in cursor.fetchall()]

        for m in memberships:
            if m['type'] == 'private':
                name = f"{m['chat_fname'] or ''} {m['chat_lname'] or ''}".strip()
                m['display_name'] = name or f"User {m['chat_id']}"
            else:
                m['display_name'] = m['title'] or f"Group {m['chat_id']}"

        user['memberships'] = memberships
    return users

def get_chats_with_members(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Fetches chats with display names and their associated members."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.*, u.first_name, u.last_name
        FROM chats c
        LEFT JOIN users u ON c.id = u.id AND c.type = 'private'
    """)
    columns = [description[0] for description in cursor.description]
    chats = [dict(zip(columns, row)) for row in cursor.fetchall()]

    for chat in chats:
        if chat['type'] == 'private':
            name = f"{chat['first_name'] or ''} {chat['last_name'] or ''}".strip()
            chat['display_name'] = name or f"User {chat['id']}"
        else:
            chat['display_name'] = chat['title'] or f"Group {chat['id']}"

        cursor.execute("""
            SELECT cm.*, u.username, u.first_name, u.last_name
            FROM chat_members cm
            JOIN users u ON cm.user_id = u.id
            WHERE cm.chat_id = ?
        """, (chat['id'],))
        cols = [d[0] for d in cursor.description]
        chat['members'] = [dict(zip(cols, row)) for row in cursor.fetchall()]
    return chats

def get_messages_grouped_by_chat(conn: sqlite3.Connection, start_time: Optional[int],
                                 end_time: Optional[int], chat_filter: Union[str, List[int]]) -> List[Dict[str, Any]]:
    """Fetches messages with sender and chat metadata for HTML dump."""
    conditions = []
    params = []

    if start_time is not None:
        conditions.append("m.date >= ?")
        params.append(start_time)
    if end_time is not None:
        conditions.append("m.date <= ?")
        params.append(end_time)
    if isinstance(chat_filter, list):
        placeholders = ",".join("?" * len(chat_filter))
        conditions.append(f"m.chat_id IN ({placeholders})")
        params.extend(chat_filter)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT
            m.*,
            u.first_name as sender_fname, u.last_name as sender_lname, u.username as sender_uname,
            c.title as chat_title, c.type as chat_type, c.username as chat_username,
            cu.first_name as private_chat_fname, cu.last_name as private_chat_lname,
            rm.text as reply_text,
            ru.first_name as reply_sender_fname, ru.last_name as reply_sender_lname
        FROM messages m
        LEFT JOIN users u ON m.sender_id = u.id
        JOIN chats c ON m.chat_id = c.id
        LEFT JOIN users cu ON c.id = cu.id AND c.type = 'private'
        LEFT JOIN messages rm ON m.reply_to_tg_id = rm.tg_id AND m.chat_id = rm.chat_id
        LEFT JOIN users ru ON rm.sender_id = ru.id
        {where_clause}
        ORDER BY m.chat_id, m.date ASC
    """

    cursor = conn.cursor()
    cursor.execute(query, tuple(params))
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]
