import sqlite3
import logging
from typing import Optional, List, Dict, Any, Union

logger = logging.getLogger(__name__)

class DatabaseRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def init_db(self) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
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
                message_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                sender_id INTEGER,
                reply_to_message_id INTEGER,
                quote_text TEXT,
                quote_entities TEXT,
                quote_offset INTEGER,
                quote_is_manual INTEGER,
                forward_sender_id INTEGER,
                forward_message_id INTEGER,
                forward_sender_name TEXT,
                original_text TEXT,
                text TEXT,
                entities TEXT,
                media_group_id TEXT,
                date INTEGER NOT NULL,
                edit_date INTEGER,
                PRIMARY KEY (message_id, chat_id)
            );

            CREATE TABLE IF NOT EXISTS message_media (
                message_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                file_unique_id TEXT NOT NULL,
                file_type TEXT NOT NULL,
                file_size INTEGER,
                mime_type TEXT,
                file_path TEXT,
                width INTEGER,
                height INTEGER,
                PRIMARY KEY (message_id, chat_id, file_unique_id),
                FOREIGN KEY (message_id, chat_id) REFERENCES messages (message_id, chat_id)
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

    def upsert_user(self, user_data: dict, timestamp: int) -> None:
        with self._get_connection() as conn:
            conn.execute("""
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
            logger.debug(f"User UPSERT: id={user_data.get('id')}")

    def upsert_chat(self, chat_data: dict, timestamp: int) -> None:
        with self._get_connection() as conn:
            conn.execute("""
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
            logger.debug(f"Chat UPSERT: id={chat_data.get('id')}")

    def upsert_message(self, message_data: dict) -> None:
        with self._get_connection() as conn:
            conn.execute("""
            INSERT INTO messages
            (message_id, chat_id, sender_id, reply_to_message_id,
             quote_text, quote_entities, quote_offset, quote_is_manual,
             forward_sender_id, forward_message_id, forward_sender_name,
             original_text, text, entities, media_group_id, date, edit_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (message_id, chat_id) DO UPDATE SET
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
                message_data.get("message_id"),
                message_data.get("chat_id"),
                message_data.get("sender_id"),
                message_data.get("reply_to_message_id"),
                message_data.get("quote_text"),
                message_data.get("quote_entities"),
                message_data.get("quote_offset"),
                message_data.get("quote_is_manual"),
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

    def insert_media(self, message_id: int, chat_id: int, media_list: List[dict]) -> None:
        with self._get_connection() as conn:
            for media in media_list:
                conn.execute("""
                INSERT OR IGNORE INTO message_media
                (message_id, chat_id, file_id, file_unique_id, file_type, file_size, mime_type, file_path, width, height)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    message_id,
                    chat_id,
                    media.get("file_id"),
                    media.get("file_unique_id"),
                    media.get("file_type"),
                    media.get("file_size"),
                    media.get("mime_type"),
                    media.get("file_path"),
                    media.get("width"),
                    media.get("height")
                ))

    def update_chat_member_activity(self, chat_id: int, user_id: int,
                                     timestamp: int, status: str = "member") -> None:
        with self._get_connection() as conn:
            conn.execute("""
            INSERT INTO chat_members (chat_id, user_id, status, first_activity, last_activity, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (chat_id, user_id) DO UPDATE SET
                status = excluded.status,
                last_activity = excluded.last_activity,
                updated_at = excluded.updated_at,
                first_activity = COALESCE(chat_members.first_activity, excluded.first_activity)
            """, (chat_id, user_id, status, timestamp, timestamp, timestamp))

    def update_chat_member_status(self, chat_id: int, user_id: int,
                                   status: str, timestamp: int, is_left: bool = False) -> None:
        with self._get_connection() as conn:
            if is_left:
                conn.execute("""
                INSERT INTO chat_members (chat_id, user_id, status, left_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (chat_id, user_id) DO UPDATE SET
                    status = excluded.status,
                    left_at = excluded.left_at,
                    updated_at = excluded.updated_at
                """, (chat_id, user_id, status, timestamp, timestamp))
            else:
                conn.execute("""
                INSERT INTO chat_members (chat_id, user_id, status, joined_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (chat_id, user_id) DO UPDATE SET
                    status = excluded.status,
                    joined_at = COALESCE(chat_members.joined_at, excluded.joined_at),
                    updated_at = excluded.updated_at
                """, (chat_id, user_id, status, timestamp, timestamp))

    def get_local_message_id(self, tg_id: int, chat_id: int) -> Optional[int]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM messages WHERE tg_id = ? AND chat_id = ?", (tg_id, chat_id))
            result = cursor.fetchone()
            return result[0] if result else None

    def fetch_table_data(self, table: str, where: Optional[str] = None, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = f"SELECT * FROM {table}"
            if where:
                query += f" WHERE {where}"
            cursor.execute(query, params or ())
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_users_with_chats(self) -> List[Dict[str, Any]]:
        users = self.fetch_table_data("users")
        with self._get_connection() as conn:
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

    def get_chats_with_members(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT c.*, u.first_name, u.last_name,
                       (SELECT COUNT(*) FROM messages WHERE chat_id = c.id) as msg_count
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

    def get_messages_grouped_by_chat(self, start_time: Optional[int],
                                     end_time: Optional[int], chat_filter: Union[str, List[int]]) -> List[Dict[str, Any]]:
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
                COALESCE(u.first_name, sc.title) as sender_fname, 
                u.last_name as sender_lname, 
                COALESCE(u.username, sc.username) as sender_uname,
                c.title as chat_title, c.type as chat_type, c.username as chat_username,
                cu.first_name as private_chat_fname, cu.last_name as private_chat_lname,
                rm.text as reply_text, ru.first_name as reply_sender_fname, ru.last_name as reply_sender_lname,
                fu.first_name as fwd_user_fname, fu.last_name as fwd_user_lname,
                fc.title as fwd_chat_title
            FROM messages m
            LEFT JOIN users u ON m.sender_id = u.id
            LEFT JOIN chats sc ON m.sender_id = sc.id
            JOIN chats c ON m.chat_id = c.id
            LEFT JOIN users cu ON c.id = cu.id AND c.type = 'private'
            LEFT JOIN messages rm ON m.reply_to_message_id = rm.message_id AND m.chat_id = rm.chat_id
            LEFT JOIN users ru ON rm.sender_id = ru.id
            LEFT JOIN users fu ON m.forward_sender_id = fu.id
            LEFT JOIN chats fc ON m.forward_sender_id = fc.id
            {where_clause}
            ORDER BY m.chat_id, m.date ASC
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            cols = [d[0] for d in cursor.description]
            messages = [dict(zip(cols, row)) for row in cursor.fetchall()]

            if not messages:
                return []

            message_ids = list(set((m['message_id'], m['chat_id']) for m in messages))
            media_map = {}
            
            for m_id, c_id in message_ids:
                cursor.execute(
                    "SELECT * FROM message_media WHERE message_id = ? AND chat_id = ?",
                    (m_id, c_id)
                )
                m_cols = [d[0] for d in cursor.description]
                media_items = [dict(zip(m_cols, r)) for r in cursor.fetchall()]
                if media_items:
                    media_map[(m_id, c_id)] = media_items

            for m in messages:
                m['media'] = media_map.get((m['message_id'], m['chat_id']), [])

            return messages
