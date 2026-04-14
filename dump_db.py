import sqlite3
import json
import argparse
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

def parse_date(date_str: str) -> int:
    """Converts ISO 8601 or Unix timestamp to Unix timestamp."""
    try:
        return int(date_str)
    except ValueError:
        try:
            dt = datetime.fromisoformat(date_str)
            return int(dt.timestamp())
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}")

def parse_chat_filter(chat_arg: str) -> Union[str, List[int]]:
    """Parses the --chat argument."""
    if chat_arg.lower() == 'none':
        return 'none'
    if chat_arg.lower() == 'all':
        return 'all'

    # Parse comma-separated list of chat IDs
    try:
        chat_ids = [int(x.strip()) for x in chat_arg.split(',')]
        return chat_ids
    except ValueError:
        raise ValueError(f"Invalid chat ID format: {chat_arg}")

def fetch_table_data(cursor: sqlite3.Cursor, table: str, where: Optional[str] = None, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Fetches all rows from a table as dictionaries."""
    query = f"SELECT * FROM {table}"
    if where:
        query += f" WHERE {where}"
    cursor.execute(query, params or ())
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def dump_database(db_path: str, output_path: str, start_time: Optional[int], end_time: Optional[int], chat_filter: Union[str, List[int]]) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    data = {}

    # Dump reference tables (users, chats, chat_members) fully
    data["users"] = fetch_table_data(cursor, "users")
    data["chats"] = fetch_table_data(cursor, "chats")
    data["chat_members"] = fetch_table_data(cursor, "chat_members")

    # Dump messages with filters
    if chat_filter == 'none':
        data["messages"] = []
        data["message_media"] = []
    else:
        message_conditions = []
        message_params = []

        # Time filter
        if start_time is not None and end_time is not None:
            message_conditions.append("date >= ? AND date <= ?")
            message_params.extend([start_time, end_time])
        elif start_time is not None:
            message_conditions.append("date >= ?")
            message_params.append(start_time)
        elif end_time is not None:
            message_conditions.append("date <= ?")
            message_params.append(end_time)

        # Chat filter
        if isinstance(chat_filter, list):
            placeholders = ",".join("?" * len(chat_filter))
            message_conditions.append(f"chat_id IN ({placeholders})")
            message_params.extend(chat_filter)

        message_where = " AND ".join(message_conditions) if message_conditions else None

        data["messages"] = fetch_table_data(cursor, "messages", message_where, tuple(message_params))

        # Dump media linked to selected messages
        if data["messages"]:
            message_ids = tuple(m["id"] for m in data["messages"])
            placeholders = ",".join("?" * len(message_ids))
            media_where = f"message_id IN ({placeholders})"
            data["message_media"] = fetch_table_data(cursor, "message_media", media_where, message_ids)
        else:
            data["message_media"] = []

    conn.close()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(description="Dump SQLite database to JSON")
    parser.add_argument("--db", required=True, help="Path to SQLite database file")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--start", help="Start time (Unix timestamp or ISO 8601)")
    parser.add_argument("--end", help="End time (Unix timestamp or ISO 8601)")
    parser.add_argument("--chat", default="all", help="Chat filter: 'all', 'none', or comma-separated chat IDs (e.g., -100123,-100456)")

    args = parser.parse_args()

    start_time = parse_date(args.start) if args.start else None
    end_time = parse_date(args.end) if args.end else None
    chat_filter = parse_chat_filter(args.chat)

    dump_database(args.db, args.output, start_time, end_time, chat_filter)
    print(f"Database dumped to {args.output}")

if __name__ == "__main__":
    main()
