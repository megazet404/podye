import sqlite3
import json
import argparse
from datetime import datetime, timezone
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

def format_timestamp(ts: Optional[int]) -> str:
    """Converts Unix timestamp to human-readable UTC string."""
    if ts is None:
        return "N/A"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def parse_chat_filter(chat_arg: str) -> Union[str, List[int]]:
    """Parses the --chat argument."""
    if chat_arg.lower() == 'none':
        return 'none'
    if chat_arg.lower() == 'all':
        return 'all'

    # Parse comma-separated list of chat IDs
    try:
        return [int(x.strip()) for x in chat_arg.split(',')]
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

def get_users_with_chats(cursor: sqlite3.Cursor) -> List[Dict[str, Any]]:
    """Fetches users and their associated chat memberships."""
    users = fetch_table_data(cursor, "users")
    for user in users:
        cursor.execute("""
            SELECT cm.*, c.title, c.type, c.username as chat_username
            FROM chat_members cm
            JOIN chats c ON cm.chat_id = c.id
            WHERE cm.user_id = ?
        """, (user['id'],))
        cols = [d[0] for d in cursor.description]
        user['memberships'] = [dict(zip(cols, row)) for row in cursor.fetchall()]
    return users

def get_chats_with_members(cursor: sqlite3.Cursor) -> List[Dict[str, Any]]:
    """Fetches chats and their associated members."""
    chats = fetch_table_data(cursor, "chats")
    for chat in chats:
        cursor.execute("""
            SELECT cm.*, u.username, u.first_name, u.last_name
            FROM chat_members cm
            JOIN users u ON cm.user_id = u.id
            WHERE cm.chat_id = ?
        """, (chat['id'],))
        cols = [d[0] for d in cursor.description]
        chat['members'] = [dict(zip(cols, row)) for row in cursor.fetchall()]
    return chats

def generate_html(data: Dict[str, Any]) -> str:
    """Generates simple HTML for debugging."""
    html = ["<html><head><meta charset='utf-8'><title>Database Dump</title></head><body>"]

    # Section: Users
    html.append("<h1>Users</h1>")
    html.append("<table border='1' cellspacing='0' cellpadding='5'>")
    html.append("<tr bgcolor='#ddd'><th>User Info</th><th>Chat Memberships</th></tr>")

    for user in data.get("users_full", []):
        # User details cell
        user_info = (
            f"<b>Name:</b> <u>{user['first_name']} {user['last_name'] or ''}</u><br>"
            f"<b>Username:</b> {user['username'] or 'N/A'}<br>"
            f"<b>ID:</b> {user['id']}<br>"
            f"<b>Bot:</b> {'Yes' if user['is_bot'] else 'No'}<br>"
            f"<b>Lang:</b> {user['language_code'] or 'N/A'}<br>"
            f"<b>Updated:</b> {format_timestamp(user['updated_at'])}"
        )

        # Memberships cell with nested table
        membership_rows = []
        if user['memberships']:
            membership_rows.append("<table border='1' cellspacing='0' cellpadding='2' style='width:100%'>")
            membership_rows.append(
                "<tr bgcolor='#eee'>"
                "<th>Chat (ID)</th>"
                "<th>Status</th>"
                "<th>Joined</th>"
                "<th>Left</th>"
                "<th>First Activity</th>"
                "<th>Last Activity</th>"
                "<th>Updated</th>"
                "</tr>"
            )
            for m in user['memberships']:
                membership_rows.append(
                    f"<tr>"
                    f"<td>{m['title'] or m['chat_username'] or 'Private'} ({m['chat_id']})</td>"
                    f"<td>{m['status']}</td>"
                    f"<td>{format_timestamp(m['joined_at'])}</td>"
                    f"<td>{format_timestamp(m['left_at'])}</td>"
                    f"<td>{format_timestamp(m['first_activity'])}</td>"
                    f"<td>{format_timestamp(m['last_activity'])}</td>"
                    f"<td>{format_timestamp(m['updated_at'])}</td>"
                    f"</tr>"
                )
            membership_rows.append("</table>")
        else:
            membership_rows.append("No active memberships found.")

        html.append(f"<tr><td valign='top'>{user_info}</td><td valign='top'>{''.join(membership_rows)}</td></tr>")
    html.append("</table>")

    all_chats = data.get("chats_full", [])
    private_chats = [c for c in all_chats if c['type'] == 'private']
    group_chats = [c for c in all_chats if c['type'] != 'private']

    # Section: Private Chats
    html.append("<h1>Chats</h1>")
    html.append("<h2>Private</h2>")
    html.append("<table border='1' cellspacing='0' cellpadding='5'>")
    html.append("<tr bgcolor='#ddd'><th>Chat Info</th></tr>")
    for chat in private_chats:
        chat_info = (
            f"<b>Username:</b> {chat['username'] or 'N/A'}<br>"
            f"<b>ID:</b> {chat['id']}<br>"
            f"<b>Updated:</b> {format_timestamp(chat['updated_at'])}"
        )
        html.append(f"<tr><td valign='top'>{chat_info}</td></tr>")
    html.append("</table>")

    # Section: Group Chats
    html.append("<h2>Groups / Channels</h2>")
    html.append("<table border='1' cellspacing='0' cellpadding='5'>")
    html.append("<tr bgcolor='#ddd'><th>Chat Info</th><th>Members</th></tr>")

    for chat in group_chats:
        chat_info = (
            f"<b>Title:</b> <u>{chat['title'] or 'N/A'}</u><br>"
            f"<b>Username:</b> {chat['username'] or 'N/A'}<br>"
            f"<b>ID:</b> {chat['id']}<br>"
            f"<b>Type:</b> {chat['type']}<br>"
            f"<b>Updated:</b> {format_timestamp(chat['updated_at'])}"
        )
        member_rows = ["<table border='1' cellspacing='0' cellpadding='2' style='width:100%'>"]
        member_rows.append(
            "<tr bgcolor='#eee'>"
            "<th>User (ID)</th>"
            "<th>Status</th>"
            "<th>Joined</th>"
            "<th>Left</th>"
            "<th>First Activity</th>"
            "<th>Last Activity</th>"
            "<th>Updated</th>"
            "</tr>"
        )
        for m in chat['members']:
            member_rows.append(
                f"<tr>"
                f"<td>{m['first_name']} {m['last_name'] or ''} ({m['user_id']})<br>@{m['username'] or 'N/A'}</td>"
                f"<td>{m['status']}</td>"
                f"<td>{format_timestamp(m['joined_at'])}</td>"
                f"<td>{format_timestamp(m['left_at'])}</td>"
                f"<td>{format_timestamp(m['first_activity'])}</td>"
                f"<td>{format_timestamp(m['last_activity'])}</td>"
                f"<td>{format_timestamp(m['updated_at'])}</td>"
                f"</tr>"
            )
        member_rows.append("</table>")
        html.append(f"<tr><td valign='top' width='25%'>{chat_info}</td><td valign='top'>{''.join(member_rows)}</td></tr>")

    html.append("</table>")
    html.append("</body></html>")
    return "\n".join(html)

def dump_database(db_path: str, output_path: str, start_time: Optional[int],
                  end_time: Optional[int], chat_filter: Union[str, List[int]],
                  fmt: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    data = {}

    if fmt == 'html':
        # Specific structure for HTML human-readable view
        data["users_full"] = get_users_with_chats(cursor)
        data["chats_full"] = get_chats_with_members(cursor)
    else:
        # Standard JSON flat structure
        data["users"] = fetch_table_data(cursor, "users")
        data["chats"] = fetch_table_data(cursor, "chats")
        data["chat_members"] = fetch_table_data(cursor, "chat_members")

        if chat_filter == 'none':
            data["messages"] = []
            data["message_media"] = []
        else:
            message_conditions = []
            message_params = []

            # Time filter
            if start_time is not None:
                message_conditions.append("date >= ?")
                message_params.append(start_time)
            if end_time is not None:
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
                m_ids = tuple(m["id"] for m in data["messages"])
                data["message_media"] = fetch_table_data(
                    cursor, "message_media",
                    f"message_id IN ({','.join('?' * len(m_ids))})", m_ids
                )
            else:
                data["message_media"] = []

    conn.close()

    with open(output_path, "w", encoding="utf-8") as f:
        if fmt == 'html':
            f.write(generate_html(data))
        else:
            json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(description="Dump SQLite database to JSON or HTML")
    parser.add_argument("--db", required=True, help="Path to SQLite database file")
    parser.add_argument("--output", required=True, help="Path to output file")
    parser.add_argument("--format", choices=['json', 'html'], default='json', help="Output format (default: json)")
    parser.add_argument("--start", help="Start time (Unix timestamp or ISO 8601)")
    parser.add_argument("--end", help="End time (Unix timestamp or ISO 8601)")
    parser.add_argument("--chat", default="all", help="Chat filter: 'all', 'none', or comma-separated chat IDs")

    args = parser.parse_args()

    st = parse_date(args.start) if args.start else None
    et = parse_date(args.end) if args.end else None
    cf = parse_chat_filter(args.chat)

    dump_database(args.db, args.output, st, et, cf, args.format)
    print(f"Database dumped to {args.output} in {args.format} format")

if __name__ == "__main__":
    main()
