import html
import json
import argparse
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from database import get_db_connection, fetch_table_data, get_users_with_chats, get_chats_with_members

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
        return "-"
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

def generate_html(data: Dict[str, Any]) -> str:
    """Generates structured HTML for debugging and audit."""

    def generate_users(users_data: List[Dict[str, Any]]) -> str:
        """Sub-function to generate the Users section."""
        html_segment = []
        html_segment.append("<table border='1' cellspacing='0' cellpadding='5'>")
        html_segment.append("<tr bgcolor='#ddd'><th>User Info</th><th>Chat Memberships</th></tr>")

        for user in users_data:
            first_name = html.escape(user['first_name'] or "")
            last_name = html.escape(user['last_name'] or "")
            username = html.escape(user['username'] or "-")
            lang = html.escape(user['language_code'] or "-")

            user_info = (
                f"<b>Name:</b> <u>{first_name} {last_name}</u><br>"
                f"<b>Username:</b> @{username}<br>"
                f"<b>ID:</b> {user['id']}<br>"
                f"<b>Bot:</b> {'Yes' if user['is_bot'] else 'No'}<br>"
                f"<b>Lang:</b> {lang}<br>"
                f"<b>Updated:</b> {format_timestamp(user['updated_at'])}"
            )

            # Memberships cell with nested table
            membership_rows = []
            if user['memberships']:
                membership_rows.append("<table border='1' cellspacing='0' cellpadding='2' style='width:100%'>")
                membership_rows.append(
                    "<tr bgcolor='#eee'>"
                    "<th>Chat</th>"
                    "<th>Status</th>"
                    "<th>Joined</th>"
                    "<th>Left</th>"
                    "<th>First Activity</th>"
                    "<th>Last Activity</th>"
                    "<th>Updated</th>"
                    "</tr>"
                )
                for m in user['memberships']:
                    d_name = html.escape(m['display_name'] or "")
                    c_username = html.escape(m['chat_username'] or "-")
                    membership_rows.append(
                        f"<tr>"
                        f"<td><u>{d_name}</u><br/>({m['chat_id']})<br/>@{c_username}</td>"
                        f"<td>{html.escape(m['status'])}</td>"
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

            html_segment.append(f"<tr><td valign='top'>{user_info}</td><td valign='top'>{''.join(membership_rows)}</td></tr>")

        html_segment.append("</table>")
        return "".join(html_segment)

    def generate_chats(chats_data: List[Dict[str, Any]]) -> str:
        """Sub-function to generate the Users section."""

        private_chats = [c for c in chats_data if c['type'] == 'private']
        group_chats   = [c for c in chats_data if c['type'] != 'private']

        html_segment  = []

        # Section: Private Chats
        html_segment.append("<h2>Private</h2>")
        html_segment.append("<table border='1' cellspacing='0' cellpadding='5'>")
        html_segment.append("<tr bgcolor='#ddd'><th>Chat Info</th></tr>")
        for chat in private_chats:
            d_name = html.escape(chat['display_name'] or "-")
            username = html.escape(chat['username'] or "-")
            chat_info = (
                f"<b>Name:</b> <u>{d_name}</u><br>"
                f"<b>Username:</b> @{username}<br>"
                f"<b>ID:</b> {chat['id']}<br>"
                f"<b>Updated:</b> {format_timestamp(chat['updated_at'])}"
            )
            html_segment.append(f"<tr><td valign='top'>{chat_info}</td></tr>")
        html_segment.append("</table>")

        # Section: Group Chats
        html_segment.append("<h2>Groups / Channels</h2>")
        html_segment.append("<table border='1' cellspacing='0' cellpadding='5'>")
        html_segment.append("<tr bgcolor='#ddd'><th>Chat Info</th><th>Members</th></tr>")

        for chat in group_chats:
            d_name = html.escape(chat['display_name'] or "")
            username = html.escape(chat['username'] or "-")
            chat_info = (
                f"<b>Title:</b> <u>{d_name}</u><br>"
                f"<b>Username:</b> @{username}<br>"
                f"<b>ID:</b> {chat['id']}<br>"
                f"<b>Type:</b> {html.escape(chat['type'] or '')}<br>"
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
            for m in chat.get('members', []):
                m_fname = html.escape(m['first_name'] or "")
                m_lname = html.escape(m['last_name'] or "")
                m_uname = html.escape(m['username'] or "-")
                member_rows.append(
                    f"<tr>"
                    f"<td><u>{m_fname} {m_lname}</u><br/>({m['user_id']})<br/>@{m_uname}</td>"
                    f"<td>{html.escape(m['status'] or '')}</td>"
                    f"<td>{format_timestamp(m['joined_at'])}</td>"
                    f"<td>{format_timestamp(m['left_at'])}</td>"
                    f"<td>{format_timestamp(m['first_activity'])}</td>"
                    f"<td>{format_timestamp(m['last_activity'])}</td>"
                    f"<td>{format_timestamp(m['updated_at'])}</td>"
                    f"</tr>"
                )
            member_rows.append("</table>")
            html_segment.append(f"<tr><td valign='top' width='25%'>{chat_info}</td><td valign='top'>{''.join(member_rows)}</td></tr>")

        html_segment.append("</table>")
        return "".join(html_segment)

    def generate_messages(messages_data: List[Dict[str, Any]]) -> str:
        if not messages_data:
            return "<p>No messages found for the given criteria.</p>"

        # Grouping by chat
        chats = {}
        for m in messages_data:
            cid = m['chat_id']
            if cid not in chats:
                if m['chat_type'] == 'private':
                    name = f"{m['private_chat_fname'] or ''} {m['private_chat_lname'] or ''}".strip()
                    display_name = name or "-"
                else:
                    display_name = m['chat_title'] or "-"

                chats[cid] = {
                    "name": display_name,
                    "username": m.get('chat_username'),
                    "type": m['chat_type'],
                    "msgs": []
                }
            chats[cid]["msgs"].append(m)

        def render_chats_list(category_chats):
            html_segment = []
            for cid, cinfo in category_chats.items():
                c_name_esc = html.escape(cinfo['name'])
                c_uname_esc = html.escape(cinfo['username'] or "-")

                html_segment.append(f"<h3>{c_name_esc} ({cid}) @{c_uname_esc}</h3>")
                html_segment.append("<table border='1' cellspacing='0' cellpadding='5'>")
                html_segment.append("<tr bgcolor='#ddd'><th>Date</th><th>Sender</th><th>Content</th></tr>")

                for m in cinfo["msgs"]:
                    sender_name = f"{m['sender_fname'] or ''} {m['sender_lname'] or ''}".strip() or "Unknown"
                    s_uname_esc = html.escape(m['sender_uname'] or "-")
                    sender = f"<u>{html.escape(sender_name)}</u><br/>({m['sender_id']})<br/>@{s_uname_esc}"

                    text_content = html.escape(m['text'] or "").replace("\n", "<br/>")

                    original = ""
                    if m['original_text'] and m['original_text'] != m['text']:
                        original_text_esc = html.escape(m['original_text']).replace("\n", "<br/>")
                        original = (
                            f"<div style='color: #777; font-size: 0.9em; border-left: 2px solid #ccc; "
                            f"padding-left: 5px; margin-bottom: 5px;'><i>Original:</i><br/>"
                            f"{original_text_esc}</div>"
                        )

                    content = f"{text_content}<br/>"
                    if m['media_group_id']:
                        mg_id_esc = html.escape(m['media_group_id'])
                        content += f"<br/><small style='color: green;'>Media Group: {mg_id_esc}</small>"
                    if m['edit_date']:
                        content += f"<br/><small style='color: blue;'>Edited at: {format_timestamp(m['edit_date'])}</small>"

                    content += original

                    html_segment.append(
                        f"<tr>"
                        f"<td valign='top'>{format_timestamp(m['date'])}</td>"
                        f"<td valign='top'>{sender}</td>"
                        f"<td valign='top'>{content}</td>"
                        f"</tr>"
                    )
                html_segment.append("</table><br/>")
            return "".join(html_segment)

        private_chats = {cid: info for cid, info in chats.items() if info['type'] == 'private'}
        group_chats = {cid: info for cid, info in chats.items() if info['type'] != 'private'}

        html_segment = []
        if private_chats:
            html_segment.append("<h2>Private chats</h2>")
            html_segment.append(render_chats_list(private_chats))

        if group_chats:
            html_segment.append("<h2>Groups / Channels</h2>")
            html_segment.append(render_chats_list(group_chats))

        return "".join(html_segment)

    html_lines = ["<html><head><meta charset='utf-8'><title>Database Dump</title></head><body>"]

    html_lines.append("<h1>Summary Information</h1>")
    html_lines.append(f"<p>Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</p>")

    html_lines.append("<h1>Users</h1>")
    html_lines.append(generate_users(data.get("users_full", [])))

    html_lines.append("<h1>Chats</h1>")
    html_lines.append(generate_chats(data.get("chats_full", [])))

    if "messages_full" in data:
        html_lines.append("<h1>Messages</h1>")
        html_lines.append(generate_messages(data["messages_full"]))

    html_lines.append("</body></html>")
    return "\n".join(html_lines)

def dump_database(db_path: str, output_path: str, start_time: Optional[int],
                  end_time: Optional[int], chat_filter: Union[str, List[int]],
                  fmt: str) -> None:
    conn = get_db_connection(db_path)
    data = {}

    if fmt == 'html':
        data["users_full"] = get_users_with_chats(conn)
        data["chats_full"] = get_chats_with_members(conn)
        if chat_filter != 'none':
            from database import get_messages_grouped_by_chat
            data["messages_full"] = get_messages_grouped_by_chat(conn, start_time, end_time, chat_filter)
    else:
        data["users"] = fetch_table_data(conn, "users")
        data["chats"] = fetch_table_data(conn, "chats")
        data["chat_members"] = fetch_table_data(conn, "chat_members")

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
            data["messages"] = fetch_table_data(conn, "messages", message_where, tuple(message_params))

            # Dump media linked to selected messages
            if data["messages"]:
                m_ids = tuple(m["id"] for m in data["messages"])
                data["message_media"] = fetch_table_data(
                    conn, "message_media",
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
