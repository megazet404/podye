import html
import json
import argparse
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from tg_bot_history.db_manager import DatabaseRepository

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
                f"<b>Name:</b> <u>{first_name} {last_name}</u><br/>"
                f"<b>Username:</b> @{username}<br/>"
                f"<b>ID:</b> {user['id']}<br/>"
                f"<b>Bot:</b> {'Yes' if user['is_bot'] else 'No'}<br/>"
                f"<b>Lang:</b> {lang}<br/>"
                f"<b>Updated:</b> {format_timestamp(user['updated_at'])}"
            )

            user_anchor_id = f"user_{user['id']}"

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

                    chat_anchor = f"chat_{m['chat_id']}"
                    chat_link = f"<a href='#{chat_anchor}' style='text-decoration: none; color: inherit;'><u>{d_name}</u></a>"

                    membership_rows.append(
                        f"<tr>"
                        f"<td>{chat_link}<br/>({m['chat_id']})<br/>@{c_username}</td>"
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

            html_segment.append(f"<tr id='{user_anchor_id}'><td valign='top'>{user_info}</td><td valign='top'>{''.join(membership_rows)}</td></tr>")

        html_segment.append("</table>")
        return "".join(html_segment)

    def generate_chats(chats_data: List[Dict[str, Any]]) -> str:
        """Sub-function to generate the Chats section."""

        def _render_private(chats: List[Dict[str, Any]]) -> str:
            segment = ["<h2>Private</h2>", "<table border='1' cellspacing='0' cellpadding='5'>",
                       "<tr bgcolor='#ddd'><th>Chat Info</th></tr>"]
            for chat in chats:
                d_name = html.escape(chat['display_name'] or "-")
                username = html.escape(chat['username'] or "-")
                msg_count = chat.get('msg_count', 0)
                msg_link = f"<a href='#chat_msgs_{chat['id']}'>{msg_count}</a>" if msg_count > 0 else "0"
                chat_info = (
                    f"<b>Name:</b> <u>{d_name}</u><br/>"
                    f"<b>Username:</b> @{username}<br/>"
                    f"<b>ID:</b> {chat['id']}<br/>"
                    f"<b>Updated:</b> {format_timestamp(chat['updated_at'])}<br/>"
                    f"<b>Messages:</b> {msg_link}"
                )
                segment.append(f"<tr id='chat_{chat['id']}'><td valign='top'>{chat_info}</td></tr>")
            segment.append("</table>")
            return "".join(segment)

        def _render_groups(chats: List[Dict[str, Any]]) -> str:
            segment = ["<h2>Groups</h2>", "<table border='1' cellspacing='0' cellpadding='5'>",
                       "<tr bgcolor='#ddd'><th>Chat Info</th><th>Members</th></tr>"]
            for chat in chats:
                d_name = html.escape(chat['display_name'] or "")
                username = html.escape(chat['username'] or "-")
                description = html.escape(chat['description'] or "-")
                msg_count = chat.get('msg_count', 0)
                msg_link = f"<a href='#chat_msgs_{chat['id']}'>{msg_count}</a>" if msg_count > 0 else "0"
                chat_info = (
                    f"<b>Title:</b> <u>{d_name}</u><br/>"
                    f"<b>Username:</b> @{username}<br/>"
                    f"<b>ID:</b> {chat['id']}<br/>"
                    f"<b>Type:</b> {html.escape(chat['type'] or '')}<br/>"
                    f"<b>Description:</b> {description}<br/>"
                    f"<b>Updated:</b> {format_timestamp(chat['updated_at'])}<br/>"
                    f"<b>Messages:</b> {msg_link}"
                )
                member_rows = ["<table border='1' cellspacing='0' cellpadding='2' style='width:100%'>",
                               "<tr bgcolor='#eee'><th>User (ID)</th><th>Status</th><th>Joined</th>"
                               "<th>Left</th><th>First Activity</th><th>Last Activity</th><th>Updated</th></tr>"]
                for m in chat.get('members', []):
                    m_fname = html.escape(m['first_name'] or "")
                    m_lname = html.escape(m['last_name'] or "")
                    m_uname = html.escape(m['username'] or "-")
                    user_anchor = f"user_{m['user_id']}"
                    user_link = f"<a href='#{user_anchor}' style='text-decoration: none; color: inherit;'><u>{m_fname} {m_lname}</u></a>"
                    member_rows.append(
                        f"<tr><td>{user_link}<br/>({m['user_id']})<br/>@{m_uname}</td>"
                        f"<td>{html.escape(m['status'] or '')}</td>"
                        f"<td>{format_timestamp(m['joined_at'])}</td>"
                        f"<td>{format_timestamp(m['left_at'])}</td>"
                        f"<td>{format_timestamp(m['first_activity'])}</td>"
                        f"<td>{format_timestamp(m['last_activity'])}</td>"
                        f"<td>{format_timestamp(m['updated_at'])}</td></tr>"
                    )
                member_rows.append("</table>")
                segment.append(f"<tr><td valign='top' width='25%' id='chat_{chat['id']}'>{chat_info}</td><td valign='top'>{''.join(member_rows)}</td></tr>")
            segment.append("</table>")
            return "".join(segment)

        def _render_channels(chats: List[Dict[str, Any]]) -> str:
            segment = ["<h2>Channels</h2>", "<table border='1' cellspacing='0' cellpadding='5'>",
                       "<tr bgcolor='#ddd'><th>Chat Info</th></tr>"]
            for chat in chats:
                d_name = html.escape(chat['display_name'] or "-")
                username = html.escape(chat['username'] or "-")
                description = html.escape(chat['description'] or "-")
                msg_count = chat.get('msg_count', 0)
                msg_link = f"<a href='#chat_msgs_{chat['id']}'>{msg_count}</a>" if msg_count > 0 else "0"
                chat_info = (
                    f"<b>Title:</b> <u>{d_name}</u><br/>"
                    f"<b>Username:</b> @{username}<br/>"
                    f"<b>ID:</b> {chat['id']}<br/>"
                    f"<b>Description:</b> {description}<br/>"
                    f"<b>Updated:</b> {format_timestamp(chat['updated_at'])}<br/>"
                    f"<b>Messages:</b> {msg_link}"
                )
                segment.append(f"<tr id='chat_{chat['id']}'><td valign='top'>{chat_info}</td></tr>")
            segment.append("</table>")
            return "".join(segment)

        private_chats = [c for c in chats_data if c['type'] == 'private']
        group_chats   = [c for c in chats_data if c['type'] in ('group', 'supergroup')]
        channel_chats = [c for c in chats_data if c['type'] == 'channel']

        return _render_private(private_chats) + _render_groups(group_chats) + _render_channels(channel_chats)

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

                html_segment.append(
                    f"<h3 id='chat_msgs_{cid}'>"
                    f"<a href='#chat_{cid}' style='text-decoration: none; color: inherit;'>"
                    f"<u>{c_name_esc}</u>"
                    f"</a>"
                    f" ({cid}) @{c_uname_esc}"
                    f"</h3>"
                )
                html_segment.append("<table border='1' cellspacing='0' cellpadding='5'>")
                html_segment.append("<tr bgcolor='#ddd'><th>Date</th><th>Sender</th><th>Content</th></tr>")

                for m in cinfo["msgs"]:
                    sender_name = f"{m['sender_fname'] or ''} {m['sender_lname'] or ''}".strip() or "Unknown"
                    s_uname_esc = html.escape(m['sender_uname'] or "-")

                    sender_id = m['sender_id']
                    if sender_id:
                        anchor_prefix = "user" if sender_id > 0 else "chat"
                        sender_link = f"<a href='#{anchor_prefix}_{sender_id}' style='text-decoration: none; color: inherit;'>"
                        sender_link_end = "</a>"
                    else:
                        sender_link = ""
                        sender_link_end = ""

                    sender = (
                        f"{sender_link}<u>{html.escape(sender_name)}</u>{sender_link_end}<br/>"
                        f"({sender_id})<br/>"
                        f"@{s_uname_esc}"
                    )

                    text_content = html.escape(m['text'] or "").replace("\n", "<br/>")

                    reply_block = ""
                    if m.get('reply_to_message_id') is not None:
                        quote_text = m.get('quote_text')
                        replied_full_text = m.get('reply_text')

                        display_text = quote_text if quote_text else (replied_full_text if replied_full_text else "[Media]")

                        r_sender = f"{m['reply_sender_fname'] or ''} {m['reply_sender_lname'] or ''}".strip() or "Unknown"
                        r_text = html.escape(display_text).replace("\n", "<br/>")

                        reply_target_id = f"msg_{m['chat_id']}_{m['reply_to_message_id']}"

                        reply_block = (
                            f"<a href='#{reply_target_id}' style='text-decoration: none; color: inherit;'>"
                            f"<div style='color: #555; font-size: 0.85em; border-left: 3px solid #0088cc; "
                            f"padding: 2px 0 2px 10px; margin-bottom: 8px; background: #f4f4f4;'>"
                            f"<b>{html.escape(r_sender)} (↑):</b><br/>{r_text}</div>"
                            f"</a>"
                        )

                    forward_block = ""
                    if m.get('forward_sender_id') is not None or m.get('forward_sender_name') is not None:
                        f_id = m.get('forward_sender_id')
                        f_msg_id = m.get('forward_message_id')
                        f_name_raw = m.get('forward_sender_name')

                        f_link = None
                        f_msg_link = None
                        if f_id:
                            if f_id > 0: # User
                                u_fname = m.get('fwd_user_fname') or ""
                                u_lname = m.get('fwd_user_lname') or ""
                                f_display_name = f"{u_fname} {u_lname}".strip() or f"User {f_id}"
                                f_link = f"#user_{f_id}"
                            else: # Chat/Channel
                                f_display_name = m.get('fwd_chat_title') or f"Chat {f_id}"
                                f_link = f"#chat_{f_id}"
                                if f_msg_id:
                                    f_msg_link = f"#msg_{f_id}_{f_msg_id}"
                        else:
                            f_display_name = f_name_raw or "Unknown Original Sender"

                        f_name_html = html.escape(f_display_name)
                        if f_link:
                            f_name_html = f"<a href='{f_link}' style='text-decoration: none; color: inherit;'><u>{f_name_html}</u></a>"

                        f_msg_html = ""
                        if f_msg_link:
                            f_msg_html = f" (<a href='{f_msg_link}' style='text-decoration: none; color: inherit;'>→</a>)"

                        forward_block = (
                            f"<div style='color: #555; font-size: 0.85em; border-left: 3px solid #52a152; "
                            f"padding: 2px 0 2px 10px; margin-bottom: 8px; background: #f4f4f4;'>"
                            f"<b>Forwarded from {f_name_html}{f_msg_html}</b></div>"
                        )

                    original = ""
                    if m['original_text'] and m['original_text'] != m['text']:
                        original_text_esc = html.escape(m['original_text']).replace("\n", "<br/>")
                        original = (
                            f"<div style='color: #777; font-size: 0.9em; border-left: 2px solid #ccc; "
                            f"padding-left: 5px; margin-top: 5px;'><i>Original:</i><br/>"
                            f"{original_text_esc}</div>"
                        )

                    media_info_block = ""
                    if m.get('media'):
                        media_items = []
                        for item in m['media']:
                            size_kb = f"{item['file_size'] / 1024:.1f} KB" if item['file_size'] else "unknown size"
                            dims = f" ({item['width']}x{item['height']})" if item['width'] and item['height'] else ""
                            media_items.append(
                                f"<li><b>{html.escape(item['file_type'])}</b>: "
                                f"{html.escape(item['mime_type'] or 'no-mime')}, {size_kb}{dims}<br/>"
                                f"<small style='color: #888;'>ID: {html.escape(item['file_unique_id'])}</small></li>"
                            )
                        
                        media_info_block = (
                            f"<div style='margin-top: 8px; padding: 5px; background: #f9f9f9; border: 1px dashed #ccc;'>"
                            f"<span style='font-size: 0.8em; color: #666;'>Attached Media Info:</span>"
                            f"<ul style='margin: 0; padding-left: 20px; font-size: 0.85em;'>"
                            f"{''.join(media_items)}</ul></div>"
                        )

                    content = f"{reply_block}{forward_block}{text_content}{media_info_block}<br/>"
                    if m['media_group_id']:
                        mg_id_esc = html.escape(m['media_group_id'])
                        content += f"<br/><small style='color: green;'>Media Group: {mg_id_esc}</small>"
                    if m['edit_date']:
                        content += f"<br/><small style='color: blue;'>Edited at: {format_timestamp(m['edit_date'])}</small>"

                    content += original

                    msg_anchor_id = f"msg_{m['chat_id']}_{m['message_id']}"
                    html_segment.append(
                        f"<tr id='{msg_anchor_id}'>"
                        f"<td valign='top'>{format_timestamp(m['date'])}</td>"
                        f"<td valign='top'>{sender}</td>"
                        f"<td valign='top'>{content}</td>"
                        f"</tr>"
                    )
                html_segment.append("</table><br/>")
            return "".join(html_segment)

        private_chats = {cid: info for cid, info in chats.items() if info['type'] == 'private'}
        group_chats   = {cid: info for cid, info in chats.items() if info['type'] in ('group', 'supergroup')}
        channel_chats = {cid: info for cid, info in chats.items() if info['type'] == 'channel'}

        html_segment = []
        if private_chats:
            html_segment.append("<h2>Private chats</h2>")
            html_segment.append(render_chats_list(private_chats))

        if group_chats:
            html_segment.append("<h2>Groups</h2>")
            html_segment.append(render_chats_list(group_chats))

        if channel_chats:
            html_segment.append("<h2>Channels</h2>")
            html_segment.append(render_chats_list(channel_chats))

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
    repo = DatabaseRepository(db_path)
    data = {}

    if fmt == 'html':
        data["users_full"] = repo.get_users_with_chats()
        data["chats_full"] = repo.get_chats_with_members()
        if chat_filter != 'none':
            data["messages_full"] = repo.get_messages_grouped_by_chat(start_time, end_time, chat_filter)
    else:
        data["users"] = repo.fetch_table_data("users")
        data["chats"] = repo.fetch_table_data("chats")
        data["chat_members"] = repo.fetch_table_data("chat_members")

        if chat_filter == 'none':
            data["messages"] = []
            data["message_media"] = []
        else:
            message_conditions = []
            message_params = []

            if start_time is not None:
                message_conditions.append("date >= ?")
                message_params.append(start_time)
            if end_time is not None:
                message_conditions.append("date <= ?")
                message_params.append(end_time)

            if isinstance(chat_filter, list):
                placeholders = ",".join("?" * len(chat_filter))
                message_conditions.append(f"chat_id IN ({placeholders})")
                message_params.extend(chat_filter)

            message_where = " AND ".join(message_conditions) if message_conditions else None
            data["messages"] = repo.fetch_table_data("messages", message_where, tuple(message_params))

            if data["messages"]:
                m_ids = tuple(m["id"] for m in data["messages"])
                data["message_media"] = repo.fetch_table_data(
                    "message_media",
                    f"message_id IN ({','.join('?' * len(m_ids))})", m_ids
                )
            else:
                data["message_media"] = []

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
