import json
import time
from typing import Optional, List, Dict, Any
from aiogram import types
from aiogram.types import ChatMemberUpdated
from database import (
    get_db_connection, upsert_user, upsert_chat, insert_message,
    insert_media, update_chat_member_activity, update_chat_member_status,
    get_local_message_id
)
from config import DB_PATH, ALLOWED_USERS, ALLOWED_CHATS

def is_allowed_chat(chat_id: int) -> bool:
    return chat_id in ALLOWED_CHATS

def is_allowed_user(user_id: int) -> bool:
    return user_id in ALLOWED_USERS

def extract_user_data(user: types.User) -> dict:
    return {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_bot": user.is_bot,
        "language_code": user.language_code
    }

def extract_chat_data(chat: types.Chat) -> dict:
    return {
        "id": chat.id,
        "type": chat.type,
        "title": chat.title,
        "username": chat.username,
        "description": chat.description if hasattr(chat, 'description') else None
    }

def extract_media_data(message: types.Message) -> List[dict]:
    media_list = []

    if message.photo:
        for photo in message.photo:
            media_list.append({
                "file_id": photo.file_id,
                "file_unique_id": photo.file_unique_id,
                "file_type": "photo",
                "file_size": photo.file_size,
                "mime_type": "image/jpeg",
                "file_path": None,
                "width": photo.width,
                "height": photo.height
            })

    if message.document:
        media_list.append({
            "file_id": message.document.file_id,
            "file_unique_id": message.document.file_unique_id,
            "file_type": "document",
            "file_size": message.document.file_size,
            "mime_type": message.document.mime_type,
            "file_path": None,
            "width": None,
            "height": None
        })

    if message.video:
        media_list.append({
            "file_id": message.video.file_id,
            "file_unique_id": message.video.file_unique_id,
            "file_type": "video",
            "file_size": message.video.file_size,
            "mime_type": message.video.mime_type,
            "file_path": None,
            "width": message.video.width,
            "height": message.video.height
        })

    if message.voice:
        media_list.append({
            "file_id": message.voice.file_id,
            "file_unique_id": message.voice.file_unique_id,
            "file_type": "voice",
            "file_size": message.voice.file_size,
            "mime_type": message.voice.mime_type,
            "file_path": None,
            "width": None,
            "height": None
        })

    if message.audio:
        media_list.append({
            "file_id": message.audio.file_id,
            "file_unique_id": message.audio.file_unique_id,
            "file_type": "audio",
            "file_size": message.audio.file_size,
            "mime_type": message.audio.mime_type,
            "file_path": None,
            "width": None,
            "height": None
        })

    if message.video_note:
        media_list.append({
            "file_id": message.video_note.file_id,
            "file_unique_id": message.video_note.file_unique_id,
            "file_type": "video_note",
            "file_size": message.video_note.file_size,
            "mime_type": "video/mp4",
            "file_path": None,
            "width": None,
            "height": None
        })

    return media_list

def extract_forward_sender_info(message: types.Message) -> tuple:
    forward_sender_id = None
    forward_message_id = None
    forward_sender_name = None

    if message.forward_origin:
        if message.forward_origin.type == "user":
            forward_sender_id = message.forward_origin.sender_user.id
        elif message.forward_origin.type == "chat":
            forward_sender_id = message.forward_origin.sender_chat.id
        elif message.forward_origin.type == "channel":
            forward_sender_id = message.forward_origin.chat.id
            forward_message_id = message.forward_origin.message_id
        elif message.forward_origin.type == "hidden_user":
            forward_sender_name = message.forward_origin.sender_user_name

        if hasattr(message.forward_origin, 'message_id'):
            forward_message_id = message.forward_origin.message_id

    return forward_sender_id, forward_message_id, forward_sender_name

async def process_message(message: types.Message) -> None:
    timestamp = int(time.time())
    conn = get_db_connection(DB_PATH)

    try:
        chat_id = message.chat.id
        if not is_allowed_chat(chat_id):
            return

        upsert_chat(conn, extract_chat_data(message.chat), timestamp)

        sender_id = None
        if message.from_user:
            sender_id = message.from_user.id
            if message.from_user.id > 0:
                upsert_user(conn, extract_user_data(message.from_user), timestamp)
                update_chat_member_activity(conn, chat_id, message.from_user.id, timestamp)

        if message.sender_chat:
            sender_id = message.sender_chat.id
            upsert_chat(conn, extract_chat_data(message.sender_chat), timestamp)

        reply_to_local_id = None
        if message.reply_to_message:
            reply_to_local_id = get_local_message_id(
                conn,
                message.reply_to_message.message_id,
                chat_id
            )

        forward_sender_id, forward_message_id, forward_sender_name = \
            extract_forward_sender_info(message)

        if forward_sender_id and forward_sender_id > 0:
            if message.forward_origin and message.forward_origin.type == "user":
                upsert_user(conn, extract_user_data(message.forward_origin.sender_user), timestamp)
        elif forward_sender_id and forward_sender_id < 0:
            if message.forward_origin and message.forward_origin.type in ["chat", "channel"]:
                chat_info = message.forward_origin.sender_chat if hasattr(message.forward_origin, 'sender_chat') else message.forward_origin.chat
                upsert_chat(conn, extract_chat_data(chat_info), timestamp)

        entities_json = json.dumps([e.model_dump() for e in message.entities]) if message.entities else None

        message_data = {
            "tg_id": message.message_id,
            "chat_id": chat_id,
            "sender_id": sender_id,
            "reply_to_local_id": reply_to_local_id,
            "forward_sender_id": forward_sender_id,
            "forward_message_id": forward_message_id,
            "forward_sender_name": forward_sender_name,
            "text": message.text,
            "entities": entities_json,
            "media_group_id": message.media_group_id,
            "date": message.date,
            "edit_date": message.edit_date
        }

        local_message_id = insert_message(conn, message_data)

        media_list = extract_media_data(message)
        if media_list and local_message_id:
            insert_media(conn, local_message_id, media_list)

        if message.new_chat_members:
            for member in message.new_chat_members:
                upsert_user(conn, extract_user_data(member), timestamp)
                update_chat_member_status(conn, chat_id, member.id, "member", timestamp)

        if message.left_chat_member:
            update_chat_member_status(
                conn, chat_id, message.left_chat_member.id,
                "left", timestamp, is_left=True
            )

    finally:
        conn.close()

async def process_chat_member_update(event: ChatMemberUpdated) -> None:
    timestamp = int(time.time())
    conn = get_db_connection(DB_PATH)

    try:
        chat_id = event.chat.id
        if not is_allowed_chat(chat_id):
            return

        user_id = event.from_user.id
        upsert_user(conn, extract_user_data(event.from_user), timestamp)

        old_status = event.old_chat_member.status
        new_status = event.new_chat_member.status

        is_left = new_status in ["left", "kicked"]
        update_chat_member_status(conn, chat_id, user_id, new_status, timestamp, is_left)

    finally:
        conn.close()

async def check_access(message: types.Message) -> bool:
    if message.chat.type == "private":
        return is_allowed_user(message.from_user.id)
    return is_allowed_chat(message.chat.id)
