import json
import time
import logging
from typing import Optional, List
from aiogram import types
from aiogram.types import ChatMemberUpdated
from .db_manager import DatabaseRepository

logger = logging.getLogger(__name__)

class HistoryCollector:
    def __init__(self, repo: DatabaseRepository):
        self.repo = repo

    def _extract_user_data(self, user: types.User) -> dict:
        return {
            "id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "is_bot": user.is_bot,
            "language_code": user.language_code
        }

    def _extract_chat_data(self, chat: types.Chat) -> dict:
        return {
            "id": chat.id,
            "type": chat.type,
            "title": chat.title,
            "username": chat.username,
            "description": chat.description if hasattr(chat, 'description') else None
        }

    def _extract_media_data(self, message: types.Message) -> List[dict]:
        media_list = []

        if message.photo:
            # Collect the photo in original resolution only.
            photo = message.photo[-1]
            media_list.append({
                "file_id": photo.file_id,
                "file_unique_id": photo.file_unique_id,
                "file_type": "photo",
                "file_size": photo.file_size,
                "mime_type": "image/jpeg",
                "width": photo.width,
                "height": photo.height
            })

        if message.document:
            media_list.append({
                "file_id": message.document.file_id,
                "file_unique_id": message.document.file_unique_id,
                "file_type": "document",
                "file_size": message.document.file_size,
                "mime_type": message.document.mime_type
            })

        if message.video:
            media_list.append({
                "file_id": message.video.file_id,
                "file_unique_id": message.video.file_unique_id,
                "file_type": "video",
                "file_size": message.video.file_size,
                "mime_type": message.video.mime_type,
                "width": message.video.width,
                "height": message.video.height
            })

        if message.voice:
            media_list.append({
                "file_id": message.voice.file_id,
                "file_unique_id": message.voice.file_unique_id,
                "file_type": "voice",
                "file_size": message.voice.file_size,
                "mime_type": message.voice.mime_type
            })

        if message.audio:
            media_list.append({
                "file_id": message.audio.file_id,
                "file_unique_id": message.audio.file_unique_id,
                "file_type": "audio",
                "file_size": message.audio.file_size,
                "mime_type": message.audio.mime_type
            })

        if message.video_note:
            media_list.append({
                "file_id": message.video_note.file_id,
                "file_unique_id": message.video_note.file_unique_id,
                "file_type": "video_note",
                "file_size": message.video_note.file_size,
                "mime_type": "video/mp4"
            })

        return media_list

    def _extract_forward_sender_info(self, message: types.Message) -> tuple:
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

    def _save_message_to_db(self, message: types.Message, timestamp: int,
                         update_activity: bool = True) -> None:
        """Internal helper to coordinate extraction and repository calls."""
        chat_id = message.chat.id
        self.repo.upsert_chat(self._extract_chat_data(message.chat), timestamp)

        sender_id = None
        if message.from_user:
            sender_id = message.from_user.id
            if sender_id > 0:
                self.repo.upsert_user(self._extract_user_data(message.from_user), timestamp)
                if update_activity:
                    self.repo.update_chat_member_activity(chat_id, sender_id, timestamp)

        if message.sender_chat:
            sender_id = message.sender_chat.id
            self.repo.upsert_chat(self._extract_chat_data(message.sender_chat), timestamp)

        reply_to_message_id = None
        if message.reply_to_message:
            self._save_message_to_db(message.reply_to_message, timestamp, update_activity=False)
            reply_to_message_id = message.reply_to_message.message_id

        entities_list = message.entities or message.caption_entities
        entities_json = json.dumps([e.model_dump() for e in entities_list]) if entities_list else None
        media_list = self._extract_media_data(message)
        message_text = message.text or message.caption

        forward_sender_id, forward_message_id, forward_sender_name = self._extract_forward_sender_info(message)

        if forward_sender_id:
            origin = message.forward_origin
            if origin.type == "user":
                self.repo.upsert_user(self._extract_user_data(origin.sender_user), timestamp)
            elif origin.type == "chat":
                self.repo.upsert_chat(self._extract_chat_data(origin.sender_chat), timestamp)
            elif origin.type == "channel":
                self.repo.upsert_chat(self._extract_chat_data(origin.chat), timestamp)

                origin_date_ts = int(origin.date.timestamp()) if hasattr(origin.date, 'timestamp') else int(origin.date)

                origin_msg_data = {
                    "message_id": origin.message_id,
                    "chat_id": origin.chat.id,
                    "sender_id": origin.chat.id,
                    "text": message_text,
                    "entities": entities_json,
                    "date": origin_date_ts
                }
                self.repo.upsert_message(origin_msg_data)
                if media_list:
                    self.repo.insert_media(origin.message_id, origin.chat.id, media_list)

        date_val = message.date
        date_ts = int(date_val.timestamp()) if hasattr(date_val, 'timestamp') else int(date_val)

        edit_date_ts = None
        if message.edit_date:
            edit_val = message.edit_date
            edit_date_ts = int(edit_val.timestamp()) if hasattr(edit_val, 'timestamp') else int(edit_val)

        quote_text = None
        quote_entities = None
        quote_offset = None
        quote_is_manual = None

        if message.quote:
            quote = message.quote
            quote_text = quote.text
            quote_offset = quote.position
            quote_is_manual = 1 if quote.is_manual else 0
            if quote.entities:
                quote_entities = json.dumps([e.model_dump() for e in quote.entities])

        message_data = {
            "message_id": message.message_id,
            "chat_id": chat_id,
            "sender_id": sender_id,
            "reply_to_message_id": reply_to_message_id,
            "quote_text": quote_text,
            "quote_entities": quote_entities,
            "quote_offset": quote_offset,
            "quote_is_manual": quote_is_manual,
            "forward_sender_id": forward_sender_id,
            "forward_message_id": forward_message_id,
            "forward_sender_name": forward_sender_name,
            "text": message_text,
            "entities": entities_json,
            "media_group_id": message.media_group_id,
            "date": date_ts,
            "edit_date": edit_date_ts
        }

        self.repo.upsert_message(message_data)
        media_list = self._extract_media_data(message)
        if media_list:
            self.repo.insert_media(message.message_id, chat_id, media_list)

    def process_message(self, message: types.Message) -> None:
        logger.debug(f"Processing message {message.message_id} from chat {message.chat.id}")
        timestamp = int(time.time())

        self._save_message_to_db(message, timestamp)

        if message.new_chat_members:
            for member in message.new_chat_members:
                self.repo.upsert_user(self._extract_user_data(member), timestamp)
                self.repo.update_chat_member_status(message.chat.id, member.id, "member", timestamp)

        if message.left_chat_member:
            self.repo.update_chat_member_status(message.chat.id, message.left_chat_member.id, "left", timestamp, is_left=True)

    def process_edited_message(self, message: types.Message) -> None:
        timestamp = int(time.time())
        self._save_message_to_db(message, timestamp, update_activity=False)

    def process_chat_member_update(self, event: ChatMemberUpdated) -> None:
        timestamp = int(time.time())
        chat_id = event.chat.id
        user_id = event.from_user.id

        self.repo.upsert_user(self._extract_user_data(event.from_user), timestamp)

        old_status = event.old_chat_member.status
        new_status = event.new_chat_member.status

        is_left = new_status in ["left", "kicked"]
        self.repo.update_chat_member_status(chat_id, user_id, new_status, timestamp, is_left)

        if not is_left:
            self.repo.update_chat_member_activity(chat_id, user_id, timestamp, new_status)
