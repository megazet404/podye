import argparse
import asyncio
import json
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ChatMemberUpdated
from config import BOT_TOKEN, ALLOWED_USERS, ALLOWED_CHATS, DB_PATH
from tg_bot_history.db_manager import init_db
from tg_bot_history.collectors import HistoryCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def is_allowed_chat(chat_id: int) -> bool:
    return chat_id in ALLOWED_CHATS or chat_id in ALLOWED_USERS

def is_allowed_user(user_id: int) -> bool:
    return user_id in ALLOWED_USERS

dp = Dispatcher()
collector = HistoryCollector(DB_PATH)

@dp.message(CommandStart())
async def cmd_start(message: Message, bot: Bot) -> None:
    if message.chat.type == "private":
        if not is_allowed_user(message.from_user.id):
            return # Silent ignore
        await message.answer("Поехали.")

@dp.message()
async def handle_message(message: Message, bot: Bot) -> None:
    logger.debug("Incoming Message: %s", json.dumps(message.model_dump(mode='json', exclude_none=True), ensure_ascii=False))

    if not is_allowed_chat(message.chat.id):
        if message.chat.type != "private":
            try:
                await bot.send_message(message.chat.id, "Всё, заебали. Я сваливаю отсюда, нахуй.")
                await bot.leave_chat(message.chat.id)
                logger.info(f"Left unauthorized chat: {message.chat.id}")
            except Exception as e:
                logger.error(f"Failed to leave chat {message.chat.id}: {e}")
        return

    collector.process_message(message)

@dp.edited_message()
async def handle_edited_message(message: Message) -> None:
    logger.debug("Incoming Edited Message: %s",
                 json.dumps(message.model_dump(mode='json', exclude_none=True), ensure_ascii=False))

    if not is_allowed_chat(message.chat.id):
        return
    collector.process_edited_message(message)

@dp.chat_member()
async def handle_chat_member(event: ChatMemberUpdated, bot: Bot) -> None:
    logger.debug("Incoming ChatMemberUpdated: %s", json.dumps(event.model_dump(mode='json', exclude_none=True), ensure_ascii=False))

    if not is_allowed_chat(event.chat.id):
        return
    collector.process_chat_member_update(event)

@dp.my_chat_member()
async def handle_my_chat_member(event: ChatMemberUpdated, bot: Bot) -> None:
    logger.debug("Incoming my ChatMemberUpdated: %s", json.dumps(event.model_dump(mode='json', exclude_none=True), ensure_ascii=False))

    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status

    if old_status != "member" and new_status == "member":
        if is_allowed_chat(event.chat.id):
            await bot.send_message(event.chat.id, "Вы кто такие? Я ва... А, это вы? Ну что ж, готов быть вашим маленьким вредным пидором.")
        else:
            try:
                await bot.send_message(event.chat.id, "Вы кто такие? Я вас не звал! Идите нахуй!")
                await bot.leave_chat(event.chat.id)
                logger.info(f"Left unauthorized chat: {event.chat.id}")
            except Exception as e:
                logger.error(f"Failed to leave chat {event.chat.id}: {e}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled")

    init_db(DB_PATH)
    bot = Bot(token=BOT_TOKEN)

    try:
        logger.info("Bot started. Press Ctrl+C to stop.")
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    finally:
        logger.info("Closing bot session...")
        await bot.session.close()
        await dp.storage.close()
        logger.info("Bot stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
