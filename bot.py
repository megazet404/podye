import argparse
import asyncio
import json
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ChatMemberUpdated
from config import BOT_TOKEN, ALLOWED_USERS, ALLOWED_CHATS, DB_PATH
from database import init_db
from handlers import process_message, process_chat_member_update, is_allowed_chat, is_allowed_user

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: types.Message) -> None:
    if message.chat.type == "private":
        if is_allowed_user(message.from_user.id):
            await message.answer("Bot initialized")
        else:
            await message.answer("Access denied")
            return

@dp.message()
async def handle_message(message: types.Message) -> None:
    logger.debug("Incoming Message: %s", json.dumps(message.model_dump(mode='json', exclude_none=True), ensure_ascii=False))

    if not is_allowed_chat(message.chat.id):
        return

    await process_message(message)

@dp.chat_member()
async def handle_chat_member(event: types.ChatMemberUpdated) -> None:
    logger.debug("Incoming ChatMemberUpdated: %s", json.dumps(event.model_dump(mode='json', exclude_none=True), ensure_ascii=False))

    await process_chat_member_update(event)

@dp.my_chat_member()
async def handle_my_chat_member(event: types.ChatMemberUpdated) -> None:
    logger.debug("Incoming my ChatMemberUpdated: %s", json.dumps(event.model_dump(mode='json', exclude_none=True), ensure_ascii=False))

    if event.new_chat_member.status == "kicked":
        return

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
