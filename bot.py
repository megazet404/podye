import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ChatMemberUpdated
from config import BOT_TOKEN, ALLOWED_USERS, ALLOWED_CHATS, DB_PATH
from database import init_db
from handlers import process_message, process_chat_member_update, check_access

logging.basicConfig(level=logging.INFO)
dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: Message):
    if message.chat.type == "private":
        if message.from_user.id in ALLOWED_USERS:
            await message.answer("Bot initialized")
        else:
            await message.answer("Access denied")
            return

@dp.message()
async def handle_message(message: Message):
    is_allowed = await check_access(message)
    if not is_allowed:
        return
    await process_message(message)

@dp.chat_member()
async def handle_chat_member(event: ChatMemberUpdated):
    await process_chat_member_update(event)

@dp.my_chat_member()
async def handle_my_chat_member(event: ChatMemberUpdated):
    if event.new_chat_member.status == "kicked":
        return

async def main():
    init_db(DB_PATH)
    bot = Bot(token=BOT_TOKEN)

    try:
        logging.info("Bot started. Press Ctrl+C to stop.")
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received")
    finally:
        logging.info("Closing bot session...")
        await bot.session.close()
        await dp.storage.close()
        logging.info("Bot stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
