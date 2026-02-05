import asyncio
import json
import os
import re
import random
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.client.default import DefaultBotProperties
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, ChannelInvalidError, UsernameInvalidError, \
    ChatWriteForbiddenError, UserNotParticipantError, FloodWaitError
from telethon.tl.functions.channels import JoinChannelRequest, GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch, InputPeerChat
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

TOKEN = '8266492120:AAEc5yqQvyc_ngCSVnAxQe_GG0yVtHGrBlw'
OWNER_ID = 8178283518
DATA_FILE = 'bot_data.json'
SESSION_DIR = 'sessions'
ITEMS_PER_PAGE = 10

# Настройки для пересылки
SOURCE_CHANNEL = "https://t.me/work24easy"
SOURCE_MESSAGE_ID = 10

# ID вашего приватного чата
PRIVATE_CHAT_ID = -1003864516969

os.makedirs(SESSION_DIR, exist_ok=True)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

DEFAULT_BOT_DATA = {
    "admins": [OWNER_ID],
    "sessions": {},
    "channels": [],
    "private_chats": [],
    "hidden_mention_channels": [],
    "settings": {
        "message_text": "",
        "message_interval": 960,
        "cycle_interval": 960,
        "hidden_mention_symbol": "⁠",
        "forward_mode": True,
        "source_channel": SOURCE_CHANNEL,
        "source_message_id": SOURCE_MESSAGE_ID
    },
    "is_running": False
}

bot_data = DEFAULT_BOT_DATA.copy()
active_clients = {}
mailing_task = None
current_pages = {}
cached_message = None

# Функция для отправки логов в бота
async def send_log_to_bot(message):
    """Отправляет важные лог-сообщения владельцу бота"""
    try:
        # Логируем в консоль
        print(f"[LOG] {message}")
        
        # Отправляем в Telegram (только важные события)
        if any(keyword in message for keyword in ["✅", "❌", "🔄", "🚀", "⏹", "🛑", "КРИТИЧЕСКАЯ"]):
            try:
                await bot.send_message(OWNER_ID, message)
            except:
                pass
    except Exception as e:
        print(f"Ошибка отправки лога: {e}")

def load_data():
    global bot_data
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            try:
                loaded_data = json.load(f)
                for key in DEFAULT_BOT_DATA:
                    if key in loaded_data:
                        if key == "settings" and isinstance(loaded_data[key], dict):
                            bot_data[key] = {**DEFAULT_BOT_DATA[key], **loaded_data[key]}
                        else:
                            bot_data[key] = loaded_data[key]
                    else:
                        bot_data[key] = DEFAULT_BOT_DATA[key]
                
                if "private_chats" not in bot_data:
                    bot_data["private_chats"] = []
                if "hidden_mention_channels" not in bot_data:
                    bot_data["hidden_mention_channels"] = []
                    
            except Exception as e:
                print(f"Ошибка загрузки данных: {e}")
                bot_data = DEFAULT_BOT_DATA.copy()
    else:
        bot_data = DEFAULT_BOT_DATA.copy()

def save_data():
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(bot_data, f, indent=4, ensure_ascii=False)

class Form(StatesGroup):
    change_text = State()
    set_message_interval = State()
    set_cycle_interval = State()
    add_admin = State()
    remove_admin = State()
    add_session = State()
    add_session_api_id = State()
    add_session_api_hash = State()
    add_session_phone = State()
    add_session_code = State()
    add_session_password = State()
    remove_session = State()
    add_channel = State()
    add_private_chat = State()
    remove_channel = State()
    hidden_mentions = State()
    set_hidden_symbol = State()

def main_menu_kb():
    kb = [
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="users")],
        [InlineKeyboardButton(text="📢 Каналы", callback_data="channels")],
        [InlineKeyboardButton(text="💬 Приватные чаты", callback_data="private_chats")],
        [InlineKeyboardButton(text="▶️ Начать рассылку", callback_data="start_mailing") if not bot_data["is_running"]
         else InlineKeyboardButton(text="⏹ Остановить рассылку", callback_data="stop_mailing")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def back_button(target):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data=target)]])

def settings_kb():
    kb = [
        [InlineKeyboardButton(text="✏️ Изменить текст/источник", callback_data="change_text")],
        [InlineKeyboardButton(text="⏱ Таймер между сообщениями", callback_data="set_message_interval")],
        [InlineKeyboardButton(text="🔄 Таймер между циклами", callback_data="set_cycle_interval")],
        [InlineKeyboardButton(text="📊 Просмотреть информацию", callback_data="view_info")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def users_kb():
    kb = [
        [InlineKeyboardButton(text="👑 Администрация", callback_data="administration")],
        [InlineKeyboardButton(text="📱 Сессии", callback_data="sessions_menu")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def administration_kb():
    kb = [
        [InlineKeyboardButton(text="➕ Добавить администратора", callback_data="add_admin")],
        [InlineKeyboardButton(text="📋 Список администраторов", callback_data="list_admins")],
        [InlineKeyboardButton(text="➖ Удалить администратора", callback_data="remove_admin")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="users")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def sessions_kb():
    kb = [
        [InlineKeyboardButton(text="➕ Добавить сессию", callback_data="add_session")],
        [InlineKeyboardButton(text="📋 Список сессий", callback_data="list_sessions")],
        [InlineKeyboardButton(text="➖ Удалить сессию", callback_data="remove_session")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="users")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def channels_kb():
    kb = [
        [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
        [InlineKeyboardButton(text="➕ Добавить приватный чат", callback_data="add_private_chat")],
        [InlineKeyboardButton(text="📋 Список каналов", callback_data="list_channels")],
        [InlineKeyboardButton(text="📋 Список приватных чатов", callback_data="list_private_chats")],
        [InlineKeyboardButton(text="➖ Удалить", callback_data="remove_channel")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

def is_admin(user_id):
    return user_id in bot_data["admins"]

async def init_clients():
    global active_clients

    for session_id, session_data in bot_data["sessions"].items():
        if session_data.get("session_string"):
            try:
                client = TelegramClient(
                    StringSession(session_data["session_string"]),
                    session_data["api_id"],
                    session_data["api_hash"]
                )

                await client.start()
                active_clients[session_id] = client

                if not await client.is_user_authorized():
                    print(f"Сессия {session_id} не авторизована")
                    bot_data["sessions"][session_id]["is_authorized"] = False
                    continue

                bot_data["sessions"][session_id]["is_authorized"] = True

                # Присоединяемся к публичным каналам
                for channel in bot_data["channels"]:
                    try:
                        entity = await client.get_entity(channel)
                        await client(JoinChannelRequest(entity))
                        print(f"✅ Сессия присоединилась к каналу: {channel}")
                    except (ChannelInvalidError, UsernameInvalidError, ValueError) as e:
                        print(f"Ошибка присоединения к каналу {channel}: {e}")
                        continue

                print(f"✅ Сессия {session_id} инициализирована")

            except Exception as e:
                print(f"Ошибка инициализации сессии {session_id}: {e}")
                bot_data["sessions"][session_id]["is_authorized"] = False

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("Доступ запрещен")
        return

    await message.answer("Главное меню:", reply_markup=main_menu_kb())

@dp.callback_query(F.data == "main_menu")
async def main_menu(call: types.CallbackQuery):
    await call.message.edit_text("Главное меню:", reply_markup=main_menu_kb())
    await call.answer()

@dp.callback_query(F.data == "settings")
async def settings_menu(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Доступ запрещен", show_alert=True)
    await call.message.edit_text("⚙️ Настройки:", reply_markup=settings_kb())
    await call.answer()

@dp.callback_query(F.data == "users")
async def users_menu(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Доступ запрещен", show_alert=True)
        return

    await call.message.edit_text("👥 Управление пользователями:", reply_markup=users_kb())
    await call.answer()

@dp.callback_query(F.data == "administration")
async def administration_menu(call: types.CallbackQuery):
    await call.message.edit_text("👑 Управление администраторов:", reply_markup=administration_kb())
    await call.answer()

@dp.callback_query(F.data == "sessions_menu")
async def sessions_menu(call: types.CallbackQuery):
    await call.message.edit_text("📱 Управление сессиями:", reply_markup=sessions_kb())
    await call.answer()

@dp.callback_query(F.data == "private_chats")
async def private_chats_menu(call: types.CallbackQuery):
    await call.message.edit_text("💬 Управление приватными чатами:", reply_markup=channels_kb())
    await call.answer()

@dp.callback_query(F.data == "channels")
async def channels_menu(call: types.CallbackQuery):
    await call.message.edit_text("📢 Управление каналами:", reply_markup=channels_kb())
    await call.answer()

@dp.callback_query(F.data == "add_private_chat")
async def add_private_chat_start(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "Введите ID приватного чата (например: -1003864516969):\n\n"
        "Как найти ID:\n"
        "1. В формате -100XXXXXXXXX\n"
        "2. Или invite-ссылку (t.me/+xxxxxxxxxx)\n"
        "3. Или username приватной группы",
        reply_markup=back_button("channels")
    )
    await state.set_state(Form.add_private_chat)
    await call.answer()

@dp.message(Form.add_private_chat)
async def add_private_chat_finish(message: types.Message, state: FSMContext):
    chat_identifier = message.text.strip()
    
    # Проверяем, не добавлен ли уже этот чат
    if chat_identifier in bot_data["private_chats"]:
        await message.answer("❌ Этот чат уже добавлен.", reply_markup=main_menu_kb())
        await state.clear()
        return
    
    # Пробуем подключиться к чату через первую доступную сессию
    success = False
    error_message = ""
    
    if not active_clients:
        await message.answer("❌ Нет активных сессий. Добавьте сначала сессию.", reply_markup=main_menu_kb())
        await state.clear()
        return
    
    client = list(active_clients.values())[0]
    
    try:
        # Пробуем получить entity разными способами
        entity = None
        try:
            # Сначала пробуем как строку
            entity = await client.get_entity(chat_identifier)
        except (ValueError, TypeError):
            # Пробуем как числовой ID
            try:
                chat_id = int(chat_identifier)
                entity = await client.get_entity(chat_id)
            except Exception as e:
                error_message = f"❌ Не удалось получить доступ к чату: {e}"
        
        if entity:
            # Проверяем, состоит ли пользователь в чате
            try:
                await client.get_participants(entity, limit=1)
                bot_data["private_chats"].append(chat_identifier)
                save_data()
                success = True
                await send_log_to_bot(f"✅ Приватный чат добавлен: {chat_identifier}")
                
            except (UserNotParticipantError, ValueError) as e:
                error_message = "❌ Бот не состоит в этом чате. Нужно добавить бота в чат."
                
    except Exception as e:
        error_message = f"❌ Ошибка: {e}"
    
    if success:
        await message.answer(f"✅ Приватный чат добавлен: {chat_identifier}", reply_markup=main_menu_kb())
    else:
        await message.answer(error_message, reply_markup=main_menu_kb())
    
    await state.clear()

@dp.callback_query(F.data == "list_private_chats")
async def list_private_chats(call: types.CallbackQuery, page: int = 0):
    if not bot_data["private_chats"]:
        await call.message.edit_text("❌ Нет добавленных приватных чатов.", reply_markup=channels_kb())
        await call.answer()
        return
    
    current_pages[call.from_user.id] = page
    total_pages = (len(bot_data["private_chats"]) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(bot_data["private_chats"]))
    
    text = f"💬 Список приватных чатов (страница {page + 1}/{total_pages}):\n\n"
    
    for i in range(start_idx, end_idx):
        chat_id = bot_data["private_chats"][i]
        text += f"{i + 1}. ID: {chat_id}\n"
    
    keyboard = []
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"private_page_{page - 1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"private_page_{page + 1}"))
        keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="channels")])
    
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    await call.answer()

@dp.callback_query(F.data.startswith("private_page_"))
async def private_page_handler(call: types.CallbackQuery):
    page = int(call.data.split("_")[2])
    await list_private_chats(call, page)

async def get_forward_message():
    global cached_message
    
    if cached_message:
        return cached_message
    
    try:
        if not active_clients:
            return None
            
        client = list(active_clients.values())[0]
        
        source_entity = await client.get_entity(bot_data["settings"]["source_channel"])
        messages = await client.get_messages(source_entity, ids=[bot_data["settings"]["source_message_id"]])
        
        if messages and messages[0]:
            cached_message = messages[0]
            return cached_message
            
    except Exception as e:
        print(f"Ошибка получения сообщения для пересылки: {e}")
    
    return None

async def get_channel_participants(client, entity):
    """Получает участников канала/чата"""
    participants = []
    
    try:
        if hasattr(entity, 'broadcast') and entity.broadcast:
            offset = 0
            limit = 100
            
            while True:
                result = await client(GetParticipantsRequest(
                    channel=entity,
                    filter=ChannelParticipantsSearch(''),
                    offset=offset,
                    limit=limit,
                    hash=0
                ))
                
                if not result.users:
                    break
                
                participants.extend(result.users)
                offset += len(result.users)
                
                if len(result.users) < limit:
                    break
        else:
            all_participants = await client.get_participants(entity)
            participants = list(all_participants)
            
    except Exception as e:
        print(f"Ошибка при получении участников: {e}")
    
    return participants

async def get_chat_entity(client, chat_identifier):
    """Пытается получить entity чата разными способами"""
    entity = None
    try:
        # Пробуем как строку
        entity = await client.get_entity(chat_identifier)
    except (ValueError, TypeError):
        # Пробуем как числовой ID
        try:
            chat_id = int(chat_identifier)
            entity = await client.get_entity(chat_id)
        except Exception:
            pass
    except Exception:
        pass
    
    return entity

async def mailing_loop():
    await send_log_to_bot("🔄 Рассылка начата")
    
    while bot_data["is_running"]:
        try:
            # Получаем авторизованные клиенты
            auth_clients = []
            for session_id, client in active_clients.items():
                if bot_data["sessions"].get(session_id, {}).get("is_authorized", False):
                    auth_clients.append(client)

            if not auth_clients:
                await send_log_to_bot("❌ Нет авторизованных сессий")
                await asyncio.sleep(60)
                continue

            # Получаем сообщение для пересылки
            forward_message = None
            if bot_data["settings"]["forward_mode"]:
                forward_message = await get_forward_message()
                if not forward_message:
                    await asyncio.sleep(60)
                    continue

            # Рассылка в ПРИВАТНЫЕ ЧАТЫ
            for chat_identifier in bot_data["private_chats"]:
                for client in auth_clients:
                    try:
                        # Пытаемся получить entity чата
                        entity = await get_chat_entity(client, chat_identifier)
                        
                        if not entity:
                            await send_log_to_bot(f"❌ Не найден чат: {chat_identifier}")
                            continue
                        
                        # Проверяем, можем ли мы отправлять сообщения
                        try:
                            # Тестовая проверка
                            await client.get_participants(entity, limit=1)
                        except UserNotParticipantError:
                            await send_log_to_bot(f"❌ Бот не состоит в чате: {chat_identifier}")
                            continue
                        
                        # Отправляем сообщение
                        if bot_data["settings"]["forward_mode"] and forward_message:
                            await forward_message.forward_to(entity)
                            await send_log_to_bot(f"✅ Сообщение отправлено в: {chat_identifier}")
                        else:
                            message_text = bot_data["settings"]["message_text"]
                            await client.send_message(entity, message_text)
                            await send_log_to_bot(f"✅ Сообщение отправлено в: {chat_identifier}")
                            
                        # Пауза между сообщениями
                        await asyncio.sleep(bot_data["settings"]["message_interval"])
                        
                    except FloodWaitError as e:
                        wait_time = e.seconds
                        await send_log_to_bot(f"⏳ FloodWait {wait_time} сек. для чата {chat_identifier}")
                        await asyncio.sleep(wait_time + 5)
                        continue
                        
                    except ChatWriteForbiddenError:
                        await send_log_to_bot(f"❌ Нет прав в чате: {chat_identifier}")
                        continue
                        
                    except Exception as e:
                        print(f"Ошибка отправки в чат {chat_identifier}: {e}")
                        await send_log_to_bot(f"❌ Ошибка отправки в {chat_identifier}: {type(e).__name__}")
                        continue
            
            # Пауза между циклами
            total_wait = bot_data["settings"]["cycle_interval"]
            await send_log_to_bot(f"⏳ Ожидание {total_wait//60} мин. до следующего цикла")
            
            while total_wait > 0 and bot_data["is_running"]:
                await asyncio.sleep(60)
                total_wait -= 60
            
        except Exception as e:
            import traceback
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА: {type(e).__name__}: {str(e)[:100]}"
            await send_log_to_bot(error_msg)
            print(f"Критическая ошибка: {traceback.format_exc()}")
            await asyncio.sleep(60)

@dp.callback_query(F.data == "set_cycle_interval")
async def set_cycle_interval_start(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text("Введите интервал между циклами (в секундах):",
                                 reply_markup=back_button("settings"))
    await state.set_state(Form.set_cycle_interval)
    await call.answer()

@dp.message(Form.set_cycle_interval)
async def set_cycle_interval_finish(message: types.Message, state: FSMContext):
    try:
        interval = int(message.text)
        if interval < 1:
            raise ValueError
        bot_data["settings"]["cycle_interval"] = interval
        save_data()
        await message.answer(f"✅ Интервал между циклами установлен: {interval} сек.", reply_markup=main_menu_kb())
        await send_log_to_bot(f"✅ Интервал циклов изменен: {interval} сек.")
    except ValueError:
        await message.answer("❌ Неверное значение. Введите целое число больше 0.", reply_markup=back_button("settings"))
    await state.clear()

@dp.callback_query(F.data == "view_info")
async def view_info(call: types.CallbackQuery):
    text = "📊 Информация о рассылке:\n\n"

    auth_sessions = [s for s in bot_data["sessions"].values() if s.get("is_authorized", False)]
    text += f"🔗 Подключенных сессий: {len(auth_sessions)}\n"

    for i, session in enumerate(auth_sessions, 1):
        text += f"{i}. Телефон: {session.get('phone', 'N/A')}\n"

    text += f"\n📢 Публичных каналов: {len(bot_data['channels'])}"
    text += f"\n💬 Приватных чатов: {len(bot_data['private_chats'])}"

    if bot_data["settings"]["forward_mode"]:
        text += f"\n\n🔄 Режим: Пересылка сообщения"
        text += f"\n📂 Источник: {bot_data['settings']['source_channel']}"
        text += f"\n📄 ID сообщения: {bot_data['settings']['source_message_id']}"
    else:
        text += f"\n\n✏️ Режим: Текстовая рассылка"
        if bot_data["settings"]["message_text"]:
            text += f"\n📝 Текст: {bot_data['settings']['message_text'][:100]}..."
        else:
            text += f"\n📝 Текст: Не установлен"

    text += f"\n\n⏱ Интервал между сообщениями: {bot_data['settings']['message_interval']} сек."
    text += f"\n🔄 Интервал между циклами: {bot_data['settings']['cycle_interval']} сек."

    await call.message.edit_text(text, reply_markup=settings_kb())
    await call.answer()

@dp.callback_query(F.data == "add_admin")
async def add_admin_start(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text("Введите ID пользователя для добавления в администраторы:",
                                 reply_markup=back_button("administration"))
    await state.set_state(Form.add_admin)
    await call.answer()

@dp.message(Form.add_admin)
async def add_admin_finish(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        if user_id in bot_data["admins"]:
            await message.answer("❌ Этот пользователь уже является администратором.", reply_markup=main_menu_kb())
        else:
            bot_data["admins"].append(user_id)
            save_data()
            await message.answer("✅ Пользователь добавлен в администраторы!", reply_markup=main_menu_kb())
            await send_log_to_bot(f"✅ Добавлен администратор: {user_id}")
    except ValueError:
        await message.answer("❌ Неверный формат ID. Введите числовой идентификатор.",
                             reply_markup=back_button("administration"))
    await state.clear()

@dp.callback_query(F.data == "list_admins")
async def list_admins(call: types.CallbackQuery):
    text = "👑 Список администраторов:\n\n"
    for i, admin_id in enumerate(bot_data["admins"], 1):
        text += f"{i}. ID: {admin_id}\n"

    await call.message.edit_text(text, reply_markup=administration_kb())
    await call.answer()

@dp.callback_query(F.data == "remove_admin")
async def remove_admin_start(call: types.CallbackQuery, state: FSMContext):
    if len(bot_data["admins"]) <= 1:
        await call.answer("❌ Нельзя удалить единственного администратора!", show_alert=True)
        return

    text = "Введите ID администратора для удаления:\n\n"
    for i, admin_id in enumerate(bot_data["admins"], 1):
        if admin_id != OWNER_ID:
            text += f"{i}. ID: {admin_id}\n"

    await call.message.edit_text(text, reply_markup=back_button("administration"))
    await state.set_state(Form.remove_admin)
    await call.answer()

@dp.message(Form.remove_admin)
async def remove_admin_finish(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        if user_id == OWNER_ID:
            await message.answer("❌ Нельзя удалить владельца бота!", reply_markup=main_menu_kb())
        elif user_id in bot_data["admins"]:
            bot_data["admins"].remove(user_id)
            save_data()
            await message.answer("✅ Администратор удален!", reply_markup=main_menu_kb())
            await send_log_to_bot(f"✅ Удален администратор: {user_id}")
        else:
            await message.answer("❌ Пользователь не найден в списке администраторов.", reply_markup=main_menu_kb())
    except ValueError:
        await message.answer("❌ Неверный формат ID. Введите числовой идентификатор.",
                             reply_markup=back_button("administration"))
    await state.clear()

@dp.callback_query(F.data == "add_session")
async def add_session_start(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text("Введите API ID для новой сессии:", reply_markup=back_button("sessions_menu"))
    await state.set_state(Form.add_session_api_id)
    await call.answer()

@dp.message(Form.add_session_api_id)
async def add_session_api_id(message: types.Message, state: FSMContext):
    try:
        api_id = int(message.text)
        await state.update_data(api_id=api_id)
        await message.answer("Введите API Hash:", reply_markup=back_button("sessions_menu"))
        await state.set_state(Form.add_session_api_hash)
    except ValueError:
        await message.answer("❌ Неверный формат API ID. Введите число.", reply_markup=back_button("sessions_menu"))

@dp.message(Form.add_session_api_hash)
async def add_session_api_hash(message: types.Message, state: FSMContext):
    api_hash = message.text.strip()
    if not api_hash:
        await message.answer("❌ API Hash не может быть пустым.", reply_markup=back_button("sessions_menu"))
        return

    await state.update_data(api_hash=api_hash)
    await message.answer("Введите номер телефона (в формате +79123456789):", reply_markup=back_button("sessions_menu"))
    await state.set_state(Form.add_session_phone)

@dp.message(Form.add_session_phone)
async def add_session_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{11,15}$', phone):
        await message.answer("❌ Неверный формат номера телефона.", reply_markup=back_button("sessions_menu"))
        return

    await state.update_data(phone=phone)
    data = await state.get_data()

    client = TelegramClient(StringSession(), data["api_id"], data["api_hash"])
    await client.connect()

    try:
        sent_code = await client.send_code_request(phone)
        await state.update_data(
            client=client,
            phone_code_hash=sent_code.phone_code_hash
        )
        await message.answer("Код подтверждения отправлен. Введите код:", reply_markup=back_button("sessions_menu"))
        await state.set_state(Form.add_session_code)
    except Exception as e:
        await client.disconnect()
        await message.answer(f"❌ Ошибка при отправке кода: {e}", reply_markup=main_menu_kb())
        await state.clear()

@dp.message(Form.add_session_code)
async def add_session_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    client = data["client"]

    try:
        await client.sign_in(data["phone"], code, phone_code_hash=data["phone_code_hash"])

        session_string = client.session.save()
        session_id = str(len(bot_data["sessions"]) + 1)

        bot_data["sessions"][session_id] = {
            "api_id": data["api_id"],
            "api_hash": data["api_hash"],
            "phone": data["phone"],
            "session_string": session_string,
            "is_authorized": True
        }

        active_clients[session_id] = client

        save_data()
        await message.answer("✅ Сессия успешно добавлена и авторизована!", reply_markup=main_menu_kb())
        await send_log_to_bot(f"✅ Добавлена сессия: {data['phone']}")

    except SessionPasswordNeededError:
        await message.answer("Требуется двухфакторная аутентификация. Введите пароль:",
                             reply_markup=back_button("sessions_menu"))
        await state.set_state(Form.add_session_password)
    except Exception as e:
        await client.disconnect()
        await message.answer(f"❌ Ошибка при авторизации: {e}", reply_markup=main_menu_kb())
        await state.clear()

@dp.message(Form.add_session_password)
async def add_session_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    client = data["client"]

    try:
        await client.sign_in(password=password)

        session_string = client.session.save()
        session_id = str(len(bot_data["sessions"]) + 1)

        bot_data["sessions"][session_id] = {
            "api_id": data["api_id"],
            "api_hash": data["api_hash"],
            "phone": data["phone"],
            "session_string": session_string,
            "is_authorized": True
        }

        active_clients[session_id] = client

        save_data()
        await message.answer("✅ Сессия успешно добавлена и авторизована!", reply_markup=main_menu_kb())
        await send_log_to_bot(f"✅ Добавлена сессия: {data['phone']}")

    except Exception as e:
        await client.disconnect()
        await message.answer(f"❌ Ошибка при авторизации: {e}", reply_markup=main_menu_kb())
    finally:
        await state.clear()

@dp.callback_query(F.data == "list_sessions")
async def list_sessions(call: types.CallbackQuery):
    if not bot_data["sessions"]:
        await call.message.edit_text("❌ Нет добавленных сессий.", reply_markup=sessions_kb())
        await call.answer()
        return

    text = "📱 Список сессий:\n\n"
    for session_id, session_data in bot_data["sessions"].items():
        status = "✅ Авторизована" if session_data.get("is_authorized", False) else "❌ Не авторизована"
        text += f"ID: {session_id}, Телефон: {session_data.get('phone', 'N/A')}, Статус: {status}\n"

    await call.message.edit_text(text, reply_markup=sessions_kb())
    await call.answer()

@dp.callback_query(F.data == "remove_session")
async def remove_session_start(call: types.CallbackQuery, state: FSMContext):
    if not bot_data["sessions"]:
        await call.answer("❌ Нет сессий для удаления!", show_alert=True)
        return

    text = "Введите ID сессии для удаления:\n\n"
    for session_id, session_data in bot_data["sessions"].items():
        status = "✅ Авторизована" if session_data.get("is_authorized", False) else "❌ Не авторизована"
        text += f"ID: {session_id}, Телефон: {session_data.get('phone', 'N/A')}, Статус: {status}\n"

    await call.message.edit_text(text, reply_markup=back_button("sessions_menu"))
    await state.set_state(Form.remove_session)
    await call.answer()

@dp.message(Form.remove_session)
async def remove_session_finish(message: types.Message, state: FSMContext):
    session_id = message.text.strip()

    if session_id in bot_data["sessions"]:
        if session_id in active_clients:
            try:
                await active_clients[session_id].disconnect()
                del active_clients[session_id]
            except:
                pass

        phone = bot_data["sessions"][session_id].get("phone", "Unknown")
        del bot_data["sessions"][session_id]
        save_data()
        await message.answer("✅ Сессия удалена!", reply_markup=main_menu_kb())
        await send_log_to_bot(f"✅ Удалена сессия: {phone}")
    else:
        await message.answer("❌ Сессия с указанным ID не найдена.", reply_markup=main_menu_kb())

    await state.clear()

@dp.callback_query(F.data == "add_channel")
async def add_channel_start(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "Введите ссылки на каналы (в формате https://t.me/...). "
        "Можно ввести несколько ссылки, разделяя их переносом строки или запятыми:",
        reply_markup=back_button("channels")
    )
    await state.set_state(Form.add_channel)
    await call.answer()

@dp.message(Form.add_channel)
async def add_channel_finish(message: types.Message, state: FSMContext):
    text = message.text.strip()

    urls = []
    for line in text.split('\n'):
        for url in line.split(','):
            url = url.strip()
            if url:
                urls.append(url)

    added_channels = []
    existing_channels = []
    invalid_channels = []

    for url in urls:
        if not url.startswith("https://t.me/"):
            invalid_channels.append(url)
            continue

        if url in bot_data["channels"]:
            existing_channels.append(url)
            continue

        bot_data["channels"].append(url)
        added_channels.append(url)

    save_data()

    for channel in added_channels:
        for session_id, client in active_clients.items():
            try:
                entity = await client.get_entity(channel)
                await client(JoinChannelRequest(entity))
                await send_log_to_bot(f"✅ Добавлен канал: {channel}")
            except (ChannelInvalidError, UsernameInvalidError, ValueError) as e:
                print(f"Ошибка присоединения к каналу {channel}: {e}")
                continue

    response = ""
    if added_channels:
        response += "✅ Добавлены каналы:\n" + "\n".join(added_channels) + "\n\n"
    if existing_channels:
        response += "⚠️ Уже были добавлены:\n" + "\n".join(existing_channels) + "\n\n"
    if invalid_channels:
        response += "❌ Неверный формат (должны начинаться с https://t.me/):\n" + "\n".join(invalid_channels) + "\n\n"

    if not added_channels and not existing_channels and not invalid_channels:
        response = "❌ Не было добавлено ни одного канала."

    await message.answer(response, reply_markup=main_menu_kb())
    await state.clear()

@dp.callback_query(F.data == "list_channels")
async def list_channels(call: types.CallbackQuery, page: int = 0):
    if not bot_data["channels"]:
        await call.message.edit_text("❌ Нет добавленных каналов.", reply_markup=channels_kb())
        await call.answer()
        return

    current_pages[call.from_user.id] = page
    total_pages = (len(bot_data["channels"]) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(bot_data["channels"]))

    text = f"📢 Список каналов (страница {page + 1}/{total_pages}):\n\n"
    for i in range(start_idx, end_idx):
        channel = bot_data["channels"][i]
        text += f"{i + 1}. {channel}\n"

    keyboard = []
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"channels_page_{page - 1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"channels_page_{page + 1}"))
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="channels")])

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    await call.answer()

@dp.callback_query(F.data.startswith("channels_page_"))
async def channels_page_handler(call: types.CallbackQuery):
    page = int(call.data.split("_")[2])
    await list_channels(call, page)

@dp.callback_query(F.data == "remove_channel")
async def remove_channel_start(call: types.CallbackQuery, state: FSMContext):
    if not bot_data["channels"] and not bot_data["private_chats"]:
        await call.answer("❌ Нет каналов/чатов для удаления!", show_alert=True)
        return
    
    # Создаем объединенный список
    all_targets = []
    for i, channel in enumerate(bot_data["channels"]):
        all_targets.append({"type": "channel", "value": channel, "index": i})
    
    for i, chat in enumerate(bot_data["private_chats"]):
        all_targets.append({"type": "private_chat", "value": chat, "index": i})
    
    if not all_targets:
        await call.message.edit_text("❌ Нет каналов/чатов для удаления.", reply_markup=channels_kb())
        return
    
    # Показываем первую страницу
    await list_all_targets_for_removal(call, all_targets, 0)
    await state.set_state(Form.remove_channel)
    await call.answer()

async def list_all_targets_for_removal(call: types.CallbackQuery, all_targets, page: int = 0):
    total_pages = (len(all_targets) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_idx = page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(all_targets))
    
    text = f"🗑 Выберите для удаления (страница {page + 1}/{total_pages}):\n\n"
    text += "Введите номера через запятую (например: 1,3,5):\n\n"
    
    for i in range(start_idx, end_idx):
        item = all_targets[i]
        type_icon = "📢" if item["type"] == "channel" else "💬"
        text += f"{i + 1}. {type_icon} {item['value']}\n"
    
    keyboard = []
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"remove_all_page_{page - 1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"remove_all_page_{page + 1}"))
        keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="channels")])
    
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))

@dp.callback_query(F.data.startswith("remove_all_page_"))
async def remove_all_page_handler(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split("_")[3])
    
    # Создаем объединенный список
    all_targets = []
    for i, channel in enumerate(bot_data["channels"]):
        all_targets.append({"type": "channel", "value": channel, "index": i})
    
    for i, chat in enumerate(bot_data["private_chats"]):
        all_targets.append({"type": "private_chat", "value": chat, "index": i})
    
    await list_all_targets_for_removal(call, all_targets, page)
    await call.answer()

@dp.message(Form.remove_channel)
async def remove_channel_finish(message: types.Message, state: FSMContext):
    try:
        numbers = [int(num.strip()) for num in message.text.split(',')]
        numbers.sort(reverse=True)
        
        # Создаем объединенный список
        all_targets = []
        for i, channel in enumerate(bot_data["channels"]):
            all_targets.append({"type": "channel", "value": channel, "index": i})
        
        for i, chat in enumerate(bot_data["private_chats"]):
            all_targets.append({"type": "private_chat", "value": chat, "index": i})
        
        removed_items = []
        
        for num in numbers:
            if 1 <= num <= len(all_targets):
                item_index = num - 1
                item = all_targets[item_index]
                
                if item["type"] == "channel":
                    removed_channel = bot_data["channels"].pop(item["index"])
                    removed_items.append(f"📢 {removed_channel}")
                    await send_log_to_bot(f"✅ Удален канал: {removed_channel}")
                else:
                    removed_chat = bot_data["private_chats"].pop(item["index"])
                    removed_items.append(f"💬 {removed_chat}")
                    await send_log_to_bot(f"✅ Удален приватный чат: {removed_chat}")
        
        if removed_items:
            save_data()
            response = "✅ Удалено:\n" + "\n".join(removed_items)
            await message.answer(response, reply_markup=main_menu_kb())
        else:
            await message.answer("❌ Не было удалено ни одного элемента.", reply_markup=main_menu_kb())
            
    except ValueError:
        await message.answer("❌ Неверный формат. Введите номера через запятую.", reply_markup=main_menu_kb())
    
    await state.clear()

@dp.callback_query(F.data == "start_mailing")
async def start_mailing(call: types.CallbackQuery):
    global mailing_task

    auth_clients = [c for c in active_clients.values()
                    if bot_data["sessions"].get(list(active_clients.keys())[list(active_clients.values()).index(c)],
                                                {}).get("is_authorized", False)]

    if not auth_clients:
        await call.answer("❌ Нет авторизованных сессий!", show_alert=True)
        return

    if not bot_data["channels"] and not bot_data["private_chats"]:
        await call.answer("❌ Нет добавленных каналов или чатов!", show_alert=True)
        return

    bot_data["is_running"] = True
    save_data()

    mailing_task = asyncio.create_task(mailing_loop())
    await call.message.edit_reply_markup(reply_markup=main_menu_kb())
    await call.answer("✅ Рассылка запущена!")
    await send_log_to_bot("✅ Рассылка запущена пользователем")

@dp.callback_query(F.data == "stop_mailing")
async def stop_mailing(call: types.CallbackQuery):
    global mailing_task

    bot_data["is_running"] = False
    save_data()

    if mailing_task:
        mailing_task.cancel()
        mailing_task = None

    await call.message.edit_reply_markup(reply_markup=main_menu_kb())
    await call.answer("⏹ Рассылка остановлена!")
    await send_log_to_bot("⏹ Рассылка остановлена")

async def main():
    load_data()
    
    # Автоматически добавляем ваш приватный чат
    if str(PRIVATE_CHAT_ID) not in bot_data["private_chats"]:
        bot_data["private_chats"].append(str(PRIVATE_CHAT_ID))
        save_data()
    
    # Инициализируем клиенты
    await init_clients()
    
    # Устанавливаем правильные интервалы для одного чата
    bot_data["settings"]["message_interval"] = 1      # 1 секунда
    bot_data["settings"]["cycle_interval"] = 960      # 16 минут
    save_data()
    
    await send_log_to_bot("✅ Бот запущен!")
    await send_log_to_bot(f"📊 Статистика: {len(bot_data['sessions'])} сессий, {len(bot_data['private_chats'])} чатов")
    
    if bot_data["is_running"]:
        global mailing_task
        mailing_task = asyncio.create_task(mailing_loop())
        await send_log_to_bot("🔄 Рассылка автоматически запущена")
    
    try:
        await dp.start_polling(bot)
    except Exception as e:
        await send_log_to_bot(f"❌ Ошибка запуска бота: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())