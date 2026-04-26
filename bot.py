import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import (
    InputPeerEmpty, Channel, Chat, User, PeerChannel, PeerChat, PeerUser
)

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден в переменных окружения!")

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
SUPPORT_USERNAME = "@VestSupport"
MAX_ACCOUNTS = 5

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Хранилище данных
user_sessions: Dict[int, Dict[str, any]] = {}
active_broadcasts: Dict[int, Dict[str, asyncio.Task]] = {}
pending_logins: Dict[int, Dict] = {}

# Временное хранилище для выбора чатов
chat_cache: Dict[int, List] = {}  # user_id -> [(chat_id, chat_name, entity_type), ...]

class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_count = State()
    waiting_for_delay = State()
    selecting_chats = State()

# ID премиум эмодзи
EMOJI = {
    "bot": "6030400221232501136",
    "settings": "5870982283724328568",
    "profile": "5870994129244131212",
    "people": "5870772616305839506",
    "check": "5870633910337015697",
    "cross": "5870657884844462243",
    "gift": "6032644646587338669",
    "write": "5870753782874246579",
    "lock": "6037249452824072506",
    "unlock": "6037496202990194718",
    "trash": "5870875489362513438",
    "back": "5893057118545646106",
    "send": "5963103826075456248",
    "celebration": "6041731551845159060",
    "stats": "5870930636742595124",
    "megaphone": "6039422865189638057",
    "clock": "5983150113483134607",
    "home": "5873147866364514353",
    "file": "5870528606328852614",
    "info": "6028435952299413210",
    "apps": "5778672437122045013",
    "link": "5769289093221454192",
}

def format_emoji(emoji_id: str) -> str:
    """Форматирует премиум эмодзи для HTML"""
    return f'<tg-emoji emoji-id="{emoji_id}"></tg-emoji>'

def safe_html(text: str) -> str:
    """Безопасно форматирует текст с HTML"""
    # Заменяем < и > в обычном тексте
    text = text.replace('<', '&lt;').replace('>', '&gt;')
    return text

# Главное меню - Reply клавиатура
def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(
        text=f"Менеджер аккаунтов",
    )
    builder.button(
        text=f"Функции",
    )
    builder.button(
        text=f"Поддержка",
    )
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

# Инлайн клавиатуры
def get_back_button(callback_data: str = "main_menu"):
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Назад",
        callback_data=callback_data,
        icon_custom_emoji_id=EMOJI["back"]
    )
    return builder.as_markup()

def get_accounts_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Добавить аккаунт",
        callback_data="add_account",
        style="primary",
        icon_custom_emoji_id=EMOJI["gift"]
    )
    builder.button(
        text="Мои аккаунты",
        callback_data="my_accounts",
        style="success",
        icon_custom_emoji_id=EMOJI["profile"]
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id=EMOJI["back"]
    )
    builder.adjust(2, 1)
    return builder.as_markup()

def get_functions_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Рассылка",
        callback_data="broadcast",
        style="primary",
        icon_custom_emoji_id=EMOJI["megaphone"]
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id=EMOJI["back"]
    )
    builder.adjust(1, 1)
    return builder.as_markup()

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    welcome_text = (
        f"{format_emoji(EMOJI['bot'])} <b>Добро пожаловать!</b>\n\n"
        f"<b>Главное меню:</b>\n"
        f"⚙ <b>Менеджер аккаунтов</b> - управление аккаунтами\n"
        f"📊 <b>Функции</b> - рассылка сообщений\n"
        f"📣 <b>Поддержка</b> - связь с поддержкой"
    )
    try:
        await message.answer(
            welcome_text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
    except TelegramBadRequest as e:
        logging.error(f"Error in cmd_start: {e}")
        # Fallback without emoji if they fail
        await message.answer(
            "🤖 <b>Добро пожаловать!</b>\n\n"
            "<b>Главное меню:</b>\n"
            "⚙ <b>Менеджер аккаунтов</b> - управление аккаунтами\n"
            "📊 <b>Функции</b> - рассылка сообщений\n"
            "📣 <b>Поддержка</b> - связь с поддержкой",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )

@dp.message(F.text == "Менеджер аккаунтов")
async def accounts_manager(message: types.Message):
    user_id = message.from_user.id
    accounts_count = len(user_sessions.get(user_id, {}))
    text = (
        f"{format_emoji(EMOJI['settings'])} <b>Менеджер аккаунтов</b>\n"
        f"{format_emoji(EMOJI['profile'])} Активных аккаунтов: {accounts_count}/{MAX_ACCOUNTS}"
    )
    try:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_accounts_menu_keyboard())
    except TelegramBadRequest:
        await message.answer(
            f"⚙ <b>Менеджер аккаунтов</b>\n"
            f"👤 Активных аккаунтов: {accounts_count}/{MAX_ACCOUNTS}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_accounts_menu_keyboard()
        )

@dp.message(F.text == "Функции")
async def functions_menu(message: types.Message):
    text = f"{format_emoji(EMOJI['stats'])} <b>Функции</b>\n{format_emoji(EMOJI['megaphone'])} Здесь вы можете запустить рассылку"
    try:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_functions_keyboard())
    except TelegramBadRequest:
        await message.answer(
            "📊 <b>Функции</b>\n📣 Здесь вы можете запустить рассылку",
            parse_mode=ParseMode.HTML,
            reply_markup=get_functions_keyboard()
        )

@dp.message(F.text == "Поддержка")
async def support(message: types.Message):
    text = f"{format_emoji(EMOJI['megaphone'])} <b>Поддержка</b>\nСвяжитесь с нами: {SUPPORT_USERNAME}"
    try:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except TelegramBadRequest:
        await message.answer(
            f"📣 <b>Поддержка</b>\nСвяжитесь с нами: {SUPPORT_USERNAME}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer(
        f"{format_emoji(EMOJI['bot'])} <b>Главное меню</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if len(user_sessions.get(user_id, {})) >= MAX_ACCOUNTS:
        await callback.answer(
            "❌ Достигнут лимит аккаунтов!",
            show_alert=True
        )
        return
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['apps'])} <b>Добавление аккаунта</b>\n"
        f"{format_emoji(EMOJI['write'])} Введите номер телефона в формате: +7XXXXXXXXXX",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button("accounts_manager")
    )
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await message.answer(
            "❌ Неверный формат номера. Пример: +79123456789",
            reply_markup=get_back_button("accounts_manager")
        )
        return
    
    user_id = message.from_user.id
    
    # Проверяем, не добавлен ли уже этот номер
    if user_id in user_sessions and phone in user_sessions[user_id]:
        await message.answer(
            "❌ Этот аккаунт уже добавлен!",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()
        return
    
    client = TelegramClient(f'sessions/{user_id}_{phone.replace("+", "")}', API_ID, API_HASH)
    
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        phone_code_hash = sent_code.phone_code_hash
        
        pending_logins[user_id] = {
            "client": client,
            "phone": phone,
            "phone_code_hash": phone_code_hash
        }
        
        await message.answer(
            f"{format_emoji(EMOJI['gift'])} Код подтверждения отправлен на {phone}\n"
            f"{format_emoji(EMOJI['write'])} Введите код из SMS:",
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_button("accounts_manager")
        )
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as e:
        await client.disconnect()
        await message.answer(
            f"❌ Ошибка: {str(e)}",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer(
            "❌ Сессия истекла. Начните заново.",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()
        return
    
    login_data = pending_logins[user_id]
    client = login_data["client"]
    phone = login_data["phone"]
    phone_code_hash = login_data["phone_code_hash"]
    
    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        await on_successful_login(user_id, phone, client, message, state)
    except errors.SessionPasswordNeededError:
        await message.answer(
            f"{format_emoji(EMOJI['lock'])} Требуется двухфакторная аутентификация\n"
            f"{format_emoji(EMOJI['write'])} Введите пароль 2FA:",
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_button("accounts_manager")
        )
        await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as e:
        await client.disconnect()
        if user_id in pending_logins:
            del pending_logins[user_id]
        await message.answer(
            f"❌ Ошибка: {str(e)}",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer(
            "❌ Сессия истекла. Начните заново.",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()
        return
    
    login_data = pending_logins[user_id]
    client = login_data["client"]
    phone = login_data["phone"]
    
    try:
        await client.sign_in(password=password)
        await on_successful_login(user_id, phone, client, message, state)
    except Exception as e:
        await client.disconnect()
        if user_id in pending_logins:
            del pending_logins[user_id]
        await message.answer(
            f"❌ Ошибка 2FA: {str(e)}",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

async def on_successful_login(user_id, phone, client, message, state):
    if user_id not in user_sessions:
        user_sessions[user_id] = {}
    user_sessions[user_id][phone] = {"client": client, "phone": phone}
    
    if user_id in pending_logins:
        del pending_logins[user_id]
    
    await message.answer(
        f"{format_emoji(EMOJI['check'])} Аккаунт {phone} успешно добавлен!",
        parse_mode=ParseMode.HTML,
        reply_markup=get_accounts_menu_keyboard()
    )
    await state.clear()

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    
    if not accounts:
        await callback.message.edit_text(
            f"{format_emoji(EMOJI['unlock'])} <b>Мои аккаунты</b>\n"
            f"{format_emoji(EMOJI['cross'])} У вас нет добавленных аккаунтов",
            parse_mode=ParseMode.HTML,
            reply_markup=get_accounts_menu_keyboard()
        )
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for phone in accounts:
        builder.button(
            text=f"📱 {phone}",
            callback_data=f"account_info_{phone}",
            style="default"
        )
    builder.button(
        text="Назад",
        callback_data="accounts_manager",
        icon_custom_emoji_id=EMOJI["back"]
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['profile'])} <b>Мои аккаунты</b>\n"
        f"{format_emoji(EMOJI['unlock'])} Выберите аккаунт для просмотра:",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("account_info_"))
async def account_info(callback: types.CallbackQuery):
    phone = callback.data.replace("account_info_", "")
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Удалить аккаунт",
        callback_data=f"delete_account_{phone}",
        style="danger",
        icon_custom_emoji_id=EMOJI["trash"]
    )
    builder.button(
        text="Назад",
        callback_data="my_accounts",
        icon_custom_emoji_id=EMOJI["back"]
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['profile'])} <b>Аккаунт:</b> {phone}\n"
        f"{format_emoji(EMOJI['unlock'])} Статус: активен",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account(callback: types.CallbackQuery):
    phone = callback.data.replace("delete_account_", "")
    user_id = callback.from_user.id
    
    if user_id in user_sessions and phone in user_sessions[user_id]:
        try:
            await user_sessions[user_id][phone]["client"].disconnect()
        except:
            pass
        del user_sessions[user_id][phone]
    
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['check'])} Аккаунт {phone} удален",
        parse_mode=ParseMode.HTML,
        reply_markup=get_accounts_menu_keyboard()
    )
    await callback.answer("✅ Аккаунт удален!", show_alert=True)

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts_count = len(user_sessions.get(user_id, {}))
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['settings'])} <b>Менеджер аккаунтов</b>\n"
        f"{format_emoji(EMOJI['profile'])} Активных аккаунтов: {accounts_count}/{MAX_ACCOUNTS}",
        parse_mode=ParseMode.HTML,
        reply_markup=get_accounts_menu_keyboard()
    )
    await callback.answer()

# ============ РАССЫЛКА ============

@dp.callback_query(F.data == "broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Новая рассылка",
        callback_data="new_broadcast",
        style="primary",
        icon_custom_emoji_id=EMOJI["megaphone"]
    )
    builder.button(
        text="Активные рассылки",
        callback_data="active_broadcasts",
        style="default",
        icon_custom_emoji_id=EMOJI["clock"]
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id=EMOJI["back"]
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['megaphone'])} <b>Рассылка</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "new_broadcast")
async def new_broadcast(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not user_sessions.get(user_id):
        await callback.answer(
            "❌ Сначала добавьте аккаунт!",
            show_alert=True
        )
        return
    
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['write'])} <b>Отправьте сообщение для рассылки:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button("broadcast")
    )
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()

@dp.message(BroadcastStates.waiting_for_message)
async def broadcast_get_message(message: types.Message, state: FSMContext):
    await state.update_data(message_text=message.text or message.caption or "")
    await message.answer(
        f"{format_emoji(EMOJI['write'])} <b>Введите количество сообщений для отправки в каждый чат:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button("broadcast")
    )
    await state.set_state(BroadcastStates.waiting_for_count)

@dp.message(BroadcastStates.waiting_for_count)
async def broadcast_get_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if count < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите целое положительное число")
        return
    
    await state.update_data(message_count=count)
    await message.answer(
        f"{format_emoji(EMOJI['write'])} <b>Введите задержку между сообщениями (в секундах):</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button("broadcast")
    )
    await state.set_state(BroadcastStates.waiting_for_delay)

@dp.message(BroadcastStates.waiting_for_delay)
async def broadcast_get_delay(message: types.Message, state: FSMContext):
    try:
        delay = float(message.text)
        if delay < 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите положительное число")
        return
    
    await state.update_data(delay=delay, selected_chats=[], current_page=0)
    
    # Загружаем диалоги
    user_id = message.from_user.id
    await message.answer("🔄 Загружаем чаты...")
    
    await load_dialogs_to_cache(user_id)
    await show_chat_selection(message, state, 0)

async def load_dialogs_to_cache(user_id: int):
    """Загружает все диалоги в кеш"""
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        chat_cache[user_id] = []
        return
    
    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]
    
    try:
        dialogs = await client.get_dialogs(limit=200)
        chats = []
        for dialog in dialogs:
            chat = dialog.entity
            if isinstance(chat, User):
                chat_name = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
                chat_id = f"user_{chat.id}"
            elif isinstance(chat, (Chat, Channel)):
                chat_name = chat.title
                chat_id = f"chat_{chat.id}"
            else:
                continue
            chats.append((chat_id, chat_name))
        chat_cache[user_id] = chats
    except Exception as e:
        logging.error(f"Error loading dialogs: {e}")
        chat_cache[user_id] = []

async def show_chat_selection(message: types.Message, state: FSMContext, page: int):
    user_id = message.from_user.id
    chats = chat_cache.get(user_id, [])
    per_page = 10
    start = page * per_page
    end = start + per_page
    page_chats = chats[start:end]
    has_more = end < len(chats)
    
    data = await state.get_data()
    selected = data.get("selected_chats", [])
    
    builder = InlineKeyboardBuilder()
    for chat_id, chat_name in page_chats:
        is_selected = chat_id in selected
        prefix = "✅ " if is_selected else ""
        builder.button(
            text=f"{prefix}{chat_name[:30]}",
            callback_data=f"select_chat_{chat_id}",
            style="success" if is_selected else "default"
        )
    
    # Навигация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(types.InlineKeyboardButton(
            text="◀ Назад",
            callback_data=f"chats_page_{page - 1}",
        ))
    if has_more:
        nav_buttons.append(types.InlineKeyboardButton(
            text="Вперед ▶",
            callback_data=f"chats_page_{page + 1}",
        ))
    if nav_buttons:
        builder.row(*nav_buttons)
    
    # Кнопки действий
    if 0 < len(selected) <= 5:
        builder.button(
            text=f"Запустить рассылку ({len(selected)})",
            callback_data="start_broadcast",
            style="primary",
            icon_custom_emoji_id=EMOJI["send"]
        )
    
    builder.button(
        text="Отмена",
        callback_data="cancel_broadcast",
        style="danger",
        icon_custom_emoji_id=EMOJI["cross"]
    )
    builder.adjust(1)
    
    await state.update_data(current_page=page)
    await message.edit_text(
        f"{format_emoji(EMOJI['people'])} <b>Выберите чаты для рассылки (до 5):</b>\n"
        f"Выбрано: {len(selected)}/5 | Страница {page + 1}",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("select_chat_"))
async def select_chat(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("select_chat_", "")
    data = await state.get_data()
    selected = data.get("selected_chats", [])
    
    if chat_id in selected:
        selected.remove(chat_id)
    else:
        if len(selected) >= 5:
            await callback.answer("❌ Максимум 5 чатов!", show_alert=True)
            return
        selected.append(chat_id)
    
    await state.update_data(selected_chats=selected)
    current_page = data.get("current_page", 0)
    await show_chat_selection(callback.message, state, current_page)
    await callback.answer()

@dp.callback_query(F.data.startswith("chats_page_"))
async def chats_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.replace("chats_page_", ""))
    await state.update_data(current_page=page)
    await show_chat_selection(callback.message, state, page)
    await callback.answer()

@dp.callback_query(F.data == "start_broadcast")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_chats = data.get("selected_chats", [])
    
    if not selected_chats:
        await callback.answer("❌ Выберите хотя бы один чат!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    broadcast_id = f"broadcast_{datetime.now().timestamp()}"
    
    if user_id not in active_broadcasts:
        active_broadcasts[user_id] = {}
    
    task = asyncio.create_task(
        execute_broadcast(
            user_id=user_id,
            broadcast_id=broadcast_id,
            chats=selected_chats,
            message_text=data.get("message_text", ""),
            count=data.get("message_count", 1),
            delay=data.get("delay", 1)
        )
    )
    active_broadcasts[user_id][broadcast_id] = task
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Остановить рассылку",
        callback_data=f"stop_broadcast_{broadcast_id}",
        style="danger",
        icon_custom_emoji_id=EMOJI["cross"]
    )
    builder.button(
        text="В главное меню",
        callback_data="main_menu",
        icon_custom_emoji_id=EMOJI["home"]
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['send'])} <b>Рассылка запущена!</b>\n"
        f"ID: {broadcast_id[:10]}...\n"
        f"Чатов: {len(selected_chats)}\n"
        f"Сообщений в каждый чат: {data.get('message_count', 1)}",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.clear()
    await callback.answer("🎉 Рассылка запущена!", show_alert=True)

async def execute_broadcast(user_id, broadcast_id, chats, message_text, count, delay):
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        return
    
    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]
    
    try:
        for chat_id_str in chats:
            if broadcast_id not in active_broadcasts.get(user_id, {}):
                break
            
            try:
                if chat_id_str.startswith("user_"):
                    entity = await client.get_entity(int(chat_id_str.replace("user_", "")))
                elif chat_id_str.startswith("chat_"):
                    entity = await client.get_entity(int(chat_id_str.replace("chat_", "")))
                else:
                    continue
            except:
                continue
            
            for i in range(count):
                if broadcast_id not in active_broadcasts.get(user_id, {}):
                    break
                
                try:
                    await client.send_message(entity, message_text)
                    await asyncio.sleep(delay)
                except Exception as e:
                    logging.error(f"Broadcast error: {e}")
                    continue
        
        if user_id in active_broadcasts and broadcast_id in active_broadcasts[user_id]:
            del active_broadcasts[user_id][broadcast_id]
    except Exception as e:
        logging.error(f"Broadcast fatal error: {e}")

@dp.callback_query(F.data == "active_broadcasts")
async def active_broadcasts_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    broadcasts = active_broadcasts.get(user_id, {})
    
    if not broadcasts:
        builder = InlineKeyboardBuilder()
        builder.button(
            text="Назад",
            callback_data="broadcast",
            icon_custom_emoji_id=EMOJI["back"]
        )
        await callback.message.edit_text(
            f"{format_emoji(EMOJI['unlock'])} <b>Нет активных рассылок</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=builder.as_markup()
        )
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for bid in broadcasts:
        builder.button(
            text=f"Остановить {bid[:10]}...",
            callback_data=f"stop_broadcast_{bid}",
            style="danger",
            icon_custom_emoji_id=EMOJI["cross"]
        )
    builder.button(
        text="Назад",
        callback_data="broadcast",
        icon_custom_emoji_id=EMOJI["back"]
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['clock'])} <b>Активные рассылки:</b> {len(broadcasts)}",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_broadcast_"))
async def stop_broadcast(callback: types.CallbackQuery):
    broadcast_id = callback.data.replace("stop_broadcast_", "")
    user_id = callback.from_user.id
    
    if user_id in active_broadcasts and broadcast_id in active_broadcasts[user_id]:
        active_broadcasts[user_id][broadcast_id].cancel()
        del active_broadcasts[user_id][broadcast_id]
        await callback.answer("✅ Рассылка остановлена!", show_alert=True)
    else:
        await callback.answer("❌ Рассылка не найдена", show_alert=True)
    
    await active_broadcasts_list(callback)

@dp.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        f"{format_emoji(EMOJI['cross'])} <b>Рассылка отменена</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

async def main():
    os.makedirs("sessions", exist_ok=True)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except:
        pass
    logging.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
