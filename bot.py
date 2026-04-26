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
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import (
    InputPeerEmpty, Channel, Chat, User, PeerChannel, PeerChat, PeerUser
)

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
SUPPORT_USERNAME = "@VestSupport"
MAX_ACCOUNTS = 5

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Хранилище данных
user_sessions: Dict[int, Dict[str, any]] = {}  # user_id -> {phone: {"client": TelegramClient, "phone": str}}
active_broadcasts: Dict[int, Dict[str, asyncio.Task]] = {}  # user_id -> {broadcast_id: task}

# Временное хранилище для процесса авторизации
pending_logins: Dict[int, Dict] = {}  # user_id -> {"client": client, "phone": phone, "phone_code_hash": hash}

class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_count = State()
    waiting_for_delay = State()
    selecting_chats = State()

def get_premium_emoji(emoji_id: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">'

def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(
        text="Менеджер аккаунтов",
        icon_custom_emoji_id="5870982283724328568"
    )
    builder.button(
        text="Функции",
        icon_custom_emoji_id="5870930636742595124"
    )
    builder.button(
        text="Поддержка",
        icon_custom_emoji_id="6039422865189638057"
    )
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_back_button():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id="5893057118545646106"
    )
    return builder.as_markup()

def get_accounts_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Добавить аккаунт",
        callback_data="add_account",
        style="primary",
        icon_custom_emoji_id="6032644646587338669"
    )
    builder.button(
        text="Мои аккаунты",
        callback_data="my_accounts",
        style="success",
        icon_custom_emoji_id="5870994129244131212"
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id="5893057118545646106"
    )
    builder.adjust(2, 1)
    return builder.as_markup()

def get_functions_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Рассылка",
        callback_data="broadcast",
        style="primary",
        icon_custom_emoji_id="6039422865189638057"
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id="5893057118545646106"
    )
    builder.adjust(1, 1)
    return builder.as_markup()

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    welcome_text = (
        f"{get_premium_emoji('6030400221232501136')} Добро пожаловать!\n\n"
        f"<b>Главное меню:</b>\n"
        f"{get_premium_emoji('5870982283724328568')} <b>Менеджер аккаунтов</b> - управление аккаунтами\n"
        f"{get_premium_emoji('5870930636742595124')} <b>Функции</b> - рассылка сообщений\n"
        f"{get_premium_emoji('6039422865189638057')} <b>Поддержка</b> - связь с поддержкой"
    )
    await message.answer(
        welcome_text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(F.text == "Менеджер аккаунтов")
async def accounts_manager(message: types.Message):
    user_id = message.from_user.id
    accounts_count = len(user_sessions.get(user_id, {}))
    text = (
        f"{get_premium_emoji('5870982283724328568')} <b>Менеджер аккаунтов</b>\n"
        f"{get_premium_emoji('5870994129244131212')} Активных аккаунтов: {accounts_count}/{MAX_ACCOUNTS}"
    )
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_accounts_menu_keyboard())

@dp.message(F.text == "Функции")
async def functions_menu(message: types.Message):
    text = (
        f"{get_premium_emoji('5870930636742595124')} <b>Функции</b>\n"
        f"{get_premium_emoji('6039422865189638057')} Здесь вы можете запустить рассылку"
    )
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_functions_keyboard())

@dp.message(F.text == "Поддержка")
async def support(message: types.Message):
    text = (
        f"{get_premium_emoji('6039422865189638057')} <b>Поддержка</b>\n"
        f"Свяжитесь с нами: {SUPPORT_USERNAME}"
    )
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        f"{get_premium_emoji('6030400221232501136')} <b>Главное меню</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if len(user_sessions.get(user_id, {})) >= MAX_ACCOUNTS:
        await callback.answer(
            f"{get_premium_emoji('5870657884844462243')} Достигнут лимит аккаунтов!",
            show_alert=True
        )
        return
    await callback.message.edit_text(
        f"{get_premium_emoji('5778672437122045013')} <b>Добавление аккаунта</b>\n"
        f"{get_premium_emoji('5870753782874246579')} Введите номер телефона в формате: +7XXXXXXXXXX",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button()
    )
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Неверный формат номера. Пример: +79123456789",
            parse_mode=ParseMode.HTML
        )
        return
    
    user_id = message.from_user.id
    client = TelegramClient(f'sessions/{user_id}_{phone}', API_ID, API_HASH)
    await client.connect()
    
    try:
        sent_code = await client.send_code_request(phone)
        phone_code_hash = sent_code.phone_code_hash
        
        pending_logins[user_id] = {
            "client": client,
            "phone": phone,
            "phone_code_hash": phone_code_hash
        }
        
        await message.answer(
            f"{get_premium_emoji('6032644646587338669')} Код подтверждения отправлен на {phone}\n"
            f"{get_premium_emoji('5870753782874246579')} Введите код из SMS:",
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_button()
        )
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as e:
        await client.disconnect()
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Ошибка: {str(e)}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Сессия истекла. Начните заново.",
            parse_mode=ParseMode.HTML,
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
            f"{get_premium_emoji('6037249452824072506')} Требуется двухфакторная аутентификация\n"
            f"{get_premium_emoji('5870753782874246579')} Введите пароль 2FA:",
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_button()
        )
        await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as e:
        await client.disconnect()
        del pending_logins[user_id]
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Ошибка: {str(e)}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Сессия истекла. Начните заново.",
            parse_mode=ParseMode.HTML,
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
        del pending_logins[user_id]
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Ошибка 2FA: {str(e)}",
            parse_mode=ParseMode.HTML,
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
        f"{get_premium_emoji('5870633910337015697')} Аккаунт {phone} успешно добавлен!",
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
            f"{get_premium_emoji('6037496202990194718')} <b>Мои аккаунты</b>\n"
            f"{get_premium_emoji('5870657884844462243')} У вас нет добавленных аккаунтов",
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
        icon_custom_emoji_id="5893057118545646106"
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{get_premium_emoji('5870994129244131212')} <b>Мои аккаунты</b>\n"
        f"{get_premium_emoji('6037496202990194718')} Выберите аккаунт для просмотра:",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("account_info_"))
async def account_info(callback: types.CallbackQuery):
    phone = callback.data.replace("account_info_", "")
    user_id = callback.from_user.id
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Удалить аккаунт",
        callback_data=f"delete_account_{phone}",
        style="danger",
        icon_custom_emoji_id="5870875489362513438"
    )
    builder.button(
        text="Назад",
        callback_data="my_accounts",
        icon_custom_emoji_id="5893057118545646106"
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{get_premium_emoji('5870994129244131212')} <b>Аккаунт:</b> {phone}\n"
        f"{get_premium_emoji('6037496202990194718')} Статус: активен",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account(callback: types.CallbackQuery):
    phone = callback.data.replace("delete_account_", "")
    user_id = callback.from_user.id
    
    if user_id in user_sessions and phone in user_sessions[user_id]:
        await user_sessions[user_id][phone]["client"].disconnect()
        del user_sessions[user_id][phone]
    
    await callback.message.edit_text(
        f"{get_premium_emoji('5870633910337015697')} Аккаунт {phone} удален",
        parse_mode=ParseMode.HTML,
        reply_markup=get_accounts_menu_keyboard()
    )
    await callback.answer(
        f"{get_premium_emoji('5870633910337015697')} Аккаунт удален!",
        show_alert=True
    )

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts_count = len(user_sessions.get(user_id, {}))
    await callback.message.edit_text(
        f"{get_premium_emoji('5870982283724328568')} <b>Менеджер аккаунтов</b>\n"
        f"{get_premium_emoji('5870994129244131212')} Активных аккаунтов: {accounts_count}/{MAX_ACCOUNTS}",
        parse_mode=ParseMode.HTML,
        reply_markup=get_accounts_menu_keyboard()
    )
    await callback.answer()

# РАССЫЛКА
@dp.callback_query(F.data == "broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Новая рассылка",
        callback_data="new_broadcast",
        style="primary",
        icon_custom_emoji_id="6039422865189638057"
    )
    builder.button(
        text="Активные рассылки",
        callback_data="active_broadcasts",
        style="default",
        icon_custom_emoji_id="5983150113483134607"
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id="5893057118545646106"
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{get_premium_emoji('6039422865189638057')} <b>Рассылка</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "new_broadcast")
async def new_broadcast(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not user_sessions.get(user_id):
        await callback.answer(
            f"{get_premium_emoji('5870657884844462243')} Сначала добавьте аккаунт!",
            show_alert=True
        )
        return
    
    await callback.message.edit_text(
        f"{get_premium_emoji('5870753782874246579')} <b>Отправьте сообщение для рассылки:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button()
    )
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()

@dp.message(BroadcastStates.waiting_for_message)
async def broadcast_get_message(message: types.Message, state: FSMContext):
    await state.update_data(message_text=message.text or message.caption or "")
    await state.update_data(message_entities=message.entities)
    await message.answer(
        f"{get_premium_emoji('5870753782874246579')} <b>Введите количество сообщений для отправки в каждый чат:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button()
    )
    await state.set_state(BroadcastStates.waiting_for_count)

@dp.message(BroadcastStates.waiting_for_count)
async def broadcast_get_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if count < 1:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Введите целое положительное число",
            parse_mode=ParseMode.HTML
        )
        return
    
    await state.update_data(message_count=count)
    await message.answer(
        f"{get_premium_emoji('5870753782874246579')} <b>Введите задержку между сообщениями (в секундах):</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_button()
    )
    await state.set_state(BroadcastStates.waiting_for_delay)

@dp.message(BroadcastStates.waiting_for_delay)
async def broadcast_get_delay(message: types.Message, state: FSMContext):
    try:
        delay = float(message.text)
        if delay < 0:
            raise ValueError
    except ValueError:
        await message.answer(
            f"{get_premium_emoji('5870657884844462243')} Введите положительное число",
            parse_mode=ParseMode.HTML
        )
        return
    
    await state.update_data(delay=delay, selected_chats=[], current_page=0)
    await show_chat_selection(message, state, 0)

async def get_dialogs_page(user_id: int, page: int) -> Tuple[List, bool]:
    """Получает страницу диалогов. Возвращает (диалоги, есть_ли_еще)"""
    per_page = 10
    offset = page * per_page
    
    # Берем первый доступный аккаунт для получения диалогов
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        return [], False
    
    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]
    
    try:
        dialogs = await client.get_dialogs(limit=per_page, offset_date=None, offset_id=0, 
                                           offset_peer=InputPeerEmpty(), ignore_pinned=True)
        # Симуляция пагинации
        if offset >= len(dialogs):
            return [], False
        page_dialogs = dialogs[offset:offset + per_page]
        has_more = offset + per_page < len(dialogs)
        return page_dialogs, has_more
    except:
        return [], False

async def show_chat_selection(message: types.Message, state: FSMContext, page: int):
    user_id = message.from_user.id
    dialogs, has_more = await get_dialogs_page(user_id, page)
    data = await state.get_data()
    selected = data.get("selected_chats", [])
    
    builder = InlineKeyboardBuilder()
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
            icon_custom_emoji_id="5893057118545646106"
        ))
    if has_more:
        nav_buttons.append(types.InlineKeyboardButton(
            text="Вперед ▶",
            callback_data=f"chats_page_{page + 1}",
            icon_custom_emoji_id="5963103826075456248"
        ))
    if nav_buttons:
        builder.row(*nav_buttons)
    
    # Кнопки действий
    if len(selected) > 0 and len(selected) <= 5:
        builder.button(
            text=f"Запустить рассылку ({len(selected)})",
            callback_data="start_broadcast",
            style="primary",
            icon_custom_emoji_id="5963103826075456248"
        )
    
    builder.button(
        text="Отмена",
        callback_data="cancel_broadcast",
        style="danger",
        icon_custom_emoji_id="5870657884844462243"
    )
    builder.adjust(1)
    
    await state.update_data(current_page=page)
    await message.edit_text(
        f"{get_premium_emoji('5870772616305839506')} <b>Выберите чаты для рассылки (до 5):</b>\n"
        f"Выбрано: {len(selected)}/5",
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
            await callback.answer(
                f"{get_premium_emoji('5870657884844462243')} Максимум 5 чатов!",
                show_alert=True
            )
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
        await callback.answer(
            f"{get_premium_emoji('5870657884844462243')} Выберите хотя бы один чат!",
            show_alert=True
        )
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
            message_entities=data.get("message_entities"),
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
        icon_custom_emoji_id="5870657884844462243"
    )
    builder.button(
        text="В главное меню",
        callback_data="main_menu",
        icon_custom_emoji_id="5873147866364514353"
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{get_premium_emoji('5963103826075456248')} <b>Рассылка запущена!</b>\n"
        f"ID: {broadcast_id[:10]}...\n"
        f"Чатов: {len(selected_chats)}\n"
        f"Сообщений в каждый чат: {data.get('message_count', 1)}",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.clear()
    await callback.answer(
        f"{get_premium_emoji('6041731551845159060')} Рассылка запущена!",
        show_alert=True
    )

async def execute_broadcast(user_id, broadcast_id, chats, message_text, message_entities, count, delay):
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        return
    
    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]
    
    try:
        for chat_id_str in chats:
            if broadcast_id not in active_broadcasts.get(user_id, {}):
                break  # Рассылка остановлена
            
            # Получаем сущность чата
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
                    await client.send_message(
                        entity,
                        message_text,
                        formatting_entities=message_entities
                    )
                    await asyncio.sleep(delay)
                except Exception as e:
                    logging.error(f"Broadcast error: {e}")
                    continue
        
        # Удаляем завершенную рассылку
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
            icon_custom_emoji_id="5893057118545646106"
        )
        await callback.message.edit_text(
            f"{get_premium_emoji('6037496202990194718')} <b>Нет активных рассылок</b>",
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
            icon_custom_emoji_id="5870657884844462243"
        )
    builder.button(
        text="Назад",
        callback_data="broadcast",
        icon_custom_emoji_id="5893057118545646106"
    )
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"{get_premium_emoji('5983150113483134607')} <b>Активные рассылки:</b> {len(broadcasts)}",
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
        await callback.answer(
            f"{get_premium_emoji('5870633910337015697')} Рассылка остановлена!",
            show_alert=True
        )
    else:
        await callback.answer(
            f"{get_premium_emoji('5870657884844462243')} Рассылка не найдена",
            show_alert=True
        )
    
    await active_broadcasts_list(callback)

@dp.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        f"{get_premium_emoji('5870657884844462243')} <b>Рассылка отменена</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

async def main():
    os.makedirs("sessions", exist_ok=True)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
