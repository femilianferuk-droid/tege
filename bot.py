import asyncio
import logging
import os
import re
import random
from datetime import datetime
from typing import Dict, List, Tuple, Optional

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from telethon import TelegramClient, errors, events
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import Channel, Chat, User, ReactionEmoji
from telethon.errors import FloodWaitError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
SUPPORT_USERNAME = "@VestSupport"
ADMIN_ID = 7973988177
BUY_ACCOUNTS = "@v3estnikov"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

user_sessions: Dict[int, Dict[str, dict]] = {}
active_broadcasts: Dict[int, Dict[str, asyncio.Task]] = {}
active_reactions: Dict[int, Dict[str, Dict]] = {}
pending_logins: Dict[int, Dict] = {}
dialogs_cache: Dict[int, Dict[str, List[Tuple[str, str]]]] = {}
user_chat_messages: Dict[int, List[int]] = {}
user_selected_account: Dict[int, str] = {}

BASE_REACTIONS = [
    ("👍", "👍"),
    ("👎", "👎"),
    ("❤", "❤"),
    ("🔥", "🔥"),
    ("🥰", "🥰"),
    ("😁", "😁"),
]

class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class BroadcastStates(StatesGroup):
    adding_messages = State()
    waiting_for_count = State()
    waiting_for_delay = State()
    selecting_chats = State()
    selecting_mode = State()

class JoinStates(StatesGroup):
    waiting_for_usernames = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_confirm = State()

class ReactionStates(StatesGroup):
    selecting_chats = State()
    waiting_for_delay = State()
    selecting_type = State()
    selecting_reaction = State()

E = {
    "settings": "5870982283724328568",
    "profile": "5870994129244131212",
    "people": "5870772616305839506",
    "file": "5870528606328852614",
    "stats": "5870930636742595124",
    "home": "5873147866364514353",
    "lock": "6037249452824072506",
    "unlock": "6037496202990194718",
    "megaphone": "6039422865189638057",
    "check": "5870633910337015697",
    "cross": "5870657884844462243",
    "trash": "5870875489362513438",
    "back": "5893057118545646106",
    "link": "5769289093221454192",
    "info": "6028435952299413210",
    "bot": "6030400221232501136",
    "eye": "6037397706505195857",
    "send": "5963103826075456248",
    "gift": "6032644646587338669",
    "clock": "5983150113483134607",
    "celebration": "6041731551845159060",
    "write": "5870753782874246579",
    "apps": "5778672437122045013",
    "code": "5940433880585605708",
    "loading": "5345906554510012647",
    "user_check": "5891207662678317861",
    "wallet": "5769126056262898415",
    "notification": "6039486778597970865",
    "smile": "5870764288364252592",
}

def em(key: str) -> str:
    return f'<tg-emoji emoji-id="{E[key]}"></tg-emoji>'

def get_main_keyboard():
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="Менеджер аккаунтов", icon_custom_emoji_id=E["settings"]),
                types.KeyboardButton(text="Функции", icon_custom_emoji_id=E["stats"]),
            ],
            [
                types.KeyboardButton(text="Поддержка", icon_custom_emoji_id=E["megaphone"]),
            ]
        ],
        resize_keyboard=True
    )

def get_accounts_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Добавить аккаунт", callback_data="add_account", style="primary", icon_custom_emoji_id=E["gift"])
    builder.button(text="Мои аккаунты", callback_data="my_accounts", style="success", icon_custom_emoji_id=E["profile"])
    builder.button(text="Выбрать аккаунт", callback_data="select_account", style="default", icon_custom_emoji_id=E["user_check"])
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(2, 1)
    return builder.as_markup()

def get_functions_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Рассылка", callback_data="broadcast", style="primary", icon_custom_emoji_id=E["megaphone"])
    builder.button(text="Вступление в чаты", callback_data="join_chats", style="success", icon_custom_emoji_id=E["people"])
    builder.button(text="Масс реакции", callback_data="mass_reactions", style="primary", icon_custom_emoji_id=E["smile"])
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Статистика", callback_data="admin_stats", style="primary", icon_custom_emoji_id=E["stats"])
    builder.button(text="Рассылка всем", callback_data="admin_broadcast", style="success", icon_custom_emoji_id=E["megaphone"])
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)
    return builder.as_markup()

def get_back_keyboard(callback_data: str = "main_menu"):
    builder = InlineKeyboardBuilder()
    builder.button(text="Назад", callback_data=callback_data, style="default", icon_custom_emoji_id=E["back"])
    return builder.as_markup()

def get_mode_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Одновременная", callback_data="mode_sync", style="primary", icon_custom_emoji_id=E["send"])
    builder.button(text="Рандомная", callback_data="mode_random", style="success", icon_custom_emoji_id=E["loading"])
    builder.button(text="Назад", callback_data="cancel_broadcast", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(2, 1)
    return builder.as_markup()

def get_message_actions_keyboard(messages_count: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="Добавить сообщение", callback_data="add_msg", style="primary", icon_custom_emoji_id=E["write"])
    builder.button(text="Запустить рассылку", callback_data="start_msg_config", style="success", icon_custom_emoji_id=E["send"])
    builder.button(text="Отмена", callback_data="cancel_broadcast", style="danger", icon_custom_emoji_id=E["cross"])
    builder.adjust(1)
    return builder.as_markup()

def get_reaction_type_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Всё подряд", callback_data="rtype_all", style="primary", icon_custom_emoji_id=E["send"])
    builder.button(text="Через один", callback_data="rtype_skip", style="success", icon_custom_emoji_id=E["loading"])
    builder.button(text="Отмена", callback_data="cancel_reactions", style="danger", icon_custom_emoji_id=E["cross"])
    builder.adjust(2, 1)
    return builder.as_markup()

def get_reactions_keyboard():
    builder = InlineKeyboardBuilder()
    for emoji_text, emoji_data in BASE_REACTIONS:
        builder.button(
            text=emoji_text,
            callback_data="react_" + emoji_text,
            style="default"
        )
    builder.button(text="Отмена", callback_data="cancel_reactions", style="danger", icon_custom_emoji_id=E["cross"])
    builder.adjust(3, 3, 1)
    return builder.as_markup()

async def safe_send(chat_id: int, text: str, reply_markup=None, **kwargs):
    try:
        return await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup, **kwargs)
    except TelegramBadRequest as e:
        if "can't parse" in str(e):
            clean = re.sub(r'<tg-emoji[^>]*></tg-emoji>', '', text)
            clean = re.sub(r'<[^>]+>', '', clean)
            return await bot.send_message(chat_id, clean, reply_markup=reply_markup, **kwargs)
        raise

async def safe_edit(message: types.Message, text: str, reply_markup=None):
    try:
        return await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        err = str(e)
        if "message can't be edited" in err or "message is not modified" in err:
            try:
                await message.delete()
            except:
                pass
            return await safe_send(message.chat.id, text, reply_markup=reply_markup)
        if "can't parse" in err:
            try:
                await message.delete()
            except:
                pass
            clean = re.sub(r'<tg-emoji[^>]*></tg-emoji>', '', text)
            clean = re.sub(r'<[^>]+>', '', clean)
            return await safe_send(message.chat.id, clean, reply_markup=reply_markup)
        raise
    except Exception:
        try:
            await message.delete()
        except:
            pass
        return await safe_send(message.chat.id, text, reply_markup=reply_markup)

async def delete_user_chat_messages(user_id: int):
    if user_id in user_chat_messages:
        for msg_id in user_chat_messages[user_id]:
            try:
                await bot.delete_message(user_id, msg_id)
            except:
                pass
        user_chat_messages[user_id] = []

async def add_user_chat_message(user_id: int, message_id: int):
    if user_id not in user_chat_messages:
        user_chat_messages[user_id] = []
    user_chat_messages[user_id].append(message_id)

def get_active_account(user_id: int) -> Optional[Tuple[str, TelegramClient]]:
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        return None
    selected_phone = user_selected_account.get(user_id)
    if selected_phone and selected_phone in accounts:
        return selected_phone, accounts[selected_phone]["client"]
    phone = list(accounts.keys())[0]
    return phone, accounts[phone]["client"]

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    text = (
        em('bot') + " <b>Добро пожаловать!</b>\n\n"
        "<b>Главное меню:</b>\n"
        + em('settings') + " <b>Менеджер аккаунтов</b> — управление аккаунтами\n"
        + em('stats') + " <b>Функции</b> — рассылка, вступление в чаты, масс реакции\n"
        + em('megaphone') + " <b>Поддержка</b> — связь с поддержкой\n\n"
        + em('wallet') + " <b>Купить аккаунт для рассылки:</b> " + BUY_ACCOUNTS
    )
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.message(Command("admin"))
async def admin_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await safe_send(message.chat.id, em('cross') + " Нет доступа")
        return
    text = (
        em('lock') + " <b>Админ панель</b>\n\n"
        + em('stats') + " <b>Статистика</b> — просмотр статистики\n"
        + em('megaphone') + " <b>Рассылка всем</b> — отправить сообщение всем"
    )
    await safe_send(message.chat.id, text, reply_markup=get_admin_keyboard())

@dp.message(F.text == "Менеджер аккаунтов")
async def accounts_manager(message: types.Message):
    user_id = message.from_user.id
    count = len(user_sessions.get(user_id, {}))
    selected = user_selected_account.get(user_id, "Не выбран")
    text = (
        em('settings') + " <b>Менеджер аккаунтов</b>\n"
        + em('profile') + " Активных аккаунтов: " + str(count) + " (безлимит)\n"
        + em('user_check') + " Выбран: <code>" + selected + "</code>"
    )
    await safe_send(message.chat.id, text, reply_markup=get_accounts_menu_keyboard())

@dp.message(F.text == "Функции")
async def functions_menu(message: types.Message):
    text = (
        em('stats') + " <b>Функции</b>\n"
        + em('megaphone') + " Рассылка сообщений\n"
        + em('people') + " Вступление в чаты\n"
        + em('smile') + " Масс реакции"
    )
    await safe_send(message.chat.id, text, reply_markup=get_functions_keyboard())

@dp.message(F.text == "Поддержка")
async def support(message: types.Message):
    text = (
        em('megaphone') + " <b>Поддержка</b>\n"
        + em('link') + " Свяжитесь с нами: " + SUPPORT_USERNAME
    )
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    text = em('bot') + " <b>Главное меню</b>"
    await safe_send(callback.message.chat.id, text, reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    text = (
        em('apps') + " <b>Добавление аккаунта</b>\n"
        + em('write') + " Введите номер телефона: <code>+79123456789</code>"
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("accounts_manager"))
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await safe_send(message.chat.id, em('cross') + " Неверный формат. Пример: <code>+79123456789</code>", reply_markup=get_back_keyboard("accounts_manager"))
        return

    user_id = message.from_user.id
    if user_id in user_sessions and phone in user_sessions[user_id]:
        await safe_send(message.chat.id, em('cross') + " Этот аккаунт уже добавлен!", reply_markup=get_accounts_menu_keyboard())
        await state.clear()
        return

    client = TelegramClient('sessions/' + str(user_id) + '_' + phone.replace("+", ""), API_ID, API_HASH)
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        pending_logins[user_id] = {"client": client, "phone": phone, "phone_code_hash": sent_code.phone_code_hash}
        text = (
            em('gift') + " Код отправлен на <code>" + phone + "</code>\n"
            + em('write') + " Введите код из SMS:"
        )
        await safe_send(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as ex:
        await client.disconnect()
        await safe_send(message.chat.id, em('cross') + " Ошибка: " + str(ex), reply_markup=get_accounts_menu_keyboard())
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    if user_id not in pending_logins:
        await safe_send(message.chat.id, em('cross') + " Сессия истекла", reply_markup=get_accounts_menu_keyboard())
        await state.clear()
        return

    data = pending_logins[user_id]
    client = data["client"]
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        await on_successful_login(user_id, phone, client, message, state)
    except errors.SessionPasswordNeededError:
        text = em('lock') + " Требуется 2FA\n" + em('write') + " Введите пароль:"
        await safe_send(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as ex:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send(message.chat.id, em('cross') + " Ошибка: " + str(ex), reply_markup=get_accounts_menu_keyboard())
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    if user_id not in pending_logins:
        await safe_send(message.chat.id, em('cross') + " Сессия истекла", reply_markup=get_accounts_menu_keyboard())
        await state.clear()
        return

    data = pending_logins[user_id]
    client = data["client"]
    phone = data["phone"]

    try:
        await client.sign_in(password=password)
        await on_successful_login(user_id, phone, client, message, state)
    except Exception as ex:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send(message.chat.id, em('cross') + " Ошибка 2FA: " + str(ex), reply_markup=get_accounts_menu_keyboard())
        await state.clear()

async def on_successful_login(user_id, phone, client, message, state):
    if user_id not in user_sessions:
        user_sessions[user_id] = {}
    user_sessions[user_id][phone] = {"client": client, "phone": phone}
    pending_logins.pop(user_id, None)
    dialogs_cache.pop(user_id, None)
    if user_id not in user_selected_account:
        user_selected_account[user_id] = phone
    await safe_send(message.chat.id, em('check') + " Аккаунт <code>" + phone + "</code> добавлен!", reply_markup=get_accounts_menu_keyboard())
    await state.clear()

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        text = em('unlock') + " <b>Мои аккаунты</b>\n\n" + em('cross') + " Нет добавленных аккаунтов"
        await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for phone in accounts:
        builder.button(text=phone, callback_data="acc_" + phone, style="default")
    builder.button(text="Назад", callback_data="accounts_manager", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    text = (
        em('profile') + " <b>Мои аккаунты</b>\n"
        + em('unlock') + " Всего: " + str(len(accounts)) + "\n"
        "Выберите аккаунт:"
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "select_account")
async def select_account_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        await callback.answer("Нет добавленных аккаунтов!", show_alert=True)
        return

    builder = InlineKeyboardBuilder()
    for phone in accounts:
        is_selected = user_selected_account.get(user_id) == phone
        prefix = "✅ " if is_selected else ""
        builder.button(
            text=prefix + phone,
            callback_data="sel_" + phone,
            style="success" if is_selected else "default"
        )
    builder.button(text="Назад", callback_data="accounts_manager", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    selected = user_selected_account.get(user_id, "Не выбран")
    text = (
        em('user_check') + " <b>Выбор аккаунта</b>\n"
        "Текущий: <code>" + selected + "</code>\n\n"
        "Выберите аккаунт для работы:"
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("sel_"))
async def select_account(callback: types.CallbackQuery):
    phone = callback.data.replace("sel_", "")
    user_id = callback.from_user.id
    if user_id in user_sessions and phone in user_sessions[user_id]:
        user_selected_account[user_id] = phone
        dialogs_cache.pop(user_id, None)
        await callback.answer("Выбран аккаунт " + phone, show_alert=True)
        await select_account_menu(callback)
    else:
        await callback.answer("Аккаунт не найден", show_alert=True)

@dp.callback_query(F.data.startswith("acc_"))
async def account_info(callback: types.CallbackQuery):
    phone = callback.data.replace("acc_", "")
    user_id = callback.from_user.id
    is_selected = user_selected_account.get(user_id) == phone
    builder = InlineKeyboardBuilder()
    if not is_selected:
        builder.button(text="Выбрать", callback_data="sel_" + phone, style="success", icon_custom_emoji_id=E["user_check"])
    builder.button(text="Удалить", callback_data="del_" + phone, style="danger", icon_custom_emoji_id=E["trash"])
    builder.button(text="Назад", callback_data="my_accounts", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    status = "✅ Выбран" if is_selected else "⚪ Доступен"
    text = (
        em('profile') + " <b>Аккаунт:</b> <code>" + phone + "</code>\n"
        + em('check') + " Статус: " + status
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("del_"))
async def delete_account(callback: types.CallbackQuery):
    phone = callback.data.replace("del_", "")
    user_id = callback.from_user.id
    if user_id in user_sessions and phone in user_sessions[user_id]:
        try:
            await user_sessions[user_id][phone]["client"].disconnect()
        except:
            pass
        del user_sessions[user_id][phone]
        dialogs_cache.pop(user_id, None)
        if user_selected_account.get(user_id) == phone:
            user_selected_account.pop(user_id, None)
            if user_sessions.get(user_id):
                user_selected_account[user_id] = list(user_sessions[user_id].keys())[0]
    await safe_edit(callback.message, em('check') + " Аккаунт <code>" + phone + "</code> удален", reply_markup=get_accounts_menu_keyboard())
    await callback.answer("Аккаунт удален!", show_alert=True)

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    count = len(user_sessions.get(user_id, {}))
    selected = user_selected_account.get(user_id, "Не выбран")
    text = (
        em('settings') + " <b>Менеджер аккаунтов</b>\n"
        + em('profile') + " Активных аккаунтов: " + str(count) + " (безлимит)\n"
        + em('user_check') + " Выбран: <code>" + selected + "</code>"
    )
    await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
    await callback.answer()

async def load_dialogs(user_id: int) -> bool:
    account = get_active_account(user_id)
    if not account:
        return False
    phone, client = account
    try:
        dialogs = await client.get_dialogs(limit=200)
        chats = []
        for dialog in dialogs:
            entity = dialog.entity
            if isinstance(entity, User):
                name = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
                if not name:
                    name = f"User {entity.id}"
                chat_id = f"user_{entity.id}"
            elif isinstance(entity, (Chat, Channel)):
                name = entity.title or f"Chat {entity.id}"
                chat_id = f"chat_{entity.id}"
            else:
                continue
            chats.append((chat_id, name))
        if user_id not in dialogs_cache:
            dialogs_cache[user_id] = {}
        dialogs_cache[user_id][phone] = chats
        return True
    except Exception as ex:
        logging.error(f"Error loading dialogs: {ex}")
        if user_id not in dialogs_cache:
            dialogs_cache[user_id] = {}
        dialogs_cache[user_id][phone] = []
        return False

# ============ MASS REACTIONS ============

@dp.callback_query(F.data == "mass_reactions")
async def mass_reactions_menu(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(text="Запустить реакции", callback_data="start_reactions", style="primary", icon_custom_emoji_id=E["smile"])
    builder.button(text="Активные реакции", callback_data="active_reactions_list", style="default", icon_custom_emoji_id=E["clock"])
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)
    await safe_edit(callback.message, em('smile') + " <b>Масс реакции</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "start_reactions")
async def start_reactions(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    account = get_active_account(user_id)
    if not account:
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return

    await state.update_data(selected_chats=[], current_page=0)
    
    status_msg = await safe_send(callback.message.chat.id, em('loading') + " <b>Загружаем чаты...</b>")
    success = await load_dialogs(user_id)
    try:
        await status_msg.delete()
    except:
        pass

    phone = user_selected_account.get(user_id) or (list(user_sessions[user_id].keys())[0] if user_sessions.get(user_id) else None)
    if not success or not dialogs_cache.get(user_id, {}).get(phone):
        await safe_send(callback.message.chat.id, em('cross') + " Не удалось загрузить чаты.", reply_markup=get_functions_keyboard())
        await state.clear()
        return

    await delete_user_chat_messages(user_id)
    await create_reaction_chat_selection(callback.message.chat.id, state, 0)
    await callback.answer()

async def create_reaction_chat_selection(chat_id: int, state: FSMContext, page: int):
    user_id = chat_id
    phone = user_selected_account.get(user_id) or (list(user_sessions[user_id].keys())[0] if user_sessions.get(user_id) else None)
    if not phone:
        return

    chats = dialogs_cache.get(user_id, {}).get(phone, [])
    per_page = 10
    total_pages = max(1, (len(chats) + per_page - 1) // per_page)
    start = page * per_page
    end = start + per_page
    page_chats = chats[start:end]

    data = await state.get_data()
    selected: List[str] = list(data.get("selected_chats", []))

    builder = InlineKeyboardBuilder()

    for chat_id_str, chat_name in page_chats:
        is_selected = chat_id_str in selected
        prefix = "✅ " if is_selected else ""
        display = chat_name[:35]
        builder.button(
            text=prefix + display,
            callback_data="rsc_" + chat_id_str + "_" + str(page),
            style="success" if is_selected else "default"
        )

    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton(text="◀ Назад", callback_data="rpg_" + str(page - 1)))
    if page < total_pages - 1:
        nav.append(types.InlineKeyboardButton(text="Вперед ▶", callback_data="rpg_" + str(page + 1)))
    if nav:
        builder.row(*nav)

    action_row = []
    if len(selected) >= 1:
        action_row.append(types.InlineKeyboardButton(
            text="🚀 Далее (" + str(len(selected)) + ")",
            callback_data="reaction_next"
        ))
    if len(selected) > 0:
        action_row.append(types.InlineKeyboardButton(
            text="🗑 Сбросить",
            callback_data="rclear_chats"
        ))
    if action_row:
        builder.row(*action_row)

    builder.button(text="Отмена", callback_data="cancel_reactions", style="default", icon_custom_emoji_id=E["cross"])
    builder.adjust(1)

    await state.update_data(current_page=page)

    limit_text = "до 5" if len(selected) < 5 else "ЛИМИТ"
    text = (
        em('people') + " <b>Выберите чаты для реакций (" + limit_text + "):</b>\n"
        "Выбрано: " + str(len(selected)) + "/5 | Страница " + str(page + 1) + "/" + str(total_pages)
    )

    msg = await safe_send(chat_id, text, reply_markup=builder.as_markup())
    await add_user_chat_message(chat_id, msg.message_id)

@dp.callback_query(F.data.startswith("rsc_"))
async def reaction_select_chat(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.replace("rsc_", "").split("_")
    
    if len(parts) >= 3:
        chat_id_str = parts[0] + "_" + parts[1]
        page = int(parts[2])
    elif len(parts) == 2:
        chat_id_str = parts[0]
        page = int(parts[1])
    else:
        chat_id_str = parts[0]
        page = 0

    data = await state.get_data()
    selected: List[str] = list(data.get("selected_chats", []))

    if chat_id_str in selected:
        selected.remove(chat_id_str)
        await callback.answer("Чат убран (" + str(len(selected)) + "/5)")
    else:
        if len(selected) >= 5:
            await callback.answer("Максимум 5 чатов!", show_alert=True)
            return
        selected.append(chat_id_str)
        await callback.answer("Чат добавлен (" + str(len(selected)) + "/5)")

    await state.update_data(selected_chats=selected)
    await state.set_state(ReactionStates.selecting_chats)

    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    try:
        await callback.message.delete()
    except:
        pass

    await create_reaction_chat_selection(user_id, state, page)

@dp.callback_query(F.data.startswith("rpg_"))
async def reaction_chats_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.replace("rpg_", ""))
    await state.update_data(current_page=page)

    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    try:
        await callback.message.delete()
    except:
        pass

    await create_reaction_chat_selection(user_id, state, page)
    await callback.answer()

@dp.callback_query(F.data == "rclear_chats")
async def reaction_clear_chats(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(selected_chats=[])
    data = await state.get_data()
    page = data.get("current_page", 0)

    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    try:
        await callback.message.delete()
    except:
        pass

    await create_reaction_chat_selection(user_id, state, page)
    await callback.answer("Выбор сброшен!")

@dp.callback_query(F.data == "reaction_next")
async def reaction_next_step(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_chats = data.get("selected_chats", [])
    
    if not selected_chats:
        await callback.answer("Выберите хотя бы один чат!", show_alert=True)
        return

    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    
    text = em('write') + " <b>Введите задержку между реакциями (секунд):</b>"
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("cancel_reactions"))
    await state.set_state(ReactionStates.waiting_for_delay)
    await callback.answer()

@dp.message(ReactionStates.waiting_for_delay)
async def reaction_get_delay(message: types.Message, state: FSMContext):
    try:
        delay = float(message.text)
        if delay < 1:
            raise ValueError
    except ValueError:
        await safe_send(message.chat.id, em('cross') + " Введите положительное число (минимум 1 секунда)")
        return

    await state.update_data(delay=delay)
    
    text = (
        em('send') + " <b>Выберите тип работы:</b>\n\n"
        + em('loading') + " <b>Всё подряд</b> — ставит реакцию на все новые сообщения\n"
        + em('loading') + " <b>Через один</b> — ставит реакцию, пропуская одно сообщение"
    )
    await safe_send(message.chat.id, text, reply_markup=get_reaction_type_keyboard())
    await state.set_state(ReactionStates.selecting_type)

@dp.callback_query(F.data.startswith("rtype_"), ReactionStates.selecting_type)
async def reaction_set_type(callback: types.CallbackQuery, state: FSMContext):
    rtype = callback.data.replace("rtype_", "")
    await state.update_data(reaction_type=rtype)
    
    text = em('smile') + " <b>Выберите реакцию:</b>"
    await safe_edit(callback.message, text, reply_markup=get_reactions_keyboard())
    await state.set_state(ReactionStates.selecting_reaction)
    await callback.answer()

@dp.callback_query(F.data.startswith("react_"), ReactionStates.selecting_reaction)
async def reaction_start(callback: types.CallbackQuery, state: FSMContext):
    reaction = callback.data.replace("react_", "")
    data = await state.get_data()
    selected_chats = data.get("selected_chats", [])
    delay = data.get("delay", 5)
    rtype = data.get("reaction_type", "all")

    if not selected_chats:
        await callback.answer("Нет выбранных чатов!", show_alert=True)
        return

    user_id = callback.from_user.id
    account = get_active_account(user_id)
    if not account:
        await callback.answer("Нет активного аккаунта!", show_alert=True)
        return

    phone, client = account

    reaction_id = "reaction_" + str(int(datetime.now().timestamp()))
    
    if user_id not in active_reactions:
        active_reactions[user_id] = {}

    # Запускаем реакции
    task = asyncio.create_task(
        run_reactions(
            user_id=user_id,
            reaction_id=reaction_id,
            chats=selected_chats,
            delay=delay,
            rtype=rtype,
            reaction=reaction,
            client=client
        )
    )

    active_reactions[user_id][reaction_id] = {
        "task": task,
        "chats": selected_chats,
        "reaction": reaction,
        "type": rtype,
        "delay": delay
    }

    text = (
        em('smile') + " <b>Масс реакции запущены!</b>\n"
        + em('people') + " Чатов: " + str(len(selected_chats)) + "\n"
        + em('clock') + " Задержка: " + str(delay) + "с\n"
        + em('send') + " Реакция: " + reaction + "\n"
        + em('loading') + " Тип: " + ("Всё подряд" if rtype == "all" else "Через один")
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="Остановить", callback_data="rstop_" + reaction_id, style="danger", icon_custom_emoji_id=E["cross"])
    builder.button(text="В меню", callback_data="main_menu", style="default", icon_custom_emoji_id=E["home"])
    builder.adjust(1)

    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await state.clear()
    await callback.answer("Реакции запущены!", show_alert=True)

async def run_reactions(user_id, reaction_id, chats, delay, rtype, reaction, client):
    """Запускает масс реакции на новые сообщения"""
    message_counters = {}
    
    @client.on(events.NewMessage(chats=[int(c.split("_")[1]) for c in chats]))
    async def reaction_handler(event):
        if reaction_id not in active_reactions.get(user_id, {}):
            return
        
        chat_id = event.chat_id
        if chat_id not in message_counters:
            message_counters[chat_id] = 0
        
        message_counters[chat_id] += 1
        
        if rtype == "skip":
            if message_counters[chat_id] % 2 == 0:
                return
        
        try:
            await asyncio.sleep(delay)
            await client(SendReactionRequest(
                peer=event.chat_id,
                msg_id=event.message.id,
                reaction=[ReactionEmoji(emoticon=reaction)]
            ))
        except Exception as ex:
            logging.error(f"Reaction error: {ex}")

    # Держим задачу активной
    while reaction_id in active_reactions.get(user_id, {}):
        await asyncio.sleep(1)
    
    client.remove_event_handler(reaction_handler)
    logging.info("Reactions stopped for " + reaction_id)

@dp.callback_query(F.data == "active_reactions_list")
async def active_reactions_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    reactions = active_reactions.get(user_id, {})

    if not reactions:
        builder = InlineKeyboardBuilder()
        builder.button(text="Назад", callback_data="mass_reactions", style="default", icon_custom_emoji_id=E["back"])
        text = em('unlock') + " <b>Нет активных реакций</b>"
        await safe_edit(callback.message, text, reply_markup=builder.as_markup())
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for rid, rdata in reactions.items():
        builder.button(
            text="Остановить " + rdata["reaction"] + " (" + str(len(rdata["chats"])) + " чатов)",
            callback_data="rstop_" + rid,
            style="danger",
            icon_custom_emoji_id=E["cross"]
        )
    builder.button(text="Назад", callback_data="mass_reactions", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    text = em('smile') + " <b>Активные реакции:</b> " + str(len(reactions))
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("rstop_"))
async def stop_reactions(callback: types.CallbackQuery):
    reaction_id = callback.data.replace("rstop_", "")
    user_id = callback.from_user.id

    if user_id in active_reactions and reaction_id in active_reactions[user_id]:
        active_reactions[user_id][reaction_id]["task"].cancel()
        del active_reactions[user_id][reaction_id]
        await callback.answer("Реакции остановлены!", show_alert=True)
    else:
        await callback.answer("Реакции не найдены", show_alert=True)

    await active_reactions_list(callback)

@dp.callback_query(F.data == "cancel_reactions")
async def cancel_reactions(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    await state.clear()
    try:
        await callback.message.delete()
    except:
        pass
    await safe_send(callback.message.chat.id, em('cross') + " <b>Реакции отменены</b>", reply_markup=get_functions_keyboard())
    await callback.answer()

# ============ BROADCAST ============

@dp.callback_query(F.data == "broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(text="Новая рассылка", callback_data="new_broadcast", style="primary", icon_custom_emoji_id=E["megaphone"])
    builder.button(text="Активные рассылки", callback_data="active_broadcasts", style="default", icon_custom_emoji_id=E["clock"])
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)
    await safe_edit(callback.message, em('megaphone') + " <b>Рассылка</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "new_broadcast")
async def new_broadcast(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    account = get_active_account(user_id)
    if not account:
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return

    await state.update_data(messages_list=[])
    
    text = (
        em('write') + " <b>Отправьте 1 сообщение для рассылки:</b>\n\n"
        + em('info') + " Поддерживается HTML:\n"
        "• <b>жирный</b> • <i>курсив</i> • <code>код</code>\n"
        "• <u>подчеркнутый</u> • <s>зачеркнутый</s>\n"
        "• <blockquote>цитата</blockquote>\n"
        "• Премиум эмодзи\n\n"
        + em('file') + " Можно добавить до 5 сообщений"
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("broadcast"))
    await state.set_state(BroadcastStates.adding_messages)
    await callback.answer()

@dp.message(BroadcastStates.adding_messages)
async def broadcast_add_message(message: types.Message, state: FSMContext):
    msg_html = message.html_text if message.html_text else message.text or ""
    data = await state.get_data()
    messages_list = list(data.get("messages_list", []))
    
    messages_list.append(msg_html)
    await state.update_data(messages_list=messages_list)
    
    count = len(messages_list)
    
    if count >= 5:
        await safe_send(
            message.chat.id,
            em('check') + " <b>Добавлено 5/5 сообщений!</b>\n\n" + em('write') + " Достигнут лимит. Настройте рассылку:",
            reply_markup=get_message_actions_keyboard(count)
        )
    else:
        text = (
            em('check') + " <b>Сообщение " + str(count) + "/5 добавлено!</b>\n\n"
            + em('write') + " Отправьте ещё сообщение или настройте рассылку:"
        )
        await safe_send(
            message.chat.id,
            text,
            reply_markup=get_message_actions_keyboard(count)
        )

@dp.callback_query(F.data == "add_msg", BroadcastStates.adding_messages)
async def add_more_messages(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    messages_list = data.get("messages_list", [])
    count = len(messages_list)
    
    if count >= 5:
        await callback.answer("Достигнут лимит в 5 сообщений!", show_alert=True)
        return
    
    text = (
        em('write') + " <b>Отправьте сообщение " + str(count + 1) + "/5:</b>\n\n"
        + em('info') + " Поддерживается HTML разметка"
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("broadcast"))
    await callback.answer()

@dp.callback_query(F.data == "start_msg_config", BroadcastStates.adding_messages)
async def start_message_config(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    messages_list = data.get("messages_list", [])
    
    if not messages_list:
        await callback.answer("Добавьте хотя бы одно сообщение!", show_alert=True)
        return
    
    text = (
        em('write') + " <b>Введите количество сообщений в каждый чат:</b>\n\n"
        + em('file') + " Сообщений для рассылки: " + str(len(messages_list))
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("broadcast"))
    await state.set_state(BroadcastStates.waiting_for_count)
    await callback.answer()

@dp.message(BroadcastStates.waiting_for_count)
async def broadcast_get_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if count < 1:
            raise ValueError
    except ValueError:
        await safe_send(message.chat.id, em('cross') + " Введите целое положительное число")
        return

    await state.update_data(message_count=count)
    await safe_send(
        message.chat.id,
        em('write') + " <b>Введите задержку (секунд):</b>",
        reply_markup=get_back_keyboard("broadcast")
    )
    await state.set_state(BroadcastStates.waiting_for_delay)

@dp.message(BroadcastStates.waiting_for_delay)
async def broadcast_get_delay(message: types.Message, state: FSMContext):
    try:
        delay = float(message.text)
        if delay < 0:
            raise ValueError
    except ValueError:
        await safe_send(message.chat.id, em('cross') + " Введите положительное число")
        return

    await state.update_data(delay=delay, selected_chats=[], current_page=0)

    user_id = message.from_user.id
    status_msg = await safe_send(message.chat.id, em('loading') + " <b>Загружаем чаты...</b>")
    success = await load_dialogs(user_id)

    try:
        await status_msg.delete()
    except:
        pass

    phone = user_selected_account.get(user_id) or (list(user_sessions[user_id].keys())[0] if user_sessions.get(user_id) else None)
    if not success or not dialogs_cache.get(user_id, {}).get(phone):
        await safe_send(message.chat.id, em('cross') + " Не удалось загрузить чаты.", reply_markup=get_functions_keyboard())
        await state.clear()
        return

    await delete_user_chat_messages(user_id)
    await create_chat_selection_message(message.chat.id, state, 0)

async def create_chat_selection_message(chat_id: int, state: FSMContext, page: int):
    user_id = chat_id
    phone = user_selected_account.get(user_id) or (list(user_sessions[user_id].keys())[0] if user_sessions.get(user_id) else None)
    if not phone:
        return

    chats = dialogs_cache.get(user_id, {}).get(phone, [])
    per_page = 10
    total_pages = max(1, (len(chats) + per_page - 1) // per_page)
    start = page * per_page
    end = start + per_page
    page_chats = chats[start:end]

    data = await state.get_data()
    selected: List[str] = list(data.get("selected_chats", []))

    builder = InlineKeyboardBuilder()

    for chat_id_str, chat_name in page_chats:
        is_selected = chat_id_str in selected
        prefix = "✅ " if is_selected else ""
        display = chat_name[:35]
        builder.button(
            text=prefix + display,
            callback_data="sc_" + chat_id_str + "_" + str(page),
            style="success" if is_selected else "default"
        )

    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton(text="◀ Назад", callback_data="pg_" + str(page - 1)))
    if page < total_pages - 1:
        nav.append(types.InlineKeyboardButton(text="Вперед ▶", callback_data="pg_" + str(page + 1)))
    if nav:
        builder.row(*nav)

    action_row = []
    if len(selected) >= 1:
        action_row.append(types.InlineKeyboardButton(
            text="🚀 Запустить (" + str(len(selected)) + ")",
            callback_data="choose_mode"
        ))
    if len(selected) > 0:
        action_row.append(types.InlineKeyboardButton(
            text="🗑 Сбросить",
            callback_data="clear_chats"
        ))
    if action_row:
        builder.row(*action_row)

    builder.button(text="Отмена", callback_data="cancel_broadcast", style="default", icon_custom_emoji_id=E["cross"])
    builder.adjust(1)

    await state.update_data(current_page=page)

    limit_text = "до 10" if len(selected) < 10 else "ЛИМИТ"
    text = (
        em('people') + " <b>Выберите чаты (" + limit_text + "):</b>\n"
        "Выбрано: " + str(len(selected)) + "/10 | Страница " + str(page + 1) + "/" + str(total_pages) + "\n\n"
        + em('info') + " После выбора чата появится новое сообщение\n"
        + em('send') + " Можно запустить от 1 чата"
    )

    msg = await safe_send(chat_id, text, reply_markup=builder.as_markup())
    await add_user_chat_message(chat_id, msg.message_id)

@dp.callback_query(F.data == "choose_mode")
async def choose_broadcast_mode(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_chats = data.get("selected_chats", [])
    if not selected_chats:
        await callback.answer("Выберите хотя бы один чат!", show_alert=True)
        return
    text = (
        em('send') + " <b>Выберите режим рассылки:</b>\n\n"
        + em('loading') + " <b>Одновременная</b> — сначала отправляет во все чаты, потом ждет задержку\n"
        + em('loading') + " <b>Рандомная</b> — отправляет в случайные чаты с задержкой"
    )
    await safe_edit(callback.message, text, reply_markup=get_mode_keyboard())
    await state.set_state(BroadcastStates.selecting_mode)
    await callback.answer()

@dp.callback_query(F.data.startswith("mode_"), BroadcastStates.selecting_mode)
async def set_broadcast_mode(callback: types.CallbackQuery, state: FSMContext):
    mode = callback.data.replace("mode_", "")
    await state.update_data(mode=mode)
    await start_broadcast_execution(callback, state)

async def start_broadcast_execution(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_chats = data.get("selected_chats", [])
    mode = data.get("mode", "sync")

    if not selected_chats:
        await callback.answer("Выберите хотя бы один чат!", show_alert=True)
        return

    user_id = callback.from_user.id
    broadcast_id = "bc_" + str(int(datetime.now().timestamp()))

    if user_id not in active_broadcasts:
        active_broadcasts[user_id] = {}

    account = get_active_account(user_id)
    if not account:
        await callback.answer("Нет активного аккаунта!", show_alert=True)
        return
    
    phone, client = account

    task = asyncio.create_task(
        execute_broadcast(
            user_id=user_id,
            broadcast_id=broadcast_id,
            chats=selected_chats,
            messages_list=data.get("messages_list", [""]),
            count=data.get("message_count", 1),
            delay=data.get("delay", 1),
            client=client,
            mode=mode
        )
    )
    active_broadcasts[user_id][broadcast_id] = task

    await delete_user_chat_messages(user_id)

    builder = InlineKeyboardBuilder()
    builder.button(text="Остановить", callback_data="stop_" + broadcast_id, style="danger", icon_custom_emoji_id=E["cross"])
    builder.button(text="В меню", callback_data="main_menu", style="default", icon_custom_emoji_id=E["home"])
    builder.adjust(1)

    mode_text = "Одновременная" if mode == "sync" else "Рандомная"
    text = (
        em('send') + " <b>Рассылка запущена!</b>\n"
        + em('people') + " Чатов: " + str(len(selected_chats)) + "\n"
        + em('file') + " Сообщений: " + str(len(data.get("messages_list", ['']))) + " (в каждый: " + str(data.get('message_count', 1)) + ")\n"
        + em('clock') + " Задержка: " + str(data.get('delay', 1)) + "с\n"
        + em('loading') + " Режим: " + mode_text
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await state.clear()
    await callback.answer("Рассылка запущена!", show_alert=True)

async def execute_broadcast(user_id, broadcast_id, chats, messages_list, count, delay, client, mode):
    completed = 0
    errors = 0
    total_messages = len(messages_list)

    if mode == "sync":
        for i in range(count):
            if broadcast_id not in active_broadcasts.get(user_id, {}):
                break
            msg = random.choice(messages_list) if total_messages > 1 else messages_list[0]
            for chat_id_str in chats:
                if broadcast_id not in active_broadcasts.get(user_id, {}):
                    break
                try:
                    entity_id = int(chat_id_str.split("_")[1])
                    entity = await client.get_entity(entity_id)
                    try:
                        await client.send_message(entity, msg, parse_mode='html')
                        completed += 1
                    except Exception as ex:
                        errors += 1
                        try:
                            clean = re.sub(r'<[^>]+>', '', msg)
                            await client.send_message(entity, clean)
                            completed += 1
                        except:
                            pass
                except Exception as ex:
                    errors += 1
            if i < count - 1 and broadcast_id in active_broadcasts.get(user_id, {}):
                await asyncio.sleep(delay)
    else:
        total_operations = count * len(chats)
        chat_indices = list(range(len(chats)))
        for i in range(total_operations):
            if broadcast_id not in active_broadcasts.get(user_id, {}):
                break
            chat_idx = random.choice(chat_indices)
            chat_id_str = chats[chat_idx]
            msg = random.choice(messages_list) if total_messages > 1 else messages_list[0]
            try:
                entity_id = int(chat_id_str.split("_")[1])
                entity = await client.get_entity(entity_id)
                try:
                    await client.send_message(entity, msg, parse_mode='html')
                    completed += 1
                except Exception as ex:
                    errors += 1
                    try:
                        clean = re.sub(r'<[^>]+>', '', msg)
                        await client.send_message(entity, clean)
                        completed += 1
                    except:
                        pass
            except Exception as ex:
                errors += 1
            if broadcast_id in active_broadcasts.get(user_id, {}):
                await asyncio.sleep(delay)

    logging.info("Broadcast " + broadcast_id + " done. Mode: " + mode + ", OK: " + str(completed) + ", ERR: " + str(errors))
    active_broadcasts.get(user_id, {}).pop(broadcast_id, None)

@dp.callback_query(F.data.startswith("sc_"))
async def select_chat(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.replace("sc_", "").split("_")
    
    if len(parts) >= 3:
        chat_id_str = parts[0] + "_" + parts[1]
        page = int(parts[2])
    elif len(parts) == 2:
        chat_id_str = parts[0]
        page = int(parts[1])
    else:
        chat_id_str = parts[0]
        page = 0

    data = await state.get_data()
    selected: List[str] = list(data.get("selected_chats", []))

    if chat_id_str in selected:
        selected.remove(chat_id_str)
        await callback.answer("Чат убран (" + str(len(selected)) + "/10)")
    else:
        if len(selected) >= 10:
            await callback.answer("Максимум 10 чатов!", show_alert=True)
            return
        selected.append(chat_id_str)
        await callback.answer("Чат добавлен (" + str(len(selected)) + "/10)")

    await state.update_data(selected_chats=selected, current_page=page)

    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    try:
        await callback.message.delete()
    except:
        pass

    await create_chat_selection_message(user_id, state, page)

@dp.callback_query(F.data.startswith("pg_"))
async def chats_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.replace("pg_", ""))
    await state.update_data(current_page=page)

    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    try:
        await callback.message.delete()
    except:
        pass

    await create_chat_selection_message(user_id, state, page)
    await callback.answer()

@dp.callback_query(F.data == "clear_chats")
async def clear_chats_selection(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(selected_chats=[])
    data = await state.get_data()
    page = data.get("current_page", 0)

    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    try:
        await callback.message.delete()
    except:
        pass

    await create_chat_selection_message(user_id, state, page)
    await callback.answer("Выбор сброшен!")

@dp.callback_query(F.data == "active_broadcasts")
async def active_broadcasts_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    broadcasts = active_broadcasts.get(user_id, {})

    if not broadcasts:
        builder = InlineKeyboardBuilder()
        builder.button(text="Назад", callback_data="broadcast", style="default", icon_custom_emoji_id=E["back"])
        text = em('unlock') + " <b>Нет активных рассылок</b>"
        await safe_edit(callback.message, text, reply_markup=builder.as_markup())
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for bid in broadcasts:
        builder.button(
            text="Остановить " + bid[:8] + "...",
            callback_data="stop_" + bid,
            style="danger",
            icon_custom_emoji_id=E["cross"]
        )
    builder.button(text="Назад", callback_data="broadcast", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    text = em('clock') + " <b>Активные рассылки:</b> " + str(len(broadcasts))
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("stop_"))
async def stop_broadcast(callback: types.CallbackQuery):
    broadcast_id = callback.data.replace("stop_", "")
    user_id = callback.from_user.id

    if user_id in active_broadcasts and broadcast_id in active_broadcasts[user_id]:
        active_broadcasts[user_id][broadcast_id].cancel()
        del active_broadcasts[user_id][broadcast_id]
        await callback.answer("Рассылка остановлена!", show_alert=True)
    else:
        await callback.answer("Рассылка не найдена", show_alert=True)

    await active_broadcasts_list(callback)

@dp.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await delete_user_chat_messages(user_id)
    await state.clear()
    try:
        await callback.message.delete()
    except:
        pass
    await safe_send(callback.message.chat.id, em('cross') + " <b>Рассылка отменена</b>", reply_markup=get_functions_keyboard())
    await callback.answer()

# ============ JOIN CHATS ============

@dp.callback_query(F.data == "join_chats")
async def join_chats_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    account = get_active_account(user_id)
    if not account:
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return

    text = (
        em('people') + " <b>Вступление в чаты</b>\n\n"
        + em('write') + " Отправьте список юзернеймов (каждый с новой строки):\n"
        "<code>@chat1\n@chat2\n@chat3</code>\n\n"
        + em('info') + " Задержка между вступлениями: 15 секунд"
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("functions"))
    await state.set_state(JoinStates.waiting_for_usernames)
    await callback.answer()

@dp.message(JoinStates.waiting_for_usernames)
async def process_join_usernames(message: types.Message, state: FSMContext):
    text = message.text.strip()
    lines = []
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
        line = line.replace('https://t.me/', '')
        line = line.replace('@', '')
        line = line.strip()
        if line:
            lines.append(line)

    if not lines:
        await safe_send(message.chat.id, em('cross') + " Не найдено ни одного юзернейма", reply_markup=get_back_keyboard("functions"))
        return

    if len(lines) > 50:
        await safe_send(message.chat.id, em('cross') + " Максимум 50 чатов за раз", reply_markup=get_back_keyboard("functions"))
        return

    user_id = message.from_user.id
    account = get_active_account(user_id)
    if not account:
        await safe_send(message.chat.id, em('cross') + " Нет активных аккаунтов", reply_markup=get_functions_keyboard())
        await state.clear()
        return

    phone, client = account

    await state.clear()

    status_msg = await safe_send(
        message.chat.id,
        em('loading') + " <b>Начинаю вступление...</b>\n" + em('people') + " Всего: " + str(len(lines)) + "\n" + em('clock') + " Задержка: 15с"
    )

    joined = 0
    failed = 0
    failed_list = []

    for i, username in enumerate(lines, 1):
        try:
            try:
                await client(JoinChannelRequest(username))
                joined += 1
            except FloodWaitError as e:
                wait_time = e.seconds
                await safe_edit(status_msg, em('loading') + " <b>FloodWait " + str(wait_time) + "с...</b>\nПрогресс: " + str(i) + "/" + str(len(lines)))
                await asyncio.sleep(wait_time)
                try:
                    await client(JoinChannelRequest(username))
                    joined += 1
                except:
                    failed += 1
                    failed_list.append(username)
            except:
                try:
                    entity = await client.get_entity(username)
                    await client(JoinChannelRequest(entity))
                    joined += 1
                except:
                    failed += 1
                    failed_list.append(username)
        except:
            failed += 1
            failed_list.append(username)

        if i % 3 == 0 or i == len(lines):
            progress_text = (
                em('loading') + " <b>Вступление в чаты...</b>\n"
                "Прогресс: " + str(i) + "/" + str(len(lines)) + "\n"
                + em('check') + " Вступил: " + str(joined) + "\n"
                + em('cross') + " Ошибок: " + str(failed)
            )
            try:
                await safe_edit(status_msg, progress_text)
            except:
                pass

        if i < len(lines):
            await asyncio.sleep(15)

    result_text = (
        em('celebration') + " <b>Вступление завершено!</b>\n\n"
        + em('check') + " Успешно: " + str(joined) + "\n"
        + em('cross') + " Ошибок: " + str(failed)
    )

    if failed_list:
        result_text += "\n\n" + em('cross') + " <b>Не удалось:</b>\n"
        result_text += "\n".join("• " + u for u in failed_list[:10])
        if len(failed_list) > 10:
            result_text += "\n... и еще " + str(len(failed_list) - 10)

    await safe_edit(status_msg, result_text, reply_markup=get_functions_keyboard())

# ============ ADMIN PANEL ============

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        return

    total_users = len(user_sessions)
    total_accounts = sum(len(accs) for accs in user_sessions.values())
    active_broadcasts_count = sum(len(bcasts) for bcasts in active_broadcasts.values())

    text = (
        em('stats') + " <b>Статистика бота</b>\n\n"
        + em('people') + " Пользователей: " + str(total_users) + "\n"
        + em('profile') + " Аккаунтов: " + str(total_accounts) + "\n"
        + em('clock') + " Активных рассылок: " + str(active_broadcasts_count) + "\n"
        + em('megaphone') + " Поддержка: " + SUPPORT_USERNAME
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=E["back"])
    
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        return

    text = (
        em('megaphone') + " <b>Рассылка всем пользователям</b>\n\n"
        + em('write') + " Отправьте сообщение с поддержкой HTML:\n"
        "• <b>жирный</b> • <i>курсив</i> • <code>код</code>\n"
        "• <u>подчеркнутый</u> • <s>зачеркнутый</s>\n"
        "• <blockquote>цитата</blockquote>\n"
        "• Премиум эмодзи"
    )
    
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("main_menu"))
    await state.set_state(AdminStates.waiting_for_broadcast)
    await callback.answer()

@dp.message(AdminStates.waiting_for_broadcast)
async def admin_broadcast_get_message(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return

    msg_html = message.html_text if message.html_text else message.text or ""
    await state.update_data(admin_broadcast_message=msg_html)

    total_users = len(user_sessions)
    
    text = (
        em('send') + " <b>Подтвердите рассылку</b>\n\n"
        + em('people') + " Пользователей: " + str(total_users) + "\n"
        + em('file') + " Сообщение:\n"
        "<blockquote>" + msg_html[:200] + ("..." if len(msg_html) > 200 else "") + "</blockquote>\n\n"
        "Отправить всем?"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="Отправить", callback_data="confirm_admin_broadcast", style="primary", icon_custom_emoji_id=E["send"])
    builder.button(text="Отмена", callback_data="cancel_admin_broadcast", style="danger", icon_custom_emoji_id=E["cross"])
    builder.adjust(2)

    await safe_send(message.chat.id, text, reply_markup=builder.as_markup())
    await state.set_state(AdminStates.waiting_for_confirm)

@dp.callback_query(F.data == "confirm_admin_broadcast", AdminStates.waiting_for_confirm)
async def admin_broadcast_confirm(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await state.clear()
        return

    data = await state.get_data()
    message_html = data.get("admin_broadcast_message", "")

    if not message_html:
        await callback.answer("Сообщение не найдено", show_alert=True)
        await state.clear()
        return

    status_msg = await safe_send(
        callback.message.chat.id,
        em('loading') + " <b>Рассылка началась...</b>\n" + em('people') + " Пользователей: " + str(len(user_sessions))
    )

    sent = 0
    failed = 0

    for user_id in user_sessions:
        try:
            await safe_send(user_id, message_html)
            sent += 1
            await asyncio.sleep(0.5)
        except:
            failed += 1

    result_text = (
        em('check') + " <b>Рассылка завершена!</b>\n\n"
        + em('check') + " Отправлено: " + str(sent) + "\n"
        + em('cross') + " Ошибок: " + str(failed)
    )

    try:
        await status_msg.delete()
    except:
        pass

    await safe_edit(callback.message, result_text, reply_markup=get_admin_keyboard())
    await state.clear()
    await callback.answer("Рассылка завершена!", show_alert=True)

@dp.callback_query(F.data == "cancel_admin_broadcast", AdminStates.waiting_for_confirm)
async def admin_broadcast_cancel(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await state.clear()
        return

    await state.clear()
    await safe_edit(callback.message, em('cross') + " <b>Рассылка отменена</b>", reply_markup=get_admin_keyboard())
    await callback.answer()

# ============ MAIN ============

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
