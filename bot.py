import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Tuple

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from telethon import TelegramClient, errors
from telethon.tl.types import Channel, Chat, User

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
SUPPORT_USERNAME = "@VestSupport"
MAX_ACCOUNTS = 5

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

user_sessions: Dict[int, Dict[str, dict]] = {}
active_broadcasts: Dict[int, Dict[str, asyncio.Task]] = {}
pending_logins: Dict[int, Dict] = {}
dialogs_cache: Dict[int, List[Tuple[str, str]]] = {}

class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_count = State()
    waiting_for_delay = State()
    selecting_chats = State()

class JoinStates(StatesGroup):
    waiting_for_usernames = State()

# Premium emoji IDs
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
    "pencil": "5870676941614354370",
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
}

# ============ KEYBOARDS ============

def get_main_keyboard():
    """Reply keyboard with premium emoji"""
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(
                    text="Менеджер аккаунтов",
                    icon_custom_emoji_id=E["settings"]
                ),
                types.KeyboardButton(
                    text="Функции",
                    icon_custom_emoji_id=E["stats"]
                ),
            ],
            [
                types.KeyboardButton(
                    text="Поддержка",
                    icon_custom_emoji_id=E["megaphone"]
                ),
            ]
        ],
        resize_keyboard=True
    )

def get_accounts_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Добавить аккаунт",
        callback_data="add_account",
        style="primary",
        icon_custom_emoji_id=E["gift"]
    )
    builder.button(
        text="Мои аккаунты",
        callback_data="my_accounts",
        style="success",
        icon_custom_emoji_id=E["profile"]
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        style="default",
        icon_custom_emoji_id=E["back"]
    )
    builder.adjust(2, 1)
    return builder.as_markup()

def get_functions_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Рассылка",
        callback_data="broadcast",
        style="primary",
        icon_custom_emoji_id=E["megaphone"]
    )
    builder.button(
        text="Вступление в чаты",
        callback_data="join_chats",
        style="success",
        icon_custom_emoji_id=E["people"]
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        style="default",
        icon_custom_emoji_id=E["back"]
    )
    builder.adjust(1)
    return builder.as_markup()

def get_back_keyboard(callback_data: str = "main_menu"):
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Назад",
        callback_data=callback_data,
        style="default",
        icon_custom_emoji_id=E["back"]
    )
    return builder.as_markup()

# ============ UTILS ============

def em(key: str) -> str:
    """Premium emoji HTML tag"""
    return f'<tg-emoji emoji-id="{E[key]}"></tg-emoji>'

async def safe_send(chat_id: int, text: str, reply_markup=None, **kwargs):
    """Safe send with fallback"""
    try:
        return await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup, **kwargs)
    except TelegramBadRequest as exc:
        if "can't parse" in str(exc):
            clean = re.sub(r'<tg-emoji[^>]*></tg-emoji>', '▫', text)
            clean = re.sub(r'<[^>]+>', '', clean)
            return await bot.send_message(chat_id, clean, reply_markup=reply_markup, **kwargs)
        raise

async def safe_edit(message: types.Message, text: str, reply_markup=None):
    """Safe edit with fallback to new message"""
    try:
        return await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        exc_str = str(exc)
        if "message can't be edited" in exc_str or "message is not modified" in exc_str:
            try:
                await message.delete()
            except:
                pass
            return await safe_send(message.chat.id, text, reply_markup=reply_markup)
        if "can't parse" in exc_str:
            try:
                await message.delete()
            except:
                pass
            clean = re.sub(r'<tg-emoji[^>]*></tg-emoji>', '▫', text)
            clean = re.sub(r'<[^>]+>', '', clean)
            return await safe_send(message.chat.id, clean, reply_markup=reply_markup)
        raise
    except Exception:
        try:
            await message.delete()
        except:
            pass
        return await safe_send(message.chat.id, text, reply_markup=reply_markup)

# ============ START ============

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    text = f"""{em('bot')} <b>Добро пожаловать!</b>

<b>Главное меню:</b>
{em('settings')} <b>Менеджер аккаунтов</b> — управление аккаунтами
{em('stats')} <b>Функции</b> — рассылка, вступление в чаты
{em('megaphone')} <b>Поддержка</b> — связь с поддержкой"""
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

# ============ TEXT HANDLERS ============

@dp.message(F.text == "Менеджер аккаунтов")
async def accounts_manager(message: types.Message):
    user_id = message.from_user.id
    count = len(user_sessions.get(user_id, {}))
    text = f"""{em('settings')} <b>Менеджер аккаунтов</b>
{em('profile')} Активных аккаунтов: {count}/{MAX_ACCOUNTS}"""
    await safe_send(message.chat.id, text, reply_markup=get_accounts_menu_keyboard())

@dp.message(F.text == "Функции")
async def functions_menu(message: types.Message):
    text = f"""{em('stats')} <b>Функции</b>
{em('megaphone')} Рассылка сообщений
{em('people')} Вступление в чаты"""
    await safe_send(message.chat.id, text, reply_markup=get_functions_keyboard())

@dp.message(F.text == "Поддержка")
async def support(message: types.Message):
    text = f"""{em('megaphone')} <b>Поддержка</b>
{em('link')} Свяжитесь с нами: {SUPPORT_USERNAME}"""
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    text = f"{em('bot')} <b>Главное меню</b>"
    await safe_send(callback.message.chat.id, text, reply_markup=get_main_keyboard())
    await callback.answer()

# ============ ADD ACCOUNT ============

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if len(user_sessions.get(user_id, {})) >= MAX_ACCOUNTS:
        await callback.answer("Достигнут лимит аккаунтов!", show_alert=True)
        return

    text = f"""{em('apps')} <b>Добавление аккаунта</b>
{em('write')} Введите номер телефона: <code>+79123456789</code>"""
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("accounts_manager"))
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await safe_send(
            message.chat.id,
            f"{em('cross')} Неверный формат. Пример: <code>+79123456789</code>",
            reply_markup=get_back_keyboard("accounts_manager")
        )
        return

    user_id = message.from_user.id
    if user_id in user_sessions and phone in user_sessions[user_id]:
        await safe_send(
            message.chat.id,
            f"{em('cross')} Этот аккаунт уже добавлен!",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()
        return

    client = TelegramClient(f'sessions/{user_id}_{phone.replace("+", "")}', API_ID, API_HASH)
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        pending_logins[user_id] = {
            "client": client,
            "phone": phone,
            "phone_code_hash": sent_code.phone_code_hash
        }
        text = f"""{em('gift')} Код отправлен на <code>{phone}</code>
{em('write')} Введите код из SMS:"""
        await safe_send(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as ex:
        await client.disconnect()
        await safe_send(message.chat.id, f"{em('cross')} Ошибка: {str(ex)}", reply_markup=get_accounts_menu_keyboard())
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    if user_id not in pending_logins:
        await safe_send(message.chat.id, f"{em('cross')} Сессия истекла", reply_markup=get_accounts_menu_keyboard())
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
        text = f"""{em('lock')} Требуется 2FA
{em('write')} Введите пароль:"""
        await safe_send(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as ex:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send(message.chat.id, f"{em('cross')} Ошибка: {str(ex)}", reply_markup=get_accounts_menu_keyboard())
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    if user_id not in pending_logins:
        await safe_send(message.chat.id, f"{em('cross')} Сессия истекла", reply_markup=get_accounts_menu_keyboard())
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
        await safe_send(message.chat.id, f"{em('cross')} Ошибка 2FA: {str(ex)}", reply_markup=get_accounts_menu_keyboard())
        await state.clear()

async def on_successful_login(user_id, phone, client, message, state):
    if user_id not in user_sessions:
        user_sessions[user_id] = {}
    user_sessions[user_id][phone] = {"client": client, "phone": phone}
    pending_logins.pop(user_id, None)
    dialogs_cache.pop(user_id, None)
    await safe_send(message.chat.id, f"{em('check')} Аккаунт <code>{phone}</code> добавлен!", reply_markup=get_accounts_menu_keyboard())
    await state.clear()

# ============ MY ACCOUNTS ============

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        text = f"{em('unlock')} <b>Мои аккаунты</b>\n\n{em('cross')} Нет добавленных аккаунтов"
        await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for phone in accounts:
        builder.button(text=phone, callback_data=f"acc_{phone}", style="default")
    builder.button(text="Назад", callback_data="accounts_manager", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    text = f"""{em('profile')} <b>Мои аккаунты</b>
{em('unlock')} Всего: {len(accounts)}/{MAX_ACCOUNTS}
Выберите аккаунт:"""
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("acc_"))
async def account_info(callback: types.CallbackQuery):
    phone = callback.data.replace("acc_", "")
    builder = InlineKeyboardBuilder()
    builder.button(text="Удалить", callback_data=f"del_{phone}", style="danger", icon_custom_emoji_id=E["trash"])
    builder.button(text="Назад", callback_data="my_accounts", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    text = f"""{em('profile')} <b>Аккаунт:</b> <code>{phone}</code>
{em('check')} Статус: активен"""
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
    await safe_edit(callback.message, f"{em('check')} Аккаунт <code>{phone}</code> удален", reply_markup=get_accounts_menu_keyboard())
    await callback.answer("Аккаунт удален!", show_alert=True)

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    count = len(user_sessions.get(user_id, {}))
    text = f"""{em('settings')} <b>Менеджер аккаунтов</b>
{em('profile')} Активных аккаунтов: {count}/{MAX_ACCOUNTS}"""
    await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
    await callback.answer()

# ============ LOAD DIALOGS ============

async def load_dialogs(user_id: int) -> bool:
    """Load dialogs fresh each time"""
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        return False

    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]

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

        dialogs_cache[user_id] = chats
        logging.info(f"Loaded {len(chats)} dialogs for user {user_id}")
        return True
    except Exception as ex:
        logging.error(f"Error loading dialogs: {ex}")
        dialogs_cache[user_id] = []
        return False

# ============ BROADCAST ============

@dp.callback_query(F.data == "broadcast")
async def broadcast_menu(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(text="Новая рассылка", callback_data="new_broadcast", style="primary", icon_custom_emoji_id=E["megaphone"])
    builder.button(text="Активные рассылки", callback_data="active_broadcasts", style="default", icon_custom_emoji_id=E["clock"])
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)
    await safe_edit(callback.message, f"{em('megaphone')} <b>Рассылка</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "new_broadcast")
async def new_broadcast(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not user_sessions.get(user_id):
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return

    text = f"""{em('write')} <b>Отправьте сообщение для рассылки:</b>

{em('info')} Поддерживается HTML:
• <b>жирный</b> • <i>курсив</i> • <code>код</code>
• <u>подчеркнутый</u> • <s>зачеркнутый</s>
• <blockquote>цитата</blockquote>
• Премиум эмодзи"""
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("broadcast"))
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()

@dp.message(BroadcastStates.waiting_for_message)
async def broadcast_get_message(message: types.Message, state: FSMContext):
    msg_html = message.html_text if message.html_text else message.text or ""
    await state.update_data(message_html=msg_html)

    await safe_send(
        message.chat.id,
        f"{em('write')} <b>Введите количество сообщений в каждый чат:</b>",
        reply_markup=get_back_keyboard("broadcast")
    )
    await state.set_state(BroadcastStates.waiting_for_count)

@dp.message(BroadcastStates.waiting_for_count)
async def broadcast_get_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text)
        if count < 1:
            raise ValueError
    except ValueError:
        await safe_send(message.chat.id, f"{em('cross')} Введите целое положительное число")
        return

    await state.update_data(message_count=count)
    await safe_send(
        message.chat.id,
        f"{em('write')} <b>Введите задержку (секунд):</b>",
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
        await safe_send(message.chat.id, f"{em('cross')} Введите положительное число")
        return

    await state.update_data(delay=delay, selected_chats=[], current_page=0)

    user_id = message.from_user.id
    status_msg = await safe_send(message.chat.id, f"{em('loading')} <b>Загружаем чаты...</b>")

    success = await load_dialogs(user_id)

    try:
        await status_msg.delete()
    except:
        pass

    if not success or not dialogs_cache.get(user_id):
        await safe_send(
            message.chat.id,
            f"{em('cross')} Не удалось загрузить чаты.",
            reply_markup=get_functions_keyboard()
        )
        await state.clear()
        return

    await show_chat_selection(message, state, 0)

async def show_chat_selection(message: types.Message, state: FSMContext, page: int):
    user_id = message.from_user.id
    chats = dialogs_cache.get(user_id, [])
    per_page = 10
    total_pages = max(1, (len(chats) + per_page - 1) // per_page)
    start = page * per_page
    end = start + per_page
    page_chats = chats[start:end]

    data = await state.get_data()
    selected = data.get("selected_chats", [])

    builder = InlineKeyboardBuilder()
    for chat_id, chat_name in page_chats:
        is_selected = chat_id in selected
        prefix = "✅ " if is_selected else ""
        display = chat_name[:35]
        builder.button(
            text=f"{prefix}{display}",
            callback_data=f"sc_{chat_id}",
            style="success" if is_selected else "default"
        )

    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton(text="◀ Назад", callback_data=f"pg_{page - 1}"))
    if page < total_pages - 1:
        nav.append(types.InlineKeyboardButton(text="Вперед ▶", callback_data=f"pg_{page + 1}"))
    if nav:
        builder.row(*nav)

    if 0 < len(selected) <= 5:
        builder.button(
            text=f"Запустить ({len(selected)})",
            callback_data="start_broadcast",
            style="primary",
            icon_custom_emoji_id=E["send"]
        )

    builder.button(text="Отмена", callback_data="cancel_broadcast", style="danger", icon_custom_emoji_id=E["cross"])
    builder.adjust(1)

    await state.update_data(current_page=page)

    text = f"""{em('people')} <b>Выберите чаты (до 5):</b>
Выбрано: {len(selected)}/5 | Страница {page + 1}/{total_pages}"""
    await safe_edit(message, text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("sc_"))
async def select_chat(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("sc_", "")
    data = await state.get_data()
    selected = data.get("selected_chats", [])

    if chat_id in selected:
        selected.remove(chat_id)
    else:
        if len(selected) >= 5:
            await callback.answer("Максимум 5 чатов!", show_alert=True)
            return
        selected.append(chat_id)

    await state.update_data(selected_chats=selected)
    await show_chat_selection(callback.message, state, data.get("current_page", 0))
    await callback.answer()

@dp.callback_query(F.data.startswith("pg_"))
async def chats_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.replace("pg_", ""))
    await state.update_data(current_page=page)
    await show_chat_selection(callback.message, state, page)
    await callback.answer()

@dp.callback_query(F.data == "start_broadcast")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_chats = data.get("selected_chats", [])

    if not selected_chats:
        await callback.answer("Выберите хотя бы один чат!", show_alert=True)
        return

    user_id = callback.from_user.id
    broadcast_id = f"bc_{int(datetime.now().timestamp())}"

    if user_id not in active_broadcasts:
        active_broadcasts[user_id] = {}

    accounts = user_sessions.get(user_id, {})
    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]

    task = asyncio.create_task(
        execute_broadcast(
            user_id=user_id,
            broadcast_id=broadcast_id,
            chats=selected_chats,
            message_html=data.get("message_html", ""),
            count=data.get("message_count", 1),
            delay=data.get("delay", 1),
            client=client
        )
    )
    active_broadcasts[user_id][broadcast_id] = task

    builder = InlineKeyboardBuilder()
    builder.button(text="Остановить", callback_data=f"stop_{broadcast_id}", style="danger", icon_custom_emoji_id=E["cross"])
    builder.button(text="В меню", callback_data="main_menu", style="default", icon_custom_emoji_id=E["home"])
    builder.adjust(1)

    text = f"""{em('send')} <b>Рассылка запущена!</b>
{em('people')} Чатов: {len(selected_chats)}
{em('file')} Сообщений в каждый: {data.get('message_count', 1)}
{em('clock')} Задержка: {data.get('delay', 1)}с"""
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await state.clear()
    await callback.answer(f"{em('celebration')} Рассылка запущена!", show_alert=True)

async def execute_broadcast(user_id, broadcast_id, chats, message_html, count, delay, client):
    completed = 0
    errors = 0

    for chat_id_str in chats:
        if broadcast_id not in active_broadcasts.get(user_id, {}):
            break

        try:
            entity_id = int(chat_id_str.split("_")[1])
            entity = await client.get_entity(entity_id)

            for i in range(count):
                if broadcast_id not in active_broadcasts.get(user_id, {}):
                    break

                try:
                    await client.send_message(entity, message_html, parse_mode='html')
                    completed += 1
                    await asyncio.sleep(delay)
                except Exception as ex:
                    errors += 1
                    logging.error(f"Send error: {ex}")
                    try:
                        clean = re.sub(r'<[^>]+>', '', message_html)
                        await client.send_message(entity, clean)
                        completed += 1
                    except:
                        pass
                    await asyncio.sleep(1)
        except Exception as ex:
            errors += 1
            logging.error(f"Entity error for {chat_id_str}: {ex}")

    logging.info(f"Broadcast {broadcast_id} done. OK: {completed}, ERR: {errors}")
    active_broadcasts.get(user_id, {}).pop(broadcast_id, None)

@dp.callback_query(F.data == "active_broadcasts")
async def active_broadcasts_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    broadcasts = active_broadcasts.get(user_id, {})

    if not broadcasts:
        builder = InlineKeyboardBuilder()
        builder.button(text="Назад", callback_data="broadcast", style="default", icon_custom_emoji_id=E["back"])
        text = f"{em('unlock')} <b>Нет активных рассылок</b>"
        await safe_edit(callback.message, text, reply_markup=builder.as_markup())
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for bid in broadcasts:
        builder.button(
            text=f"Остановить {bid[:8]}...",
            callback_data=f"stop_{bid}",
            style="danger",
            icon_custom_emoji_id=E["cross"]
        )
    builder.button(text="Назад", callback_data="broadcast", style="default", icon_custom_emoji_id=E["back"])
    builder.adjust(1)

    text = f"{em('clock')} <b>Активные рассылки:</b> {len(broadcasts)}"
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
    await state.clear()
    await safe_edit(callback.message, f"{em('cross')} <b>Рассылка отменена</b>", reply_markup=get_functions_keyboard())
    await callback.answer()

# ============ JOIN CHATS ============

@dp.callback_query(F.data == "join_chats")
async def join_chats_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not user_sessions.get(user_id):
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return

    text = f"""{em('people')} <b>Вступление в чаты</b>

{em('write')} Отправьте список юзернеймов (каждый с новой строки):
<code>@chat1
@chat2
@chat3</code>

{em('info')} Задержка между вступлениями: 15 секунд"""
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("functions"))
    await state.set_state(JoinStates.waiting_for_usernames)
    await callback.answer()

@dp.message(JoinStates.waiting_for_usernames)
async def process_join_usernames(message: types.Message, state: FSMContext):
    text = message.text.strip()
    lines = [line.strip().replace('@', '').strip() for line in text.split('\n') if line.strip()]

    if not lines:
        await safe_send(message.chat.id, f"{em('cross')} Не найдено ни одного юзернейма", reply_markup=get_back_keyboard("functions"))
        return

    if len(lines) > 50:
        await safe_send(message.chat.id, f"{em('cross')} Максимум 50 чатов за раз", reply_markup=get_back_keyboard("functions"))
        return

    user_id = message.from_user.id
    accounts = user_sessions.get(user_id, {})
    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]

    await state.clear()

    status_msg = await safe_send(
        message.chat.id,
        f"{em('loading')} <b>Начинаю вступление...</b>\n{em('people')} Всего: {len(lines)}\n{em('clock')} Задержка: 15с"
    )

    joined = 0
    failed = 0
    failed_list = []

    for i, username in enumerate(lines, 1):
        try:
            await client(JoinChannelRequest(username))
            joined += 1
        except FloodWaitError as e:
            wait_time = e.seconds
            await safe_edit(status_msg, f"{em('loading')} <b>FloodWait {wait_time}с...</b>\nПрогресс: {i}/{len(lines)}")
            await asyncio.sleep(wait_time)
            try:
                await client(JoinChannelRequest(username))
                joined += 1
            except:
                failed += 1
                failed_list.append(username)
        except Exception as e:
            failed += 1
            failed_list.append(username)

        if i % 5 == 0 or i == len(lines):
            progress_text = f"""{em('loading')} <b>Вступление в чаты...</b>
Прогресс: {i}/{len(lines)}
{em('check')} Вступил: {joined}
{em('cross')} Ошибок: {failed}"""
            try:
                await safe_edit(status_msg, progress_text)
            except:
                pass

        if i < len(lines):
            await asyncio.sleep(15)

    result_text = f"""{em('celebration')} <b>Вступление завершено!</b>

{em('check')} Успешно: {joined}
{em('cross')} Ошибок: {failed}"""

    if failed_list:
        result_text += f"\n\n{em('cross')} <b>Не удалось:</b>\n"
        result_text += "\n".join(f"• {u}" for u in failed_list[:10])
        if len(failed_list) > 10:
            result_text += f"\n... и еще {len(failed_list) - 10}"

    await safe_edit(status_msg, result_text, reply_markup=get_functions_keyboard())

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
