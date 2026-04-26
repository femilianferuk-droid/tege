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

# ID премиум эмодзи
EMOJI = {
    "settings": "5870982283724328568",
    "profile": "5870994129244131212",
    "people": "5870772616305839506",
    "user_check": "5891207662678317861",
    "user_cross": "5893192487324880883",
    "file": "5870528606328852614",
    "smile": "5870764288364252592",
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
    "paperclip": "6039451237743595514",
    "link": "5769289093221454192",
    "info": "6028435952299413210",
    "bot": "6030400221232501136",
    "eye": "6037397706505195857",
    "eye_hidden": "6037243349675544634",
    "send": "5963103826075456248",
    "download": "6039802767931871481",
    "notification": "6039486778597970865",
    "gift": "6032644646587338669",
    "clock": "5983150113483134607",
    "celebration": "6041731551845159060",
    "font": "5870801517140775623",
    "write": "5870753782874246579",
    "media": "6035128606563241721",
    "geo": "6042011682497106307",
    "wallet": "5769126056262898415",
    "box": "5884479287171485878",
    "calendar": "5890937706803894250",
    "tag": "5886285355279193209",
    "time_passed": "5775896410780079073",
    "apps": "5778672437122045013",
    "brush": "6050679691004612757",
    "add_text": "5771851822897566479",
    "resolution": "5778479949572738874",
    "money": "5904462880941545555",
    "money_send": "5890848474563352982",
    "money_accept": "5879814368572478751",
    "code": "5940433880585605708",
    "loading": "5345906554510012647",
    "subscribe": "6039450962865688331",
    "check_sub": "5774022692642492953",
    "blue": "5373141891321699086",
    "red": "5370810157871667232",
    "green": "5471984997361523302",
    "broadcast_emoji": "5370599459661045441",
}

def e(id_key: str) -> str:
    """Создает премиум эмодзи тег"""
    return f'<tg-emoji emoji-id="{EMOJI[id_key]}"></tg-emoji>'

# ============ КЛАВИАТУРЫ ============

def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text=f"{e('settings')} Менеджер аккаунтов")
    builder.button(text=f"{e('stats')} Функции")
    builder.button(text=f"{e('megaphone')} Поддержка")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_accounts_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Добавить аккаунт",
        callback_data="add_account",
        icon_custom_emoji_id=EMOJI["gift"]
    )
    builder.button(
        text="Мои аккаунты",
        callback_data="my_accounts",
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
        icon_custom_emoji_id=EMOJI["megaphone"]
    )
    builder.button(
        text="Вступление в чаты",
        callback_data="join_chats",
        icon_custom_emoji_id=EMOJI["people"]
    )
    builder.button(
        text="Назад",
        callback_data="main_menu",
        icon_custom_emoji_id=EMOJI["back"]
    )
    builder.adjust(1)
    return builder.as_markup()

def get_back_keyboard(callback_data: str = "main_menu"):
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Назад",
        callback_data=callback_data,
        icon_custom_emoji_id=EMOJI["back"]
    )
    return builder.as_markup()

# ============ УТИЛИТЫ ============

def clean_entities(text):
    """Удаляет все HTML теги для fallback"""
    clean = re.sub(r'<tg-emoji[^>]*></tg-emoji>', '', text)
    clean = re.sub(r'<[^>]+>', '', clean)
    return clean

async def safe_send(chat_id: int, text: str, reply_markup=None, parse_mode=ParseMode.HTML, **kwargs):
    """Безопасная отправка с fallback при ошибке парсинга"""
    try:
        return await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup, **kwargs)
    except TelegramBadRequest as exc:
        if "can't parse" in str(exc):
            return await bot.send_message(chat_id, clean_entities(text), reply_markup=reply_markup, **kwargs)
        raise

async def safe_edit(message: types.Message, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    """Безопасное редактирование с fallback на новое сообщение"""
    try:
        return await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message can't be edited" in str(exc) or "message is not modified" in str(exc):
            try:
                await message.delete()
            except:
                pass
            return await safe_send(message.chat.id, text, reply_markup=reply_markup)
        if "can't parse" in str(exc):
            try:
                await message.delete()
            except:
                pass
            return await safe_send(message.chat.id, clean_entities(text), reply_markup=reply_markup)
        raise
    except Exception as exc:
        logging.error(f"safe_edit error: {exc}")
        try:
            await message.delete()
        except:
            pass
        return await safe_send(message.chat.id, text, reply_markup=reply_markup)

# ============ ОБРАБОТЧИКИ КОМАНД ============

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    text = (
        f"{e('bot')} <b>Добро пожаловать!</b>\n\n"
        f"<b>Главное меню:</b>\n"
        f"{e('settings')} <b>Менеджер аккаунтов</b> — управление аккаунтами\n"
        f"{e('stats')} <b>Функции</b> — рассылка, вступление в чаты\n"
        f"{e('megaphone')} <b>Поддержка</b> — связь с поддержкой"
    )
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.message(F.text.in_({f"{e('settings')} Менеджер аккаунтов", "Менеджер аккаунтов"}))
async def accounts_manager(message: types.Message):
    user_id = message.from_user.id
    count = len(user_sessions.get(user_id, {}))
    text = (
        f"{e('settings')} <b>Менеджер аккаунтов</b>\n"
        f"{e('profile')} Активных аккаунтов: {count}/{MAX_ACCOUNTS}"
    )
    await safe_send(message.chat.id, text, reply_markup=get_accounts_menu_keyboard())

@dp.message(F.text.in_({f"{e('stats')} Функции", "Функции"}))
async def functions_menu(message: types.Message):
    text = (
        f"{e('stats')} <b>Функции</b>\n"
        f"{e('megaphone')} Рассылка сообщений\n"
        f"{e('people')} Вступление в чаты"
    )
    await safe_send(message.chat.id, text, reply_markup=get_functions_keyboard())

@dp.message(F.text.in_({f"{e('megaphone')} Поддержка", "Поддержка"}))
async def support(message: types.Message):
    text = (
        f"{e('megaphone')} <b>Поддержка</b>\n"
        f"{e('link')} Свяжитесь с нами: {SUPPORT_USERNAME}"
    )
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await safe_send(
        callback.message.chat.id,
        f"{e('bot')} <b>Главное меню</b>",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# ============ ДОБАВЛЕНИЕ АККАУНТА ============

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if len(user_sessions.get(user_id, {})) >= MAX_ACCOUNTS:
        await callback.answer(f"Достигнут лимит аккаунтов!", show_alert=True)
        return
    
    text = (
        f"{e('apps')} <b>Добавление аккаунта</b>\n"
        f"{e('write')} Введите номер телефона в формате: <code>+79123456789</code>"
    )
    await safe_edit(callback.message, text, reply_markup=get_back_keyboard("accounts_manager"))
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await safe_send(
            message.chat.id,
            f"{e('cross')} Неверный формат. Пример: <code>+79123456789</code>",
            reply_markup=get_back_keyboard("accounts_manager")
        )
        return
    
    user_id = message.from_user.id
    
    if user_id in user_sessions and phone in user_sessions[user_id]:
        await safe_send(
            message.chat.id,
            f"{e('cross')} Этот аккаунт уже добавлен!",
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
        
        text = (
            f"{e('gift')} Код отправлен на <code>{phone}</code>\n"
            f"{e('write')} Введите код из SMS:"
        )
        await safe_send(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as ex:
        await client.disconnect()
        await safe_send(
            message.chat.id,
            f"{e('cross')} Ошибка: {str(ex)}",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await safe_send(
            message.chat.id,
            f"{e('cross')} Сессия истекла",
            reply_markup=get_accounts_menu_keyboard()
        )
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
        text = (
            f"{e('lock')} Требуется двухфакторная аутентификация\n"
            f"{e('write')} Введите пароль 2FA:"
        )
        await safe_send(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as ex:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send(
            message.chat.id,
            f"{e('cross')} Ошибка: {str(ex)}",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await safe_send(message.chat.id, f"{e('cross')} Сессия истекла", reply_markup=get_accounts_menu_keyboard())
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
        await safe_send(
            message.chat.id,
            f"{e('cross')} Ошибка 2FA: {str(ex)}",
            reply_markup=get_accounts_menu_keyboard()
        )
        await state.clear()

async def on_successful_login(user_id, phone, client, message, state):
    if user_id not in user_sessions:
        user_sessions[user_id] = {}
    user_sessions[user_id][phone] = {"client": client, "phone": phone}
    pending_logins.pop(user_id, None)
    dialogs_cache.pop(user_id, None)
    
    await safe_send(
        message.chat.id,
        f"{e('check')} Аккаунт <code>{phone}</code> успешно добавлен!",
        reply_markup=get_accounts_menu_keyboard()
    )
    await state.clear()

# ============ МОИ АККАУНТЫ ============

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    
    if not accounts:
        text = f"{e('unlock')} <b>Мои аккаунты</b>\n\n{e('cross')} Нет добавленных аккаунтов"
        await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for phone in accounts:
        builder.button(text=phone, callback_data=f"acc_{phone}", style="default")
    builder.button(text="Назад", callback_data="accounts_manager", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    text = (
        f"{e('profile')} <b>Мои аккаунты</b>\n"
        f"{e('unlock')} Всего: {len(accounts)}/{MAX_ACCOUNTS}\n"
        f"Выберите аккаунт:"
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("acc_"))
async def account_info(callback: types.CallbackQuery):
    phone = callback.data.replace("acc_", "")
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Удалить аккаунт",
        callback_data=f"del_{phone}",
        style="danger",
        icon_custom_emoji_id=EMOJI["trash"]
    )
    builder.button(text="Назад", callback_data="my_accounts", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    text = (
        f"{e('profile')} <b>Аккаунт:</b> <code>{phone}</code>\n"
        f"{e('check')} Статус: активен"
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
    
    await safe_edit(
        callback.message,
        f"{e('check')} Аккаунт <code>{phone}</code> удален",
        reply_markup=get_accounts_menu_keyboard()
    )
    await callback.answer("Аккаунт удален!", show_alert=True)

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    count = len(user_sessions.get(user_id, {}))
    text = (
        f"{e('settings')} <b>Менеджер аккаунтов</b>\n"
        f"{e('profile')} Активных аккаунтов: {count}/{MAX_ACCOUNTS}"
    )
    await safe_edit(callback.message, text, reply_markup=get_accounts_menu_keyboard())
    await callback.answer()

# ============ ЗАГРУЗКА ДИАЛОГОВ ============

async def load_dialogs(user_id: int) -> bool:
    """Загружает диалоги. Возвращает True если успешно"""
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
    builder.button(text="Назад", callback_data="main_menu", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    await safe_edit(
        callback.message,
        f"{e('megaphone')} <b>Рассылка</b>",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "new_broadcast")
async def new_broadcast(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not user_sessions.get(user_id):
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return
    
    await safe_edit(
        callback.message,
        f"{e('write')} <b>Отправьте сообщение для рассылки:</b>\n\n"
        f"{e('info')} Поддерживается HTML: <b>жирный</b>, <i>курсив</i>, <code>код</code>, <u>подчеркнутый</u>, <s>зачеркнутый</s>, <blockquote>цитата</blockquote>\n"
        f"{e('info')} Также можно использовать премиум эмодзи",
        reply_markup=get_back_keyboard("broadcast")
    )
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()

@dp.message(BroadcastStates.waiting_for_message)
async def broadcast_get_message(message: types.Message, state: FSMContext):
    # Сохраняем текст и сущности
    msg_text = message.html_text if message.html_text else message.text or ""
    await state.update_data(message_html=msg_text)
    
    await safe_send(
        message.chat.id,
        f"{e('write')} <b>Введите количество сообщений в каждый чат:</b>",
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
        await safe_send(message.chat.id, f"{e('cross')} Введите целое положительное число")
        return
    
    await state.update_data(message_count=count)
    await safe_send(
        message.chat.id,
        f"{e('write')} <b>Введите задержку между сообщениями (в секундах):</b>",
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
        await safe_send(message.chat.id, f"{e('cross')} Введите положительное число")
        return
    
    await state.update_data(delay=delay, selected_chats=[], current_page=0)
    
    user_id = message.from_user.id
    status_msg = await safe_send(message.chat.id, f"{e('loading')} <b>Загружаем чаты...</b>")
    
    # Загружаем диалоги при каждом запуске
    success = await load_dialogs(user_id)
    
    try:
        await status_msg.delete()
    except:
        pass
    
    if not success or not dialogs_cache.get(user_id):
        await safe_send(
            message.chat.id,
            f"{e('cross')} Не удалось загрузить чаты. Проверьте подключение аккаунта.",
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
        # Обрезаем имя для кнопки
        display = chat_name[:35]
        builder.button(
            text=f"{prefix}{display}",
            callback_data=f"sc_{chat_id}",
            style="success" if is_selected else "default"
        )
    
    # Навигация
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton(
            text="◀ Назад",
            callback_data=f"pg_{page - 1}"
        ))
    if page < total_pages - 1:
        nav.append(types.InlineKeyboardButton(
            text="Вперед ▶",
            callback_data=f"pg_{page + 1}"
        ))
    if nav:
        builder.row(*nav)
    
    # Кнопки действий
    if 0 < len(selected) <= 5:
        builder.button(
            text=f"Запустить ({len(selected)})",
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
    
    text = (
        f"{e('people')} <b>Выберите чаты (до 5):</b>\n"
        f"Выбрано: {len(selected)}/5 | Страница {page + 1}/{total_pages}"
    )
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
    builder.button(
        text="Остановить",
        callback_data=f"stop_{broadcast_id}",
        style="danger",
        icon_custom_emoji_id=EMOJI["cross"]
    )
    builder.button(text="В меню", callback_data="main_menu", icon_custom_emoji_id=EMOJI["home"])
    builder.adjust(1)
    
    text = (
        f"{e('send')} <b>Рассылка запущена!</b>\n"
        f"Чатов: {len(selected_chats)}\n"
        f"Сообщений в каждый: {data.get('message_count', 1)}\n"
        f"Задержка: {data.get('delay', 1)}с"
    )
    await safe_edit(callback.message, text, reply_markup=builder.as_markup())
    await state.clear()
    await callback.answer("Рассылка запущена!", show_alert=True)

async def execute_broadcast(user_id, broadcast_id, chats, message_html, count, delay, client):
    """Выполнение рассылки с поддержкой HTML"""
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
                    # Отправляем с HTML разметкой
                    await client.send_message(entity, message_html, parse_mode='html')
                    completed += 1
                    await asyncio.sleep(delay)
                except Exception as ex:
                    errors += 1
                    logging.error(f"Send error: {ex}")
                    # Пробуем без разметки
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
        builder.button(text="Назад", callback_data="broadcast", icon_custom_emoji_id=EMOJI["back"])
        text = f"{e('unlock')} <b>Нет активных рассылок</b>"
        await safe_edit(callback.message, text, reply_markup=builder.as_markup())
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for bid in broadcasts:
        builder.button(
            text=f"Остановить {bid[:8]}...",
            callback_data=f"stop_{bid}",
            style="danger",
            icon_custom_emoji_id=EMOJI["cross"]
        )
    builder.button(text="Назад", callback_data="broadcast", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    text = f"{e('clock')} <b>Активные рассылки:</b> {len(broadcasts)}"
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
    await safe_edit(
        callback.message,
        f"{e('cross')} <b>Рассылка отменена</b>",
        reply_markup=get_functions_keyboard()
    )
    await callback.answer()

# ============ ВСТУПЛЕНИЕ В ЧАТЫ ============

@dp.callback_query(F.data == "join_chats")
async def join_chats_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not user_sessions.get(user_id):
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return
    
    await safe_edit(
        callback.message,
        f"{e('people')} <b>Вступление в чаты</b>\n\n"
        f"{e('write')} Отправьте список юзернеймов чатов (каждый с новой строки):\n"
        f"<code>@chat1\n@chat2\n@chat3</code>\n\n"
        f"{e('info')} Бот вступит в каждый чат с задержкой 15 секунд",
        reply_markup=get_back_keyboard("functions")
    )
    await state.set_state(JoinStates.waiting_for_usernames)
    await callback.answer()

@dp.message(JoinStates.waiting_for_usernames)
async def process_join_usernames(message: types.Message, state: FSMContext):
    text = message.text.strip()
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    # Извлекаем юзернеймы (убираем @ если есть)
    usernames = []
    for line in lines:
        line = line.replace('@', '').strip()
        if line:
            usernames.append(line)
    
    if not usernames:
        await safe_send(
            message.chat.id,
            f"{e('cross')} Не найдено ни одного юзернейма",
            reply_markup=get_back_keyboard("functions")
        )
        return
    
    if len(usernames) > 50:
        await safe_send(
            message.chat.id,
            f"{e('cross')} Максимум 50 чатов за раз",
            reply_markup=get_back_keyboard("functions")
        )
        return
    
    user_id = message.from_user.id
    accounts = user_sessions.get(user_id, {})
    phone = list(accounts.keys())[0]
    client = accounts[phone]["client"]
    
    await state.clear()
    
    status_msg = await safe_send(
        message.chat.id,
        f"{e('loading')} <b>Начинаю вступление в чаты...</b>\n"
        f"Всего: {len(usernames)}\n"
        f"Задержка: 15 секунд"
    )
    
    joined = 0
    failed = 0
    failed_list = []
    
    for i, username in enumerate(usernames, 1):
        try:
            await client(JoinChannelRequest(username))
            joined += 1
            logging.info(f"Joined: {username}")
        except FloodWaitError as e:
            wait_time = e.seconds
            logging.warning(f"Flood wait {wait_time}s for {username}")
            await safe_edit(
                status_msg,
                f"{e('loading')} <b>Ожидание {wait_time} сек из-за FloodWait...</b>\n"
                f"Прогресс: {i}/{len(usernames)}"
            )
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
            logging.error(f"Failed to join {username}: {e}")
        
        # Обновление статуса каждые 5 чатов
        if i % 5 == 0 or i == len(usernames):
            progress_text = (
                f"{e('loading')} <b>Вступление в чаты...</b>\n"
                f"Прогресс: {i}/{len(usernames)}\n"
                f"{e('check')} Вступил: {joined}\n"
                f"{e('cross')} Ошибок: {failed}"
            )
            try:
                await safe_edit(status_msg, progress_text)
            except:
                pass
        
        # Задержка 15 секунд между вступлениями (кроме последнего)
        if i < len(usernames):
            await asyncio.sleep(15)
    
    # Финальный отчет
    result_text = (
        f"{e('check')} <b>Вступление завершено!</b>\n\n"
        f"{e('check')} Успешно: {joined}\n"
        f"{e('cross')} Ошибок: {failed}"
    )
    if failed_list:
        result_text += f"\n\n{e('cross')} <b>Не удалось вступить:</b>\n"
        result_text += "\n".join(f"• {u}" for u in failed_list[:10])
        if len(failed_list) > 10:
            result_text += f"\n... и еще {len(failed_list) - 10}"
    
    await safe_edit(status_msg, result_text, reply_markup=get_functions_keyboard())

# ============ ЗАПУСК ============

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
