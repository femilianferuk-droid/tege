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
from telethon.tl.types import Channel, Chat, User

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
SUPPORT_USERNAME = "@VestSupport"
MAX_ACCOUNTS = 5

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

user_sessions: Dict[int, Dict[str, any]] = {}
active_broadcasts: Dict[int, Dict[str, asyncio.Task]] = {}
pending_logins: Dict[int, Dict] = {}
dialogs_cache: Dict[int, Dict[int, List[Tuple[str, str]]]] = {}

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

def emoji(id: str) -> str:
    """Создает премиум эмодзи тег"""
    return f'<tg-emoji emoji-id="{id}"></tg-emoji>'

async def safe_edit_text(message: types.Message, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    """Безопасное редактирование сообщения"""
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message can't be edited" in str(e) or "message is not modified" in str(e):
            try:
                await message.delete()
            except:
                pass
            await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            raise

async def safe_send_message(chat_id: int, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    """Безопасная отправка сообщения"""
    try:
        return await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "can't parse entities" in str(e):
            # Fallback - удаляем все tg-emoji теги
            import re
            clean_text = re.sub(r'<tg-emoji[^>]*></tg-emoji>', '', text)
            clean_text = re.sub(r'<[^>]+>', '', clean_text)
            return await bot.send_message(chat_id, clean_text, reply_markup=reply_markup)
        else:
            raise

# Главное меню
def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="Менеджер аккаунтов")
    builder.button(text="Функции")
    builder.button(text="Поддержка")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

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

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    text = (
        f"{emoji(EMOJI['bot'])} <b>Добро пожаловать!</b>\n\n"
        f"<b>Главное меню:</b>\n"
        f"{emoji(EMOJI['settings'])} <b>Менеджер аккаунтов</b> — управление аккаунтами\n"
        f"{emoji(EMOJI['stats'])} <b>Функции</b> — рассылка сообщений\n"
        f"{emoji(EMOJI['megaphone'])} <b>Поддержка</b> — связь с поддержкой"
    )
    await safe_send_message(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.message(F.text == "Менеджер аккаунтов")
async def accounts_manager(message: types.Message):
    user_id = message.from_user.id
    count = len(user_sessions.get(user_id, {}))
    text = (
        f"{emoji(EMOJI['settings'])} <b>Менеджер аккаунтов</b>\n"
        f"{emoji(EMOJI['profile'])} Активных аккаунтов: {count}/{MAX_ACCOUNTS}"
    )
    await safe_send_message(message.chat.id, text, reply_markup=get_accounts_menu_keyboard())

@dp.message(F.text == "Функции")
async def functions_menu(message: types.Message):
    text = (
        f"{emoji(EMOJI['stats'])} <b>Функции</b>\n"
        f"{emoji(EMOJI['megaphone'])} Здесь вы можете запустить рассылку"
    )
    await safe_send_message(message.chat.id, text, reply_markup=get_functions_keyboard())

@dp.message(F.text == "Поддержка")
async def support(message: types.Message):
    text = f"{emoji(EMOJI['megaphone'])} <b>Поддержка</b>\nСвяжитесь с нами: {SUPPORT_USERNAME}"
    await safe_send_message(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except:
        pass
    await safe_send_message(
        callback.message.chat.id,
        f"{emoji(EMOJI['bot'])} <b>Главное меню</b>",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# ============ ДОБАВЛЕНИЕ АККАУНТА ============

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if len(user_sessions.get(user_id, {})) >= MAX_ACCOUNTS:
        await callback.answer("❌ Достигнут лимит аккаунтов!", show_alert=True)
        return
    
    text = (
        f"{emoji(EMOJI['apps'])} <b>Добавление аккаунта</b>\n"
        f"{emoji(EMOJI['write'])} Введите номер телефона в формате: +79123456789"
    )
    await safe_edit_text(callback.message, text, reply_markup=get_back_keyboard("accounts_manager"))
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await safe_send_message(
            message.chat.id,
            f"{emoji(EMOJI['cross'])} Неверный формат. Пример: +79123456789",
            reply_markup=get_back_keyboard("accounts_manager")
        )
        return
    
    user_id = message.from_user.id
    
    if user_id in user_sessions and phone in user_sessions[user_id]:
        await safe_send_message(
            message.chat.id,
            f"{emoji(EMOJI['cross'])} Этот аккаунт уже добавлен!",
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
            f"{emoji(EMOJI['gift'])} Код отправлен на {phone}\n"
            f"{emoji(EMOJI['write'])} Введите код из SMS:"
        )
        await safe_send_message(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as e:
        await client.disconnect()
        await safe_send_message(message.chat.id, f"{emoji(EMOJI['cross'])} Ошибка: {str(e)}", reply_markup=get_accounts_menu_keyboard())
        await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await safe_send_message(message.chat.id, "❌ Сессия истекла", reply_markup=get_accounts_menu_keyboard())
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
        text = f"{emoji(EMOJI['lock'])} Требуется 2FA\n{emoji(EMOJI['write'])} Введите пароль:"
        await safe_send_message(message.chat.id, text, reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as e:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send_message(message.chat.id, f"{emoji(EMOJI['cross'])} Ошибка: {str(e)}", reply_markup=get_accounts_menu_keyboard())
        await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    
    if user_id not in pending_logins:
        await safe_send_message(message.chat.id, "❌ Сессия истекла", reply_markup=get_accounts_menu_keyboard())
        await state.clear()
        return
    
    data = pending_logins[user_id]
    client = data["client"]
    phone = data["phone"]
    
    try:
        await client.sign_in(password=password)
        await on_successful_login(user_id, phone, client, message, state)
    except Exception as e:
        await client.disconnect()
        pending_logins.pop(user_id, None)
        await safe_send_message(message.chat.id, f"{emoji(EMOJI['cross'])} Ошибка 2FA: {str(e)}", reply_markup=get_accounts_menu_keyboard())
        await state.clear()

async def on_successful_login(user_id, phone, client, message, state):
    if user_id not in user_sessions:
        user_sessions[user_id] = {}
    user_sessions[user_id][phone] = {"client": client, "phone": phone}
    pending_logins.pop(user_id, None)
    
    # Сбрасываем кеш диалогов при добавлении нового аккаунта
    if user_id in dialogs_cache:
        del dialogs_cache[user_id]
    
    await safe_send_message(
        message.chat.id,
        f"{emoji(EMOJI['check'])} Аккаунт {phone} успешно добавлен!",
        reply_markup=get_accounts_menu_keyboard()
    )
    await state.clear()

# ============ МОИ АККАУНТЫ ============

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    
    if not accounts:
        text = f"{emoji(EMOJI['unlock'])} <b>Мои аккаунты</b>\n{emoji(EMOJI['cross'])} Нет добавленных аккаунтов"
        await safe_edit_text(callback.message, text, reply_markup=get_accounts_menu_keyboard())
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for phone in accounts:
        builder.button(text=f"📱 {phone}", callback_data=f"account_info_{phone}", style="default")
    builder.button(text="Назад", callback_data="accounts_manager", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    text = f"{emoji(EMOJI['profile'])} <b>Мои аккаунты</b>\n{emoji(EMOJI['unlock'])} Выберите аккаунт:"
    await safe_edit_text(callback.message, text, reply_markup=builder.as_markup())
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
    builder.button(text="Назад", callback_data="my_accounts", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    text = f"{emoji(EMOJI['profile'])} <b>Аккаунт:</b> {phone}\n{emoji(EMOJI['unlock'])} Статус: активен"
    await safe_edit_text(callback.message, text, reply_markup=builder.as_markup())
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
        
        # Сбрасываем кеш диалогов
        if user_id in dialogs_cache:
            del dialogs_cache[user_id]
    
    await safe_edit_text(
        callback.message,
        f"{emoji(EMOJI['check'])} Аккаунт {phone} удален",
        reply_markup=get_accounts_menu_keyboard()
    )
    await callback.answer("✅ Аккаунт удален!", show_alert=True)

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    count = len(user_sessions.get(user_id, {}))
    text = f"{emoji(EMOJI['settings'])} <b>Менеджер аккаунтов</b>\n{emoji(EMOJI['profile'])} Активных аккаунтов: {count}/{MAX_ACCOUNTS}"
    await safe_edit_text(callback.message, text, reply_markup=get_accounts_menu_keyboard())
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
    builder.button(text="Назад", callback_data="main_menu", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    await safe_edit_text(callback.message, f"{emoji(EMOJI['megaphone'])} <b>Рассылка</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "new_broadcast")
async def new_broadcast(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not user_sessions.get(user_id):
        await callback.answer("❌ Сначала добавьте аккаунт!", show_alert=True)
        return
    
    # Предзагружаем диалоги в фоне
    asyncio.create_task(preload_dialogs(user_id))
    
    await safe_edit_text(
        callback.message,
        f"{emoji(EMOJI['write'])} <b>Отправьте сообщение для рассылки:</b>",
        reply_markup=get_back_keyboard("broadcast")
    )
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()

async def preload_dialogs(user_id: int):
    """Предзагрузка диалогов в кеш"""
    if user_id in dialogs_cache and dialogs_cache[user_id]:
        return
    
    accounts = user_sessions.get(user_id, {})
    if not accounts:
        return
    
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
        
        # Группируем по страницам
        pages = {}
        for i in range(0, len(chats), 10):
            page_num = i // 10
            pages[page_num] = chats[i:i+10]
        
        dialogs_cache[user_id] = pages
        logging.info(f"Loaded {len(chats)} dialogs for user {user_id}")
    except Exception as e:
        logging.error(f"Error preloading dialogs: {e}")
        dialogs_cache[user_id] = {}

@dp.message(BroadcastStates.waiting_for_message)
async def broadcast_get_message(message: types.Message, state: FSMContext):
    await state.update_data(message_text=message.text or message.caption or "")
    
    await safe_send_message(
        message.chat.id,
        f"{emoji(EMOJI['write'])} <b>Введите количество сообщений для отправки в каждый чат:</b>",
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
        await safe_send_message(message.chat.id, f"{emoji(EMOJI['cross'])} Введите целое положительное число")
        return
    
    await state.update_data(message_count=count)
    await safe_send_message(
        message.chat.id,
        f"{emoji(EMOJI['write'])} <b>Введите задержку между сообщениями (в секундах):</b>",
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
        await safe_send_message(message.chat.id, f"{emoji(EMOJI['cross'])} Введите положительное число")
        return
    
    await state.update_data(delay=delay, selected_chats=[], current_page=0)
    
    user_id = message.from_user.id
    
    # Ждем загрузки диалогов если они еще не загружены
    if user_id not in dialogs_cache or not dialogs_cache[user_id]:
        status_msg = await safe_send_message(message.chat.id, f"{emoji(EMOJI['clock'])} Загружаем чаты...")
        await preload_dialogs(user_id)
        try:
            await status_msg.delete()
        except:
            pass
    
    await show_chat_selection(message, state, 0)

async def show_chat_selection(message: types.Message, state: FSMContext, page: int):
    user_id = message.from_user.id
    pages = dialogs_cache.get(user_id, {})
    page_chats = pages.get(page, [])
    
    if not page_chats and page == 0:
        # Если чатов нет, пробуем загрузить
        await preload_dialogs(user_id)
        pages = dialogs_cache.get(user_id, {})
        page_chats = pages.get(page, [])
    
    total_pages = len(pages)
    data = await state.get_data()
    selected = data.get("selected_chats", [])
    
    builder = InlineKeyboardBuilder()
    
    if page_chats:
        for chat_id, chat_name in page_chats:
            is_selected = chat_id in selected
            # Обрезаем имя до 30 символов для кнопки
            display_name = chat_name[:30]
            prefix = "✅ " if is_selected else ""
            builder.button(
                text=f"{prefix}{display_name}",
                callback_data=f"sel_{chat_id}",
                style="success" if is_selected else "default"
            )
    
    # Навигация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(types.InlineKeyboardButton(
            text="◀ Назад",
            callback_data=f"pg_{page - 1}"
        ))
    if page < total_pages - 1:
        nav_buttons.append(types.InlineKeyboardButton(
            text="Вперед ▶",
            callback_data=f"pg_{page + 1}"
        ))
    if nav_buttons:
        builder.row(*nav_buttons)
    
    # Кнопки действий
    if 0 < len(selected) <= 5:
        builder.button(
            text=f"🚀 Запустить ({len(selected)})",
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
    
    if not page_chats and total_pages == 0:
        text = (
            f"{emoji(EMOJI['people'])} <b>Чаты не найдены</b>\n"
            f"Проверьте, что у аккаунта есть диалоги"
        )
    else:
        text = (
            f"{emoji(EMOJI['people'])} <b>Выберите чаты (до 5):</b>\n"
            f"Выбрано: {len(selected)}/5 | Страница {page + 1}/{max(total_pages, 1)}"
        )
    
    await safe_send_or_edit(message, text, reply_markup=builder.as_markup())

async def safe_send_or_edit(message: types.Message, text: str, reply_markup=None):
    """Отправляет новое сообщение или редактирует существующее"""
    try:
        await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except TelegramBadRequest:
        try:
            await message.delete()
        except:
            pass
        await safe_send_message(message.chat.id, text, reply_markup=reply_markup)

@dp.callback_query(F.data.startswith("sel_"))
async def select_chat(callback: types.CallbackQuery, state: FSMContext):
    chat_id = callback.data.replace("sel_", "")
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
        await callback.answer("❌ Выберите хотя бы один чат!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    broadcast_id = f"bcast_{int(datetime.now().timestamp())}"
    
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
            message_text=data.get("message_text", ""),
            count=data.get("message_count", 1),
            delay=data.get("delay", 1),
            client=client
        )
    )
    active_broadcasts[user_id][broadcast_id] = task
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Остановить",
        callback_data=f"stop_broadcast_{broadcast_id}",
        style="danger",
        icon_custom_emoji_id=EMOJI["cross"]
    )
    builder.button(text="В меню", callback_data="main_menu", icon_custom_emoji_id=EMOJI["home"])
    builder.adjust(1)
    
    text = (
        f"{emoji(EMOJI['send'])} <b>Рассылка запущена!</b>\n"
        f"Чатов: {len(selected_chats)}\n"
        f"Сообщений в каждый: {data.get('message_count', 1)}\n"
        f"Задержка: {data.get('delay', 1)}с"
    )
    await safe_edit_text(callback.message, text, reply_markup=builder.as_markup())
    await state.clear()
    await callback.answer("🎉 Рассылка запущена!", show_alert=True)

async def execute_broadcast(user_id, broadcast_id, chats, message_text, count, delay, client):
    """Выполнение рассылки"""
    completed = 0
    errors = 0
    
    for chat_id_str in chats:
        if broadcast_id not in active_broadcasts.get(user_id, {}):
            logging.info(f"Broadcast {broadcast_id} stopped by user")
            break
        
        try:
            # Извлекаем ID
            entity_id = int(chat_id_str.split("_")[1])
            entity = await client.get_entity(entity_id)
            
            for i in range(count):
                if broadcast_id not in active_broadcasts.get(user_id, {}):
                    break
                
                try:
                    await client.send_message(entity, message_text)
                    completed += 1
                    await asyncio.sleep(delay)
                except Exception as e:
                    errors += 1
                    logging.error(f"Error sending message: {e}")
                    await asyncio.sleep(1)  # Пауза при ошибке
                    
        except Exception as e:
            errors += 1
            logging.error(f"Error getting entity {chat_id_str}: {e}")
            continue
    
    logging.info(f"Broadcast {broadcast_id} finished. Completed: {completed}, Errors: {errors}")
    
    # Удаляем завершенную рассылку
    if user_id in active_broadcasts and broadcast_id in active_broadcasts[user_id]:
        del active_broadcasts[user_id][broadcast_id]

@dp.callback_query(F.data == "active_broadcasts")
async def active_broadcasts_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    broadcasts = active_broadcasts.get(user_id, {})
    
    if not broadcasts:
        builder = InlineKeyboardBuilder()
        builder.button(text="Назад", callback_data="broadcast", icon_custom_emoji_id=EMOJI["back"])
        text = f"{emoji(EMOJI['unlock'])} <b>Нет активных рассылок</b>"
        await safe_edit_text(callback.message, text, reply_markup=builder.as_markup())
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for bid in broadcasts:
        builder.button(
            text=f"⏹ Остановить {bid[:8]}...",
            callback_data=f"stop_broadcast_{bid}",
            style="danger",
            icon_custom_emoji_id=EMOJI["cross"]
        )
    builder.button(text="Назад", callback_data="broadcast", icon_custom_emoji_id=EMOJI["back"])
    builder.adjust(1)
    
    text = f"{emoji(EMOJI['clock'])} <b>Активные рассылки:</b> {len(broadcasts)}"
    await safe_edit_text(callback.message, text, reply_markup=builder.as_markup())
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
    text = f"{emoji(EMOJI['cross'])} <b>Рассылка отменена</b>"
    await safe_edit_text(callback.message, text, reply_markup=get_functions_keyboard())
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
