import asyncio
import logging
import os
import re
import random
import sys
import traceback
import asyncpg
import json
import io
import aiohttp
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telethon import TelegramClient, errors, events
from telethon.tl.functions.photos import UploadProfilePhotoRequest, DeletePhotosRequest
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.types import (
    Channel, Chat, User, InputPhoto, InputFile,
    PeerUser, PeerChat, PeerChannel
)
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError,
    ChannelPrivateError, UsernameNotOccupiedError
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не найден в переменных окружения!")

# Константы
API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
SUPPORT_USERNAME = "@VestSupport"
ADMIN_ID = 7973988177
BUY_ACCOUNTS = "@v3estnikov"
DONATION_CHANNEL = "@VestSoftTG"
CRYPTO_RATE = 90

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Глобальные переменные
db_pool: Optional[asyncpg.Pool] = None
user_sessions: Dict[int, Dict[str, dict]] = {}
active_broadcasts: Dict[int, Dict[str, asyncio.Task]] = {}
pending_logins: Dict[int, Dict] = {}
dialogs_cache: Dict[int, Dict[str, List[Tuple[str, str]]]] = {}
user_chat_messages: Dict[int, List[int]] = {}
active_auto_reply_handlers: Dict[int, Dict] = {}

PLACEHOLDERS = {
    "{NICK}": "first_name", "{FIRSTNAME}": "first_name",
    "{LASTNAME}": "last_name", "{FULLNAME}": "full_name",
    "{USERNAME}": "username", "{ID}": "id", "{PHONE}": "phone",
}

# Состояния FSM
class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()
    waiting_for_account_name = State()
    waiting_for_account_description = State()
    waiting_for_avatar = State()
    waiting_for_parse_chat = State()

class BroadcastStates(StatesGroup):
    adding_messages = State()
    waiting_for_count = State()
    waiting_for_delay = State()
    selecting_chats = State()
    selecting_mode = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_confirm = State()

class CommandStates(StatesGroup):
    waiting_for_command_name = State()
    waiting_for_command_response = State()
    waiting_for_crypto_token = State()
    waiting_for_product = State()

class WelcomeStates(StatesGroup):
    waiting_for_welcome_message = State()

class ConfigStates(StatesGroup):
    waiting_for_config_name = State()

# Премиум эмодзи
E = {
    "settings": '<tg-emoji emoji-id="5870982283724328568">⚙</tg-emoji>',
    "profile": '<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji>',
    "people": '<tg-emoji emoji-id="5870772616305839506">👥</tg-emoji>',
    "file": '<tg-emoji emoji-id="5870528606328852614">📁</tg-emoji>',
    "stats": '<tg-emoji emoji-id="5870930636742595124">📊</tg-emoji>',
    "home": '<tg-emoji emoji-id="5873147866364514353">🏘</tg-emoji>',
    "lock": '<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji>',
    "unlock": '<tg-emoji emoji-id="6037496202990194718">🔓</tg-emoji>',
    "megaphone": '<tg-emoji emoji-id="6039422865189638057">📣</tg-emoji>',
    "check": '<tg-emoji emoji-id="5870633910337015697">✅</tg-emoji>',
    "cross": '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji>',
    "trash": '<tg-emoji emoji-id="5870875489362513438">🗑</tg-emoji>',
    "back": '<tg-emoji emoji-id="5893057118545646106">◁</tg-emoji>',
    "link": '<tg-emoji emoji-id="5769289093221454192">🔗</tg-emoji>',
    "info": '<tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji>',
    "bot": '<tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji>',
    "eye": '<tg-emoji emoji-id="6037397706505195857">👁</tg-emoji>',
    "send": '<tg-emoji emoji-id="5963103826075456248">⬆</tg-emoji>',
    "gift": '<tg-emoji emoji-id="6032644646587338669">🎁</tg-emoji>',
    "clock": '<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji>',
    "celebration": '<tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji>',
    "write": '<tg-emoji emoji-id="5870753782874246579">✍</tg-emoji>',
    "apps": '<tg-emoji emoji-id="5778672437122045013">📦</tg-emoji>',
    "code": '<tg-emoji emoji-id="5940433880585605708">🔨</tg-emoji>',
    "loading": '<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji>',
    "user_check": '<tg-emoji emoji-id="5891207662678317861">👤</tg-emoji>',
    "wallet": '<tg-emoji emoji-id="5769126056262898415">👛</tg-emoji>',
    "notification": '<tg-emoji emoji-id="6039486778597970865">🔔</tg-emoji>',
    "smile": '<tg-emoji emoji-id="5870764288364252592">🙂</tg-emoji>',
    "download": '<tg-emoji emoji-id="6039802767931871481">⬇</tg-emoji>',
    "money": '<tg-emoji emoji-id="5904462880941545555">🪙</tg-emoji>',
    "box": '<tg-emoji emoji-id="5884479287171485878">📦</tg-emoji>',
    "pencil": '<tg-emoji emoji-id="5870676941614354370">🖋</tg-emoji>',
    "media": '<tg-emoji emoji-id="6035128606563241721">🖼</tg-emoji>',
    "geo": '<tg-emoji emoji-id="6042011682497106307">📍</tg-emoji>',
    "calendar": '<tg-emoji emoji-id="5890937706803894250">📅</tg-emoji>',
    "tag": '<tg-emoji emoji-id="5886285355279193209">🏷</tg-emoji>',
}

E_ID = {
    "settings": "5870982283724328568", "profile": "5870994129244131212",
    "people": "5870772616305839506", "file": "5870528606328852614",
    "stats": "5870930636742595124", "home": "5873147866364514353",
    "lock": "6037249452824072506", "unlock": "6037496202990194718",
    "megaphone": "6039422865189638057", "check": "5870633910337015697",
    "cross": "5870657884844462243", "trash": "5870875489362513438",
    "back": "5893057118545646106", "link": "5769289093221454192",
    "info": "6028435952299413210", "bot": "6030400221232501136",
    "eye": "6037397706505195857", "send": "5963103826075456248",
    "gift": "6032644646587338669", "clock": "5983150113483134607",
    "celebration": "6041731551845159060", "write": "5870753782874246579",
    "apps": "5778672437122045013", "code": "5940433880585605708",
    "loading": "5345906554510012647", "user_check": "5891207662678317861",
    "wallet": "5769126056262898415", "notification": "6039486778597970865",
    "smile": "5870764288364252592", "download": "6039802767931871481",
    "money": "5904462880941545555", "box": "5884479287171485878",
    "pencil": "5870676941614354370", "subscribe": "6039450962865688331",
    "check_sub": "5774022692642492953", "media": "6035128606563241721",
    "geo": "6042011682497106307", "calendar": "5890937706803894250",
    "tag": "5886285355279193209",
}

def em(key: str) -> str:
    return E.get(key, "")

def eid(key: str) -> str:
    return E_ID.get(key, "")

def process_placeholders(text: str, user_info: dict, command_data: dict = None) -> str:
    result = text
    for placeholder, field in PLACEHOLDERS.items():
        if placeholder in result:
            value = user_info.get(field, "")
            if field == "full_name":
                first = user_info.get("first_name", "")
                last = user_info.get("last_name", "")
                value = f"{first} {last}".strip() or "Пользователь"
            result = result.replace(placeholder, str(value) if value else placeholder)
    
    if command_data:
        crypto_invoice = command_data.get("crypto_invoice")
        if crypto_invoice and "{CRYPTOBOT}" in result:
            result = result.replace("{CRYPTOBOT}", crypto_invoice.get("pay_url", ""))
        if "{RUB}" in result and command_data.get("crypto_amount"):
            rub_match = re.search(r'\{RUB\}(\d+)', result)
            if rub_match:
                result = result.replace(rub_match.group(0), str(command_data.get("crypto_amount", "")))
    
    return result

# ============ БАЗА ДАННЫХ ============

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                crypto_token TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                phone TEXT NOT NULL,
                account_name TEXT,
                description TEXT,
                avatar_path TEXT,
                session_file TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                is_selected BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, phone)
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS commands (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                response TEXT NOT NULL,
                has_crypto BOOLEAN DEFAULT FALSE,
                crypto_amount DECIMAL,
                product_text TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, command)
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS configs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                config_name TEXT NOT NULL,
                config_data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, config_name)
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS welcome_settings (
                user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                is_enabled BOOLEAN DEFAULT FALSE,
                welcome_message TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS crypto_invoices (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                invoice_id BIGINT,
                command_name TEXT,
                amount DECIMAL,
                product_text TEXT,
                chat_id BIGINT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW(),
                paid_at TIMESTAMP,
                payer_id BIGINT
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_members (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                chat_id BIGINT,
                chat_title TEXT,
                member_id BIGINT,
                member_username TEXT,
                member_first_name TEXT,
                member_last_name TEXT,
                parsed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_accounts_user ON accounts(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_commands_user ON commands(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_status ON crypto_invoices(status)")
    
    logger.info("База данных инициализирована")

async def save_user(user_id: int, username: str, first_name: str, last_name: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, username, first_name, last_name) VALUES ($1,$2,$3,$4)
            ON CONFLICT (user_id) DO UPDATE SET username=$2, first_name=$3, last_name=$4, updated_at=NOW()
        """, user_id, username, first_name, last_name)

async def get_crypto_token(user_id: int) -> Optional[str]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT crypto_token FROM users WHERE user_id=$1", user_id)
        return row['crypto_token'] if row else None

async def set_crypto_token(user_id: int, token: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, crypto_token) VALUES ($1,$2)
            ON CONFLICT (user_id) DO UPDATE SET crypto_token=$2, updated_at=NOW()
        """, user_id, token)

async def save_account(user_id: int, phone: str, session_file: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO accounts (user_id, phone, session_file) VALUES ($1,$2,$3)
            ON CONFLICT (user_id, phone) DO UPDATE SET is_active=TRUE, session_file=$3
        """, user_id, phone, session_file)

async def update_account_name(user_id: int, phone: str, name: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE accounts SET account_name=$3, updated_at=NOW() WHERE user_id=$1 AND phone=$2", user_id, phone, name)

async def update_account_description(user_id: int, phone: str, desc: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE accounts SET description=$3, updated_at=NOW() WHERE user_id=$1 AND phone=$2", user_id, phone, desc)

async def update_account_avatar(user_id: int, phone: str, path: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE accounts SET avatar_path=$3, updated_at=NOW() WHERE user_id=$1 AND phone=$2", user_id, phone, path)

async def select_account_db(user_id: int, phone: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE accounts SET is_selected=FALSE WHERE user_id=$1", user_id)
        await conn.execute("UPDATE accounts SET is_selected=TRUE WHERE user_id=$1 AND phone=$2", user_id, phone)

async def delete_account_db(user_id: int, phone: str):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM accounts WHERE user_id=$1 AND phone=$2", user_id, phone)

async def save_command_db(user_id: int, cmd: str, response: str, has_crypto: bool = False, amount: float = None, product: str = None):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO commands (user_id, command, response, has_crypto, crypto_amount, product_text) VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (user_id, command) DO UPDATE SET response=$3, has_crypto=$4, crypto_amount=$5, product_text=$6
        """, user_id, cmd, response, has_crypto, amount, product)

async def get_commands_db(user_id: int) -> Dict[str, dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT command, response, has_crypto, crypto_amount, product_text FROM commands WHERE user_id=$1", user_id)
        return {r['command']: {'response': r['response'], 'has_crypto': r['has_crypto'], 'crypto_amount': float(r['crypto_amount']) if r['crypto_amount'] else None, 'product_text': r['product_text']} for r in rows}

async def delete_command_db(user_id: int, cmd: str):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM commands WHERE user_id=$1 AND command=$2", user_id, cmd)

async def save_config_db(user_id: int, name: str, data: dict):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO configs (user_id, config_name, config_data) VALUES ($1,$2,$3)
            ON CONFLICT (user_id, config_name) DO UPDATE SET config_data=$3
        """, user_id, name, json.dumps(data))

async def get_configs_db(user_id: int) -> Dict[str, dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT config_name, config_data FROM configs WHERE user_id=$1", user_id)
        return {r['config_name']: json.loads(r['config_data']) for r in rows}

async def save_welcome_db(user_id: int, enabled: bool, msg: str = None):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO welcome_settings (user_id, is_enabled, welcome_message) VALUES ($1,$2,$3)
            ON CONFLICT (user_id) DO UPDATE SET is_enabled=$2, welcome_message=$3, updated_at=NOW()
        """, user_id, enabled, msg)

async def get_welcome_db(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_enabled, welcome_message FROM welcome_settings WHERE user_id=$1", user_id)
        return {'is_enabled': row['is_enabled'], 'welcome_message': row['welcome_message']} if row else {'is_enabled': False, 'welcome_message': None}

async def create_invoice_db(user_id: int, invoice_id: int, cmd: str, amount: float, product: str, chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO crypto_invoices (user_id, invoice_id, command_name, amount, product_text, chat_id) VALUES ($1,$2,$3,$4,$5,$6)", user_id, invoice_id, cmd, amount, product, chat_id)

async def update_invoice_status_db(invoice_id: int, status: str, payer_id: int = None):
    async with db_pool.acquire() as conn:
        if status == 'paid':
            await conn.execute("UPDATE crypto_invoices SET status=$2, paid_at=NOW(), payer_id=$3 WHERE invoice_id=$1", invoice_id, status, payer_id)
        else:
            await conn.execute("UPDATE crypto_invoices SET status=$2 WHERE invoice_id=$1", invoice_id, status)

async def get_pending_invoice_db(user_id: int, cmd: str, chat_id: int) -> Optional[dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM crypto_invoices WHERE user_id=$1 AND command_name=$2 AND chat_id=$3 AND status='pending' ORDER BY created_at DESC LIMIT 1", user_id, cmd, chat_id)
        return dict(row) if row else None

async def get_users_count_db() -> int:
    async with db_pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM users")

# ============ CRYPTOBOT API ============

async def create_crypto_invoice(user_id: int, amount: float, description: str) -> Optional[dict]:
    token = await get_crypto_token(user_id)
    if not token:
        return None
    
    url = "https://pay.crypt.bot/api/createInvoice"
    headers = {"Crypto-Pay-API-Token": token}
    
    # Конвертация рублей в USDT
    usdt_amount = round(amount / CRYPTO_RATE, 2)
    
    data = {
        "asset": "USDT",
        "amount": str(usdt_amount),
        "description": description[:100],
        "paid_btn_name": "callback",
        "paid_btn_url": f"https://t.me/{(await bot.get_me()).username}",
        "expires_in": 3600
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as resp:
                result = await resp.json()
                if result.get("ok"):
                    return result["result"]
                logger.error(f"CryptoBot API error: {result}")
    except Exception as ex:
        logger.error(f"CryptoBot create invoice error: {ex}")
    return None

async def check_crypto_invoice_status(user_id: int, invoice_id: int) -> Optional[str]:
    token = await get_crypto_token(user_id)
    if not token:
        return None
    
    url = "https://pay.crypt.bot/api/getInvoices"
    headers = {"Crypto-Pay-API-Token": token}
    params = {"invoice_ids": str(invoice_id)}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                result = await resp.json()
                if result.get("ok") and result["result"]["items"]:
                    return result["result"]["items"][0]["status"]
    except Exception as ex:
        logger.error(f"CryptoBot check invoice error: {ex}")
    return None

# ============ ПАРСИНГ УЧАСТНИКОВ ============

async def parse_chat_members(client: TelegramClient, chat_entity, user_id: int) -> Tuple[str, int]:
    try:
        chat_title = getattr(chat_entity, 'title', str(chat_entity.id))
        participants = await client.get_participants(chat_entity, limit=5000)
        
        async with db_pool.acquire() as conn:
            for member in participants:
                await conn.execute("""
                    INSERT INTO chat_members (user_id, chat_id, chat_title, member_id, member_username, member_first_name, member_last_name)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                """, user_id, chat_entity.id, chat_title, member.id,
                    getattr(member, 'username', '') or '',
                    getattr(member, 'first_name', '') or '',
                    getattr(member, 'last_name', '') or '')
        
        filename = f"members_{chat_entity.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        filepath = Path("exports") / filename
        filepath.parent.mkdir(exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"Участники чата: {chat_title}\n")
            f.write(f"ID чата: {chat_entity.id}\n")
            f.write(f"Всего участников: {len(participants)}\n")
            f.write(f"Дата парсинга: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n\n")
            
            for i, member in enumerate(participants, 1):
                username = getattr(member, 'username', '') or ''
                first_name = getattr(member, 'first_name', '') or ''
                last_name = getattr(member, 'last_name', '') or ''
                full_name = f"{first_name} {last_name}".strip()
                
                f.write(f"{i}. ID: {member.id}\n")
                f.write(f"   Username: @{username if username else 'Нет'}\n")
                f.write(f"   Имя: {full_name or 'Не указано'}\n")
                if hasattr(member, 'phone') and member.phone:
                    f.write(f"   Телефон: {member.phone}\n")
                f.write("\n")
        
        return str(filepath), len(participants)
    except Exception as ex:
        logger.error(f"Parse members error: {ex}")
        raise

# ============ КЛАВИАТУРЫ ============

def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="Менеджер аккаунтов", icon_custom_emoji_id=eid("settings"))
    builder.button(text="Функции", icon_custom_emoji_id=eid("stats"))
    builder.button(text="Поддержка", icon_custom_emoji_id=eid("megaphone"))
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def get_accounts_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Добавить аккаунт", callback_data="add_account", style="primary", icon_custom_emoji_id=eid("gift"))
    builder.button(text="Мои аккаунты", callback_data="my_accounts", style="success", icon_custom_emoji_id=eid("profile"))
    builder.button(text="Выбрать аккаунт", callback_data="select_account", style="default", icon_custom_emoji_id=eid("user_check"))
    builder.button(text="Парсинг участников", callback_data="parse_members", style="primary", icon_custom_emoji_id=eid("people"))
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(2, 1)
    return builder.as_markup()

def get_account_info_keyboard(phone: str, is_selected: bool):
    builder = InlineKeyboardBuilder()
    builder.button(text="Изменить имя", callback_data=f"edit_name_{phone}", style="default", icon_custom_emoji_id=eid("pencil"))
    builder.button(text="Изменить описание", callback_data=f"edit_desc_{phone}", style="default", icon_custom_emoji_id=eid("write"))
    builder.button(text="Изменить аватар", callback_data=f"edit_avatar_{phone}", style="default", icon_custom_emoji_id=eid("media"))
    if not is_selected:
        builder.button(text="Выбрать", callback_data=f"sel_{phone}", style="success", icon_custom_emoji_id=eid("user_check"))
    builder.button(text="Удалить", callback_data=f"del_{phone}", style="danger", icon_custom_emoji_id=eid("trash"))
    builder.button(text="Назад", callback_data="my_accounts", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def get_functions_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Рассылка", callback_data="broadcast", style="primary", icon_custom_emoji_id=eid("megaphone"))
    builder.button(text="Команды", callback_data="commands_menu", style="success", icon_custom_emoji_id=eid("code"))
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    return builder.as_markup()

def get_commands_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Добавить команду", callback_data="add_command", style="primary", icon_custom_emoji_id=eid("code"))
    builder.button(text="Мои команды", callback_data="my_commands", style="success", icon_custom_emoji_id=eid("file"))
    builder.button(text="Crypto Token", callback_data="set_crypto_token", style="primary", icon_custom_emoji_id=eid("money"))
    builder.button(text="Приветствие новым", callback_data="welcome_setup", style="primary", icon_custom_emoji_id=eid("gift"))
    builder.button(text="Сохранить конфиг", callback_data="save_config", style="default", icon_custom_emoji_id=eid("download"))
    builder.button(text="Загрузить конфиг", callback_data="load_config", style="default", icon_custom_emoji_id=eid("box"))
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Статистика", callback_data="admin_stats", style="primary", icon_custom_emoji_id=eid("stats"))
    builder.button(text="Рассылка всем", callback_data="admin_broadcast", style="success", icon_custom_emoji_id=eid("megaphone"))
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    return builder.as_markup()

def get_back_keyboard(callback_data: str = "main_menu"):
    builder = InlineKeyboardBuilder()
    builder.button(text="Назад", callback_data=callback_data, style="default", icon_custom_emoji_id=eid("back"))
    return builder.as_markup()

def get_mode_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Одновременная", callback_data="mode_sync", style="primary", icon_custom_emoji_id=eid("send"))
    builder.button(text="Рандомная", callback_data="mode_random", style="success", icon_custom_emoji_id=eid("loading"))
    builder.button(text="Назад", callback_data="cancel_broadcast", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(2, 1)
    return builder.as_markup()

def get_message_actions_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="Добавить сообщение", callback_data="add_msg", style="primary", icon_custom_emoji_id=eid("write"))
    builder.button(text="Запустить рассылку", callback_data="start_msg_config", style="success", icon_custom_emoji_id=eid("send"))
    builder.button(text="Отмена", callback_data="cancel_broadcast", style="danger", icon_custom_emoji_id=eid("cross"))
    builder.adjust(1)
    return builder.as_markup()

# ============ УТИЛИТЫ ============

async def safe_send(chat_id: int, text: str, reply_markup=None, **kwargs):
    try:
        return await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=reply_markup, **kwargs)
    except TelegramBadRequest as e:
        if "can't parse" in str(e):
            clean = re.sub(r'<tg-emoji[^>]*>.*?</tg-emoji>', '', text)
            clean = re.sub(r'<[^>]+>', '', clean)
            return await bot.send_message(chat_id, clean, reply_markup=reply_markup, **kwargs)
        raise

async def safe_edit(message: types.Message, text: str, reply_markup=None):
    try:
        return await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        err = str(e)
        if "message can't be edited" in err or "message is not modified" in err:
            try: await message.delete()
            except: pass
            return await safe_send(message.chat.id, text, reply_markup=reply_markup)
        if "can't parse" in err:
            try: await message.delete()
            except: pass
            clean = re.sub(r'<tg-emoji[^>]*>.*?</tg-emoji>', '', text)
            clean = re.sub(r'<[^>]+>', '', clean)
            return await safe_send(message.chat.id, clean, reply_markup=reply_markup)
        raise
    except Exception:
        try: await message.delete()
        except: pass
        return await safe_send(message.chat.id, text, reply_markup=reply_markup)

async def delete_user_chat_messages(user_id: int):
    if user_id in user_chat_messages:
        for msg_id in user_chat_messages[user_id]:
            try: await bot.delete_message(user_id, msg_id)
            except: pass
        user_chat_messages[user_id] = []

async def add_user_chat_message(user_id: int, message_id: int):
    if user_id not in user_chat_messages: user_chat_messages[user_id] = []
    user_chat_messages[user_id].append(message_id)

def get_active_account(user_id: int) -> Optional[Tuple[str, TelegramClient]]:
    accounts = user_sessions.get(user_id, {})
    if not accounts: return None
    for phone, data in accounts.items():
        if data.get("is_selected"): return phone, data["client"]
    phone = list(accounts.keys())[0]
    return phone, accounts[phone]["client"]

async def load_dialogs(user_id: int) -> bool:
    account = get_active_account(user_id)
    if not account: return False
    phone, client = account
    try:
        dialogs = await client.get_dialogs(limit=200)
        chats = []
        for dialog in dialogs:
            entity = dialog.entity
            if isinstance(entity, User):
                name = f"{entity.first_name or ''} {entity.last_name or ''}".strip() or f"User {entity.id}"
                chat_id = f"user_{entity.id}"
            elif isinstance(entity, (Chat, Channel)):
                name = entity.title or f"Chat {entity.id}"
                chat_id = f"chat_{entity.id}"
            else: continue
            chats.append((chat_id, name))
        if user_id not in dialogs_cache: dialogs_cache[user_id] = {}
        dialogs_cache[user_id][phone] = chats
        return True
    except Exception as ex:
        logger.error(f"Error loading dialogs: {ex}")
        return False

# ============ ОСНОВНЫЕ ОБРАБОТЧИКИ ============

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    await save_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    
    text = (
        em('bot') + " <b>Добро пожаловать!</b>\n\n"
        "<b>Главное меню:</b>\n"
        + em('settings') + " <b>Менеджер аккаунтов</b> — управление аккаунтами\n"
        + em('stats') + " <b>Функции</b> — рассылка, команды\n"
        + em('megaphone') + " <b>Поддержка</b> — " + SUPPORT_USERNAME + "\n\n"
        + em('wallet') + " <b>Купить аккаунт:</b> " + BUY_ACCOUNTS + "\n"
        + em('link') + " <b>Новости:</b> " + DONATION_CHANNEL
    )
    await safe_send(message.chat.id, text, reply_markup=get_main_keyboard())

@dp.message(Command("admin"))
async def admin_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await safe_send(message.chat.id, em('cross') + " Нет доступа")
        return
    text = em('lock') + " <b>Админ панель</b>\n\n" + em('stats') + " Статистика\n" + em('megaphone') + " Рассылка всем"
    await safe_send(message.chat.id, text, reply_markup=get_admin_keyboard())

@dp.message(F.text == "Менеджер аккаунтов")
async def accounts_manager_handler(message: types.Message):
    user_id = message.from_user.id
    count = len(user_sessions.get(user_id, {}))
    text = em('settings') + " <b>Менеджер аккаунтов</b>\n" + em('profile') + " Активных: " + str(count) + " (безлимит)"
    await safe_send(message.chat.id, text, reply_markup=get_accounts_menu_keyboard())

@dp.message(F.text == "Функции")
async def functions_menu_handler(message: types.Message):
    text = em('stats') + " <b>Функции</b>\n" + em('megaphone') + " Рассылка\n" + em('code') + " Команды"
    await safe_send(message.chat.id, text, reply_markup=get_functions_keyboard())

@dp.message(F.text == "Поддержка")
async def support_handler(message: types.Message):
    await safe_send(message.chat.id, em('megaphone') + " <b>Поддержка</b>\n" + em('link') + " " + SUPPORT_USERNAME, reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery):
    try: await callback.message.delete()
    except: pass
    await safe_send(callback.message.chat.id, em('bot') + " <b>Главное меню</b>", reply_markup=get_main_keyboard())
    await callback.answer()

# ============ ДОБАВЛЕНИЕ АККАУНТА ============

@dp.callback_query(F.data == "add_account")
async def add_account(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit(callback.message, em('apps') + " <b>Добавление аккаунта</b>\n" + em('write') + " Введите номер: <code>+79123456789</code>", reply_markup=get_back_keyboard("accounts_manager"))
    await state.set_state(AccountStates.waiting_for_phone)
    await callback.answer()

@dp.message(AccountStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{10,15}$', phone):
        await safe_send(message.chat.id, em('cross') + " Неверный формат."); return
    user_id = message.from_user.id
    client = TelegramClient('sessions/' + str(user_id) + '_' + phone.replace("+", ""), API_ID, API_HASH)
    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        pending_logins[user_id] = {"client": client, "phone": phone, "phone_code_hash": sent_code.phone_code_hash}
        await safe_send(message.chat.id, em('gift') + " Код на <code>" + phone + "</code>\n" + em('write') + " Введите код:", reply_markup=get_back_keyboard("accounts_manager"))
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as ex:
        await client.disconnect(); await safe_send(message.chat.id, em('cross') + " Ошибка: " + str(ex)); await state.clear()

@dp.message(AccountStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    if user_id not in pending_logins: await safe_send(message.chat.id, em('cross') + " Сессия истекла"); await state.clear(); return
    data = pending_logins[user_id]
    try:
        await data["client"].sign_in(phone=data["phone"], code=code, phone_code_hash=data["phone_code_hash"])
        await on_successful_login(user_id, data["phone"], data["client"], message, state)
    except SessionPasswordNeededError:
        await safe_send(message.chat.id, em('lock') + " Требуется 2FA\n" + em('write') + " Введите пароль:"); await state.set_state(AccountStates.waiting_for_2fa)
    except Exception as ex:
        await data["client"].disconnect(); pending_logins.pop(user_id, None); await safe_send(message.chat.id, em('cross') + " Ошибка: " + str(ex)); await state.clear()

@dp.message(AccountStates.waiting_for_2fa)
async def process_2fa(message: types.Message, state: FSMContext):
    password = message.text.strip()
    user_id = message.from_user.id
    if user_id not in pending_logins: await safe_send(message.chat.id, em('cross') + " Сессия истекла"); await state.clear(); return
    data = pending_logins[user_id]
    try:
        await data["client"].sign_in(password=password)
        await on_successful_login(user_id, data["phone"], data["client"], message, state)
    except Exception as ex:
        await data["client"].disconnect(); pending_logins.pop(user_id, None); await safe_send(message.chat.id, em('cross') + " Ошибка: " + str(ex)); await state.clear()

async def on_successful_login(user_id, phone, client, message, state):
    if user_id not in user_sessions: user_sessions[user_id] = {}
    user_sessions[user_id][phone] = {"client": client, "phone": phone, "is_selected": False}
    pending_logins.pop(user_id, None); dialogs_cache.pop(user_id, None)
    await save_account(user_id, phone, f'sessions/{user_id}_{phone.replace("+", "")}.session')
    if len(user_sessions[user_id]) == 1:
        user_sessions[user_id][phone]["is_selected"] = True
        await select_account_db(user_id, phone)
    await safe_send(message.chat.id, em('check') + " Аккаунт <code>" + phone + "</code> добавлен!", reply_markup=get_accounts_menu_keyboard())
    await state.clear()

# ============ УПРАВЛЕНИЕ АККАУНТАМИ ============

@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    if not accounts: await safe_edit(callback.message, em('unlock') + " <b>Мои аккаунты</b>\n\n" + em('cross') + " Нет аккаунтов", reply_markup=get_accounts_menu_keyboard()); await callback.answer(); return
    builder = InlineKeyboardBuilder()
    for phone in accounts: builder.button(text=phone, callback_data="acc_" + phone, style="default")
    builder.button(text="Назад", callback_data="accounts_manager", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    await safe_edit(callback.message, em('profile') + " <b>Мои аккаунты</b>\n" + em('unlock') + " Всего: " + str(len(accounts)), reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "select_account")
async def select_account_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    accounts = user_sessions.get(user_id, {})
    if not accounts: await callback.answer("Нет аккаунтов!", show_alert=True); return
    builder = InlineKeyboardBuilder()
    for phone, data in accounts.items():
        prefix = "✅ " if data.get("is_selected") else ""
        builder.button(text=prefix + phone, callback_data="sel_" + phone, style="success" if data.get("is_selected") else "default")
    builder.button(text="Назад", callback_data="accounts_manager", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    await safe_edit(callback.message, em('user_check') + " <b>Выбор аккаунта</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("sel_"))
async def select_account_handler(callback: types.CallbackQuery):
    phone = callback.data.replace("sel_", "")
    user_id = callback.from_user.id
    if user_id in user_sessions and phone in user_sessions[user_id]:
        for p in user_sessions[user_id]: user_sessions[user_id][p]["is_selected"] = False
        user_sessions[user_id][phone]["is_selected"] = True
        dialogs_cache.pop(user_id, None)
        await select_account_db(user_id, phone)
        await callback.answer("Выбран " + phone, show_alert=True); await select_account_menu(callback)

@dp.callback_query(F.data.startswith("acc_"))
async def account_info(callback: types.CallbackQuery):
    phone = callback.data.replace("acc_", "")
    user_id = callback.from_user.id
    data = user_sessions.get(user_id, {}).get(phone)
    if not data: await callback.answer("Не найден", show_alert=True); return
    is_selected = data.get("is_selected", False)
    text = em('profile') + " <b>Аккаунт:</b> <code>" + phone + "</code>\n" + em('check') + " Статус: " + ("✅ Выбран" if is_selected else "⚪ Доступен")
    await safe_edit(callback.message, text, reply_markup=get_account_info_keyboard(phone, is_selected))
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_name_"))
async def edit_name_handler(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.replace("edit_name_", "")
    await state.update_data(edit_phone=phone)
    await safe_edit(callback.message, em('pencil') + " <b>Новое имя:</b>", reply_markup=get_back_keyboard(f"acc_{phone}"))
    await state.set_state(AccountStates.waiting_for_account_name)
    await callback.answer()

@dp.message(AccountStates.waiting_for_account_name)
async def process_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    data = await state.get_data(); phone = data.get("edit_phone")
    user_id = message.from_user.id
    await update_account_name(user_id, phone, name)
    account = user_sessions.get(user_id, {}).get(phone)
    if account:
        try: await account["client"](UpdateProfileRequest(first_name=name))
        except: pass
    await state.clear()
    await safe_send(message.chat.id, em('check') + " <b>Имя обновлено!</b>", reply_markup=get_accounts_menu_keyboard())

@dp.callback_query(F.data.startswith("edit_desc_"))
async def edit_desc_handler(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.replace("edit_desc_", "")
    await state.update_data(edit_phone=phone)
    await safe_edit(callback.message, em('write') + " <b>Новое описание:</b>", reply_markup=get_back_keyboard(f"acc_{phone}"))
    await state.set_state(AccountStates.waiting_for_account_description)
    await callback.answer()

@dp.message(AccountStates.waiting_for_account_description)
async def process_description(message: types.Message, state: FSMContext):
    desc = message.text.strip()
    data = await state.get_data(); phone = data.get("edit_phone")
    user_id = message.from_user.id
    await update_account_description(user_id, phone, desc)
    account = user_sessions.get(user_id, {}).get(phone)
    if account:
        try: await account["client"](UpdateProfileRequest(about=desc))
        except: pass
    await state.clear()
    await safe_send(message.chat.id, em('check') + " <b>Описание обновлено!</b>", reply_markup=get_accounts_menu_keyboard())

@dp.callback_query(F.data.startswith("edit_avatar_"))
async def edit_avatar_handler(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.replace("edit_avatar_", "")
    await state.update_data(edit_phone=phone)
    await safe_edit(callback.message, em('media') + " <b>Отправьте фото:</b>", reply_markup=get_back_keyboard(f"acc_{phone}"))
    await state.set_state(AccountStates.waiting_for_avatar)
    await callback.answer()

@dp.message(AccountStates.waiting_for_avatar, F.photo)
async def process_avatar(message: types.Message, state: FSMContext):
    data = await state.get_data(); phone = data.get("edit_phone")
    user_id = message.from_user.id
    account = user_sessions.get(user_id, {}).get(phone)
    if account and message.photo:
        try:
            photo_file = await bot.download(message.photo[-1])
            photo_bytes = photo_file.read() if hasattr(photo_file, 'read') else photo_file
            uploaded = await account["client"].upload_file(io.BytesIO(photo_bytes))
            await account["client"](UploadProfilePhotoRequest(uploaded))
            photos = await account["client"].get_profile_photos("me", limit=10)
            if len(photos) > 1:
                old = [InputPhoto(id=p.id, access_hash=p.access_hash, file_reference=p.file_reference) for p in photos[1:]]
                await account["client"](DeletePhotosRequest(old))
            avatar_path = f"avatars/{user_id}_{phone}.jpg"
            Path("avatars").mkdir(exist_ok=True)
            with open(avatar_path, 'wb') as f: f.write(photo_bytes)
            await update_account_avatar(user_id, phone, avatar_path)
            await safe_send(message.chat.id, em('check') + " <b>Аватар обновлен!</b>", reply_markup=get_accounts_menu_keyboard())
        except Exception as ex:
            await safe_send(message.chat.id, em('cross') + " <b>Ошибка:</b> " + str(ex)[:200], reply_markup=get_accounts_menu_keyboard())
    await state.clear()

@dp.callback_query(F.data.startswith("del_"))
async def delete_account_handler(callback: types.CallbackQuery):
    phone = callback.data.replace("del_", "")
    user_id = callback.from_user.id
    if user_id in user_sessions and phone in user_sessions[user_id]:
        try: await user_sessions[user_id][phone]["client"].disconnect()
        except: pass
        was_selected = user_sessions[user_id][phone].get("is_selected")
        del user_sessions[user_id][phone]
        dialogs_cache.pop(user_id, None)
        await delete_account_db(user_id, phone)
        if was_selected and user_sessions.get(user_id):
            first = list(user_sessions[user_id].keys())[0]
            user_sessions[user_id][first]["is_selected"] = True
            await select_account_db(user_id, first)
    await safe_edit(callback.message, em('check') + " Аккаунт удален", reply_markup=get_accounts_menu_keyboard())
    await callback.answer("Удален!", show_alert=True)

@dp.callback_query(F.data == "accounts_manager")
async def back_to_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await safe_edit(callback.message, em('settings') + " <b>Менеджер аккаунтов</b>\n" + em('profile') + " Активных: " + str(len(user_sessions.get(user_id, {}))), reply_markup=get_accounts_menu_keyboard())
    await callback.answer()

# ============ ПАРСИНГ УЧАСТНИКОВ ============

@dp.callback_query(F.data == "parse_members")
async def parse_members_menu(callback: types.CallbackQuery, state: FSMContext):
    if not get_active_account(callback.from_user.id): await callback.answer("Сначала добавьте аккаунт!", show_alert=True); return
    await safe_edit(callback.message, em('people') + " <b>Парсинг участников</b>\n\n" + em('write') + " Отправьте username чата:\n<code>@chatname</code>", reply_markup=get_back_keyboard("accounts_manager"))
    await state.set_state(AccountStates.waiting_for_parse_chat)
    await callback.answer()

@dp.message(AccountStates.waiting_for_parse_chat)
async def process_parse_chat(message: types.Message, state: FSMContext):
    chat_username = message.text.strip().replace('@', '').replace('https://t.me/', '')
    user_id = message.from_user.id
    account = get_active_account(user_id)
    if not account: await safe_send(message.chat.id, em('cross') + " Нет аккаунта"); return
    phone, client = account
    status_msg = await safe_send(message.chat.id, em('loading') + " <b>Парсим участников...</b>\nЭто может занять время...")
    try:
        entity = await client.get_entity(chat_username)
        filepath, count = await parse_chat_members(client, entity, user_id)
        try: await status_msg.delete()
        except: pass
        if filepath:
            await message.answer_document(FSInputFile(filepath), caption=em('check') + f" <b>Парсинг завершен!</b>\nУчастников: {count}")
        else:
            await safe_send(message.chat.id, em('cross') + " <b>Ошибка парсинга</b>")
    except Exception as ex:
        try: await status_msg.delete()
        except: pass
        await safe_send(message.chat.id, em('cross') + " <b>Ошибка:</b> " + str(ex)[:200])
    await state.clear()

# ============ СИСТЕМА КОМАНД ============

@dp.callback_query(F.data == "commands_menu")
async def commands_menu(callback: types.CallbackQuery):
    text = (
        em('code') + " <b>Команды</b>\n\n"
        + em('info') + " Плейсхолдеры: {NICK}, {FIRSTNAME}, {LASTNAME}, {FULLNAME}, {USERNAME}, {ID}\n"
        + em('money') + " Для Crypto: {CRYPTOBOT} - ссылка, {RUB} - сумма\n"
        + em('info') + " Пример: {RUB}100 = 100 рублей\n"
        + em('info') + " При создании Crypto команды авто-создается .оплатил"
    )
    await safe_edit(callback.message, text, reply_markup=get_commands_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "set_crypto_token")
async def set_crypto_token_handler(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit(callback.message, em('money') + " <b>Настройка Crypto Bot Token</b>\n\n" + em('write') + " Отправьте ваш Crypto Bot API Token:\n<code>123456:ABC...</code>\n\n" + em('info') + " Получить: @CryptoBot", reply_markup=get_back_keyboard("commands_menu"))
    await state.set_state(CommandStates.waiting_for_crypto_token)
    await callback.answer()

@dp.message(CommandStates.waiting_for_crypto_token)
async def process_crypto_token(message: types.Message, state: FSMContext):
    token = message.text.strip()
    await set_crypto_token(message.from_user.id, token)
    await state.clear()
    await safe_send(message.chat.id, em('check') + " <b>Crypto Token сохранен!</b>", reply_markup=get_commands_keyboard())

@dp.callback_query(F.data == "add_command")
async def add_command_handler(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit(callback.message, em('write') + " <b>Добавление команды</b>\n\nОтправьте название (с точкой):\n<code>.прайс</code>\n<code>.товар</code>", reply_markup=get_back_keyboard("commands_menu"))
    await state.set_state(CommandStates.waiting_for_command_name)
    await callback.answer()

@dp.message(CommandStates.waiting_for_command_name)
async def command_get_name(message: types.Message, state: FSMContext):
    cmd_name = message.text.strip().lower()
    if not cmd_name.startswith('.'): await safe_send(message.chat.id, em('cross') + " Должна начинаться с точки"); return
    if len(cmd_name) < 2: await safe_send(message.chat.id, em('cross') + " Слишком короткая"); return
    existing = await get_commands_db(message.from_user.id)
    if cmd_name in existing: await safe_send(message.chat.id, em('cross') + " Такая команда уже есть!"); return
    await state.update_data(command_name=cmd_name)
    text = (
        em('write') + " <b>Команда: <code>" + cmd_name + "</code></b>\n\n"
        + em('info') + " Отправьте ответ:\n"
        "Плейсхолдеры: {NICK}, {FIRSTNAME}, {LASTNAME}, {FULLNAME}, {USERNAME}, {ID}\n"
        + em('money') + " Для Crypto: {CRYPTOBOT} - ссылка, {RUB} - сумма\n"
        + em('info') + " Пример: {RUB}100 = 100 рублей\n"
        "Поддерживается HTML и премиум эмодзи"
    )
    await safe_send(message.chat.id, text, reply_markup=get_back_keyboard("commands_menu"))
    await state.set_state(CommandStates.waiting_for_command_response)

@dp.message(CommandStates.waiting_for_command_response)
async def command_get_response_handler(message: types.Message, state: FSMContext):
    response = message.html_text if message.html_text else message.text or ""
    data = await state.get_data()
    cmd_name = data.get("command_name", ".unknown")
    user_id = message.from_user.id
    
    has_crypto = "{CRYPTOBOT}" in response
    crypto_amount = None
    product_text = None
    
    if has_crypto:
        rub_match = re.search(r'\{RUB\}(\d+)', response)
        if rub_match:
            crypto_amount = float(rub_match.group(1))
            await state.update_data(command_name=cmd_name, command_response=response, crypto_amount=crypto_amount)
            await safe_send(message.chat.id, em('write') + " <b>Опишите товар/услугу для оплаты:</b>\n(будет показано покупателю)", reply_markup=get_back_keyboard("commands_menu"))
            await state.set_state(CommandStates.waiting_for_product)
            return
        else:
            await safe_send(message.chat.id, em('cross') + " Укажите сумму через {RUB}!\nПример: {RUB}100"); return
    
    await save_command_db(user_id, cmd_name, response)
    await state.clear()
    await setup_command_handler(user_id)
    await safe_send(message.chat.id, em('check') + " <b>Команда добавлена!</b>\n<code>" + cmd_name + "</code>", reply_markup=get_commands_keyboard())

@dp.message(CommandStates.waiting_for_product)
async def command_get_product_handler(message: types.Message, state: FSMContext):
    product_text = message.text or ""
    data = await state.get_data()
    cmd_name = data.get("command_name", ".unknown")
    cmd_response = data.get("command_response", "")
    crypto_amount = data.get("crypto_amount", 0)
    user_id = message.from_user.id
    
    await save_command_db(user_id, cmd_name, cmd_response, True, crypto_amount, product_text)
    
    paid_cmd = ".оплатил_" + cmd_name[1:]
    paid_response = "Проверяю оплату... Ожидайте."
    await save_command_db(user_id, paid_cmd, paid_response, True, crypto_amount, product_text)
    
    await state.clear()
    await setup_command_handler(user_id)
    await safe_send(message.chat.id, em('check') + " <b>Команда с CryptoBot создана!</b>\n<code>" + cmd_name + "</code>\nАвто-создана: <code>" + paid_cmd + "</code>", reply_markup=get_commands_keyboard())

async def setup_command_handler(user_id: int):
    account = get_active_account(user_id)
    if not account: return
    phone, client = account
    
    if user_id in active_auto_reply_handlers:
        try:
            old = active_auto_reply_handlers[user_id].get("cmd_handler")
            if old: client.remove_event_handler(old)
        except: pass
    
    commands_data = await get_commands_db(user_id)
    
    @client.on(events.NewMessage(incoming=True))
    async def command_handler(event):
        if not event.is_private: return
        msg_text = (event.message.text or "").strip().lower()
        if not msg_text: return
        
        if msg_text not in commands_data: return
        
        cmd_data = commands_data[msg_text]
        sender = await event.get_sender()
        user_info = {
            "first_name": getattr(sender, 'first_name', '') or "",
            "last_name": getattr(sender, 'last_name', '') or "",
            "full_name": "",
            "username": getattr(sender, 'username', '') or "",
            "id": str(event.sender_id),
            "phone": getattr(sender, 'phone', '') or "",
        }
        user_info["full_name"] = f"{user_info['first_name']} {user_info['last_name']}".strip()
        
        response = cmd_data['response']
        command_data_extra = {}
        
        if cmd_data.get('has_crypto') and msg_text.startswith('.') and not msg_text.startswith('.оплатил'):
            token = await get_crypto_token(user_id)
            if not token:
                await client.send_message(event.sender_id, "Ошибка: не настроен Crypto Token", parse_mode='html')
                return
            
            amount = cmd_data.get('crypto_amount', 0)
            description = cmd_data.get('product_text', f"Оплата команды {msg_text}")[:100]
            invoice = await create_crypto_invoice(user_id, amount, description)
            
            if invoice:
                command_data_extra['crypto_invoice'] = invoice
                command_data_extra['crypto_amount'] = amount
                await create_invoice_db(user_id, invoice['invoice_id'], msg_text, amount, description, event.sender_id)
            else:
                await client.send_message(event.sender_id, "Ошибка создания счета. Попробуйте позже.", parse_mode='html')
                return
        
        if msg_text.startswith('.оплатил'):
            original_cmd = msg_text.replace('.оплатил_', '.')
            invoice_data = await get_pending_invoice_db(user_id, original_cmd, event.sender_id)
            
            if invoice_data:
                status = await check_crypto_invoice_status(user_id, invoice_data['invoice_id'])
                if status == 'paid':
                    await update_invoice_status_db(invoice_data['invoice_id'], 'paid', event.sender_id)
                    product = invoice_data['product_text'] or "Спасибо за оплату!"
                    await client.send_message(event.sender_id, em('check') + " <b>Оплата получена!</b>\n\n" + product, parse_mode='html')
                    return
                else:
                    await client.send_message(event.sender_id,
                        em('loading') + " <b>Оплата еще не поступила.</b>\nПроверьте оплату и попробуйте снова.\n\n" +
                        em('info') + " Если вы только что оплатили, подождите 1-2 минуты.")
                    return
        
        processed_response = process_placeholders(response, user_info, command_data_extra)
        try:
            await client.send_message(event.sender_id, processed_response, parse_mode='html')
        except Exception as ex:
            logger.error(f"Command reply error: {ex}")
            try:
                clean = re.sub(r'<[^>]+>', '', processed_response)
                await client.send_message(event.sender_id, clean)
            except: pass
    
    if user_id not in active_auto_reply_handlers: active_auto_reply_handlers[user_id] = {}
    active_auto_reply_handlers[user_id]["cmd_handler"] = command_handler
    active_auto_reply_handlers[user_id]["client"] = client
    
    welcome_data = await get_welcome_db(user_id)
    if welcome_data['is_enabled']:
        await setup_welcome_handler(user_id)

async def setup_welcome_handler(user_id: int):
    account = get_active_account(user_id)
    if not account: return
    phone, client = account
    welcomed_users = set()
    welcome_data = await get_welcome_db(user_id)
    
    @client.on(events.NewMessage(incoming=True))
    async def welcome_handler(event):
        if not event.is_private: return
        sender_id = event.sender_id
        if sender_id in welcomed_users: return
        try:
            messages = await client.get_messages(sender_id, limit=2)
            if len(messages) <= 1:
                welcomed_users.add(sender_id)
                if welcome_data['welcome_message']:
                    sender = await event.get_sender()
                    user_info = {
                        "first_name": getattr(sender, 'first_name', '') or "",
                        "last_name": getattr(sender, 'last_name', '') or "",
                        "full_name": "", "username": getattr(sender, 'username', '') or "",
                        "id": str(event.sender_id), "phone": "",
                    }
                    user_info["full_name"] = f"{user_info['first_name']} {user_info['last_name']}".strip()
                    response = process_placeholders(welcome_data['welcome_message'], user_info)
                    try: await client.send_message(sender_id, response, parse_mode='html')
                    except: pass
        except: pass
    
    if user_id not in active_auto_reply_handlers: active_auto_reply_handlers[user_id] = {}
    active_auto_reply_handlers[user_id]["welcome_handler"] = welcome_handler
    active_auto_reply_handlers[user_id]["client"] = client

@dp.callback_query(F.data == "welcome_setup")
async def welcome_setup_handler(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    welcome_data = await get_welcome_db(user_id)
    if welcome_data['is_enabled']:
        await save_welcome_db(user_id, False)
        await callback.answer("Приветствие отключено!", show_alert=True); await commands_menu(callback); return
    await safe_edit(callback.message, em('gift') + " <b>Настройка приветствия</b>\n\nОтправьте приветствие (можно {NICK}, HTML):\nОтвечает на ПЕРВОЕ сообщение", reply_markup=get_back_keyboard("commands_menu"))
    await state.set_state(WelcomeStates.waiting_for_welcome_message)
    await callback.answer()

@dp.message(WelcomeStates.waiting_for_welcome_message)
async def welcome_get_message_handler(message: types.Message, state: FSMContext):
    welcome_msg = message.html_text if message.html_text else message.text or ""
    user_id = message.from_user.id
    await save_welcome_db(user_id, True, welcome_msg)
    await state.clear()
    await setup_welcome_handler(user_id)
    await safe_send(message.chat.id, em('check') + " <b>Приветствие настроено!</b>", reply_markup=get_commands_keyboard())

@dp.callback_query(F.data == "my_commands")
async def my_commands_list_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    commands_data = await get_commands_db(user_id)
    display_cmds = {k: v for k, v in commands_data.items() if not k.startswith('.оплатил_')}
    if not display_cmds:
        builder = InlineKeyboardBuilder(); builder.button(text="Назад", callback_data="commands_menu", style="default", icon_custom_emoji_id=eid("back"))
        await safe_edit(callback.message, em('unlock') + " <b>Нет команд</b>", reply_markup=builder.as_markup()); await callback.answer(); return
    builder = InlineKeyboardBuilder()
    for cmd_name in display_cmds: builder.button(text=cmd_name, callback_data="edit_cmd_" + cmd_name, style="default")
    builder.button(text="Назад", callback_data="commands_menu", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(2)
    await safe_edit(callback.message, em('file') + " <b>Мои команды (" + str(len(display_cmds)) + "):</b>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_cmd_"))
async def edit_command_handler(callback: types.CallbackQuery):
    cmd_name = callback.data.replace("edit_cmd_", "")
    user_id = callback.from_user.id
    commands_data = await get_commands_db(user_id)
    if cmd_name not in commands_data: await callback.answer("Не найдена", show_alert=True); return
    cmd = commands_data[cmd_name]
    builder = InlineKeyboardBuilder()
    builder.button(text="Удалить", callback_data="del_cmd_" + cmd_name, style="danger", icon_custom_emoji_id=eid("trash"))
    builder.button(text="Назад", callback_data="my_commands", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    info = ""
    if cmd.get('has_crypto'): info = em('money') + " Crypto: " + str(cmd.get('crypto_amount', 0)) + "₽\n"
    await safe_edit(callback.message, em('code') + " <b>" + cmd_name + "</b>\n" + info + "<blockquote>" + cmd['response'][:200] + "</blockquote>", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("del_cmd_"))
async def delete_command_handler(callback: types.CallbackQuery):
    cmd_name = callback.data.replace("del_cmd_", "")
    await delete_command_db(callback.from_user.id, cmd_name)
    if cmd_name.startswith('.'):
        paid_cmd = '.оплатил_' + cmd_name[1:]
        await delete_command_db(callback.from_user.id, paid_cmd)
    await callback.answer("Удалена!", show_alert=True); await my_commands_list_handler(callback)

# ============ КОНФИГИ ============

@dp.callback_query(F.data == "save_config")
async def save_config_handler(callback: types.CallbackQuery, state: FSMContext):
    commands_data = await get_commands_db(callback.from_user.id)
    if not commands_data: await callback.answer("Нет команд!", show_alert=True); return
    await safe_edit(callback.message, em('write') + " <b>Название конфига:</b>", reply_markup=get_back_keyboard("commands_menu"))
    await state.set_state(ConfigStates.waiting_for_config_name); await callback.answer()

@dp.message(ConfigStates.waiting_for_config_name)
async def config_get_name_handler(message: types.Message, state: FSMContext):
    config_name = message.text.strip()
    if not config_name: await safe_send(message.chat.id, em('cross') + " Введите название"); return
    user_id = message.from_user.id
    commands_data = await get_commands_db(user_id)
    welcome_data = await get_welcome_db(user_id)
    config = {"commands": commands_data, "welcome": welcome_data}
    await save_config_db(user_id, config_name, config)
    await state.clear()
    await safe_send(message.chat.id, em('check') + " <b>Конфиг '" + config_name + "' сохранен!</b>", reply_markup=get_commands_keyboard())

@dp.callback_query(F.data == "load_config")
async def load_config_handler(callback: types.CallbackQuery):
    configs = await get_configs_db(callback.from_user.id)
    if not configs: await callback.answer("Нет конфигов!", show_alert=True); return
    builder = InlineKeyboardBuilder()
    for cfg_name in configs: builder.button(text=cfg_name, callback_data="ldcfg_" + cfg_name, style="default")
    builder.button(text="Назад", callback_data="commands_menu", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(2)
    await safe_edit(callback.message, em('box') + " <b>Загрузить конфиг:</b>", reply_markup=builder.as_markup()); await callback.answer()

@dp.callback_query(F.data.startswith("ldcfg_"))
async def load_config_confirm_handler(callback: types.CallbackQuery):
    config_name = callback.data.replace("ldcfg_", "")
    configs = await get_configs_db(callback.from_user.id)
    if config_name not in configs: await callback.answer("Не найден!", show_alert=True); return
    cfg = configs[config_name]
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM commands WHERE user_id=$1", user_id)
    for cmd, data in cfg.get("commands", {}).items():
        await save_command_db(user_id, cmd, data['response'], data.get('has_crypto', False), data.get('crypto_amount'), data.get('product_text'))
    welcome = cfg.get("welcome", {})
    await save_welcome_db(user_id, welcome.get('is_enabled', False), welcome.get('welcome_message'))
    await setup_command_handler(user_id)
    await callback.answer("Конфиг '" + config_name + "' загружен!", show_alert=True); await commands_menu(callback)

# ============ РАССЫЛКА ============

@dp.callback_query(F.data == "broadcast")
async def broadcast_menu_handler(callback: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(text="Новая рассылка", callback_data="new_broadcast", style="primary", icon_custom_emoji_id=eid("megaphone"))
    builder.button(text="Активные рассылки", callback_data="active_broadcasts", style="default", icon_custom_emoji_id=eid("clock"))
    builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    await safe_edit(callback.message, em('megaphone') + " <b>Рассылка</b>", reply_markup=builder.as_markup()); await callback.answer()

@dp.callback_query(F.data == "new_broadcast")
async def new_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
    if not get_active_account(callback.from_user.id): await callback.answer("Сначала добавьте аккаунт!", show_alert=True); return
    await state.update_data(messages_list=[])
    await safe_edit(callback.message, em('write') + " <b>Отправьте 1 сообщение для рассылки:</b>\n\n" + em('info') + " HTML и премиум эмодзи\n" + em('file') + " До 5 сообщений", reply_markup=get_back_keyboard("broadcast"))
    await state.set_state(BroadcastStates.adding_messages); await callback.answer()

@dp.message(BroadcastStates.adding_messages)
async def broadcast_add_message_handler(message: types.Message, state: FSMContext):
    msg_html = message.html_text or message.text or ""
    data = await state.get_data(); messages_list = list(data.get("messages_list", []))
    messages_list.append(msg_html); await state.update_data(messages_list=messages_list)
    count = len(messages_list)
    if count >= 5: await safe_send(message.chat.id, em('check') + " <b>5/5 добавлено!</b>", reply_markup=get_message_actions_keyboard())
    else: await safe_send(message.chat.id, em('check') + " <b>Сообщение " + str(count) + "/5 добавлено!</b>", reply_markup=get_message_actions_keyboard())

@dp.callback_query(F.data == "add_msg", BroadcastStates.adding_messages)
async def add_more_messages_handler(callback: types.CallbackQuery, state: FSMContext):
    if len((await state.get_data()).get("messages_list", [])) >= 5: await callback.answer("Лимит 5!", show_alert=True); return
    await safe_edit(callback.message, em('write') + " <b>Отправьте сообщение:</b>", reply_markup=get_back_keyboard("broadcast")); await callback.answer()

@dp.callback_query(F.data == "start_msg_config", BroadcastStates.adding_messages)
async def start_message_config_handler(callback: types.CallbackQuery, state: FSMContext):
    if not (await state.get_data()).get("messages_list"): await callback.answer("Добавьте сообщение!", show_alert=True); return
    await safe_edit(callback.message, em('write') + " <b>Количество в каждый чат:</b>", reply_markup=get_back_keyboard("broadcast"))
    await state.set_state(BroadcastStates.waiting_for_count); await callback.answer()

@dp.message(BroadcastStates.waiting_for_count)
async def broadcast_get_count_handler(message: types.Message, state: FSMContext):
    try: count = int(message.text); 
    except ValueError: await safe_send(message.chat.id, em('cross') + " Введите число"); return
    await state.update_data(message_count=count)
    await safe_send(message.chat.id, em('write') + " <b>Задержка (сек):</b>", reply_markup=get_back_keyboard("broadcast"))
    await state.set_state(BroadcastStates.waiting_for_delay)

@dp.message(BroadcastStates.waiting_for_delay)
async def broadcast_get_delay_handler(message: types.Message, state: FSMContext):
    try: delay = float(message.text); 
    except ValueError: await safe_send(message.chat.id, em('cross') + " Введите число"); return
    await state.update_data(delay=delay, selected_chats=[], current_page=0)
    await state.set_state(BroadcastStates.selecting_chats)
    user_id = message.from_user.id
    status_msg = await safe_send(message.chat.id, em('loading') + " <b>Загружаем чаты...</b>")
    success = await load_dialogs(user_id)
    try: await status_msg.delete()
    except: pass
    phone = get_active_account(user_id)
    if not success or not phone or not dialogs_cache.get(user_id, {}).get(phone[0]):
        await safe_send(message.chat.id, em('cross') + " Не удалось загрузить чаты.", reply_markup=get_functions_keyboard())
        await state.clear(); return
    await delete_user_chat_messages(user_id)
    await create_chat_selection_message(message.chat.id, state, 0)

async def create_chat_selection_message(chat_id: int, state: FSMContext, page: int):
    user_id = chat_id
    phone = get_active_account(user_id)
    if not phone: return
    phone = phone[0]
    chats = dialogs_cache.get(user_id, {}).get(phone, [])
    if not chats: await safe_send(chat_id, em('cross') + " <b>Нет чатов</b>", reply_markup=get_functions_keyboard()); await state.clear(); return
    per_page = 10; total_pages = max(1, (len(chats) + per_page - 1) // per_page)
    page_chats = chats[page * per_page:(page + 1) * per_page]
    data = await state.get_data(); selected = list(data.get("selected_chats", []))
    builder = InlineKeyboardBuilder()
    for chat_id_str, chat_name in page_chats:
        builder.button(text=("✅ " if chat_id_str in selected else "") + chat_name[:35], callback_data="sc_" + chat_id_str + "_" + str(page), style="success" if chat_id_str in selected else "default")
    nav = []
    if page > 0: nav.append(types.InlineKeyboardButton(text="◀ Назад", callback_data="pg_" + str(page - 1)))
    if page < total_pages - 1: nav.append(types.InlineKeyboardButton(text="Вперед ▶", callback_data="pg_" + str(page + 1)))
    if nav: builder.row(*nav)
    if len(selected) >= 1: builder.button(text="🚀 Запустить (" + str(len(selected)) + ")", callback_data="choose_mode", style="primary")
    if len(selected) > 0: builder.button(text="🗑 Сбросить", callback_data="clear_chats", style="danger")
    builder.button(text="Отмена", callback_data="cancel_broadcast", style="default", icon_custom_emoji_id=eid("cross"))
    builder.adjust(1)
    msg = await safe_send(chat_id, em('people') + " <b>Выберите чаты (до 10):</b>\nВыбрано: " + str(len(selected)) + "/10 | Страница " + str(page + 1) + "/" + str(total_pages), reply_markup=builder.as_markup())
    await add_user_chat_message(chat_id, msg.message_id)

@dp.callback_query(F.data.startswith("sc_"))
async def select_chat_handler(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.replace("sc_", "").split("_")
    chat_id_str = (parts[0] + "_" + parts[1]) if len(parts) >= 3 else parts[0]
    page = int(parts[-1])
    data = await state.get_data(); selected = list(data.get("selected_chats", []))
    if chat_id_str in selected: selected.remove(chat_id_str)
    elif len(selected) >= 10: await callback.answer("Максимум 10!", show_alert=True); return
    else: selected.append(chat_id_str)
    await state.update_data(selected_chats=selected)
    user_id = callback.from_user.id; await delete_user_chat_messages(user_id)
    try: await callback.message.delete()
    except: pass
    await create_chat_selection_message(user_id, state, page)

@dp.callback_query(F.data.startswith("pg_"))
async def chats_page_handler(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.replace("pg_", ""))
    user_id = callback.from_user.id; await delete_user_chat_messages(user_id)
    try: await callback.message.delete()
    except: pass
    await create_chat_selection_message(user_id, state, page); await callback.answer()

@dp.callback_query(F.data == "choose_mode")
async def choose_broadcast_mode_handler(callback: types.CallbackQuery, state: FSMContext):
    if not (await state.get_data()).get("selected_chats"): await callback.answer("Выберите чаты!", show_alert=True); return
    await safe_edit(callback.message, em('send') + " <b>Выберите режим:</b>", reply_markup=get_mode_keyboard())
    await state.set_state(BroadcastStates.selecting_mode); await callback.answer()

@dp.callback_query(F.data.startswith("mode_"), BroadcastStates.selecting_mode)
async def set_broadcast_mode_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(mode=callback.data.replace("mode_", ""))
    await start_broadcast_execution(callback, state)

async def start_broadcast_execution(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data(); selected_chats = data.get("selected_chats", [])
    if not selected_chats: return
    user_id = callback.from_user.id; broadcast_id = "bc_" + str(int(datetime.now().timestamp()))
    if user_id not in active_broadcasts: active_broadcasts[user_id] = {}
    account = get_active_account(user_id)
    if not account: return
    phone, client = account
    task = asyncio.create_task(execute_broadcast(user_id, broadcast_id, selected_chats, data.get("messages_list", [""]), data.get("message_count", 1), data.get("delay", 1), client, data.get("mode", "sync")))
    active_broadcasts[user_id][broadcast_id] = task
    await delete_user_chat_messages(user_id)
    builder = InlineKeyboardBuilder()
    builder.button(text="Остановить", callback_data="stop_" + broadcast_id, style="danger", icon_custom_emoji_id=eid("cross"))
    builder.button(text="В меню", callback_data="main_menu", style="default", icon_custom_emoji_id=eid("home"))
    builder.adjust(1)
    mode_text = "Одновременная" if data.get("mode") == "sync" else "Рандомная"
    try: await callback.message.delete()
    except: pass
    await safe_send(user_id, em('send') + " <b>Рассылка запущена!</b>\n" + em('people') + " Чатов: " + str(len(selected_chats)) + "\n" + em('clock') + " Задержка: " + str(data.get('delay', 1)) + "с\n" + em('loading') + " Режим: " + mode_text, reply_markup=builder.as_markup())
    await state.clear(); await callback.answer("Запущена!", show_alert=True)

async def execute_broadcast(user_id, broadcast_id, chats, messages_list, count, delay, client, mode):
    total_messages = len(messages_list)
    for i in range(count):
        if broadcast_id not in active_broadcasts.get(user_id, {}): break
        msg = random.choice(messages_list) if total_messages > 1 else messages_list[0]
        if mode == "sync":
            for chat_id_str in chats:
                if broadcast_id not in active_broadcasts.get(user_id, {}): break
                try: await client.send_message(await client.get_entity(int(chat_id_str.split("_")[1])), msg, parse_mode='html')
                except: pass
            if i < count - 1: await asyncio.sleep(delay)
        else:
            try: await client.send_message(await client.get_entity(int(random.choice(chats).split("_")[1])), msg, parse_mode='html')
            except: pass
            await asyncio.sleep(delay)
    active_broadcasts.get(user_id, {}).pop(broadcast_id, None)

@dp.callback_query(F.data == "active_broadcasts")
async def active_broadcasts_list_handler(callback: types.CallbackQuery):
    broadcasts = active_broadcasts.get(callback.from_user.id, {})
    if not broadcasts: await safe_edit(callback.message, em('unlock') + " <b>Нет активных</b>", reply_markup=get_back_keyboard("broadcast")); await callback.answer(); return
    builder = InlineKeyboardBuilder()
    for bid in broadcasts: builder.button(text="Остановить " + bid[:8], callback_data="stop_" + bid, style="danger", icon_custom_emoji_id=eid("cross"))
    builder.button(text="Назад", callback_data="broadcast", style="default", icon_custom_emoji_id=eid("back"))
    builder.adjust(1)
    await safe_edit(callback.message, em('clock') + " <b>Активные:</b> " + str(len(broadcasts)), reply_markup=builder.as_markup()); await callback.answer()

@dp.callback_query(F.data.startswith("stop_"))
async def stop_broadcast_handler(callback: types.CallbackQuery):
    broadcast_id = callback.data.replace("stop_", ""); user_id = callback.from_user.id
    if user_id in active_broadcasts and broadcast_id in active_broadcasts[user_id]:
        active_broadcasts[user_id][broadcast_id].cancel(); del active_broadcasts[user_id][broadcast_id]
    await active_broadcasts_list_handler(callback)

@dp.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
    await delete_user_chat_messages(callback.from_user.id); await state.clear()
    try: await callback.message.delete()
    except: pass
    await safe_send(callback.message.chat.id, em('cross') + " <b>Отменена</b>", reply_markup=get_functions_keyboard()); await callback.answer()

# ============ АДМИН ПАНЕЛЬ ============

@dp.callback_query(F.data == "admin_stats")
async def admin_stats_handler(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    total_users = await get_users_count_db()
    async with db_pool.acquire() as conn:
        total_accounts = await conn.fetchval("SELECT COUNT(*) FROM accounts WHERE is_active=TRUE")
        total_commands = await conn.fetchval("SELECT COUNT(*) FROM commands")
        total_configs = await conn.fetchval("SELECT COUNT(*) FROM configs")
    active_bcasts = sum(len(b) for b in active_broadcasts.values())
    text = (
        em('stats') + " <b>Статистика</b>\n\n"
        + em('people') + " Пользователей: " + str(total_users) + "\n"
        + em('profile') + " Аккаунтов: " + str(total_accounts) + "\n"
        + em('code') + " Команд: " + str(total_commands) + "\n"
        + em('box') + " Конфигов: " + str(total_configs) + "\n"
        + em('clock') + " Рассылок: " + str(active_bcasts)
    )
    builder = InlineKeyboardBuilder(); builder.button(text="Назад", callback_data="main_menu", style="default", icon_custom_emoji_id=eid("back"))
    await safe_edit(callback.message, text, reply_markup=builder.as_markup()); await callback.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start_handler(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return
    await safe_edit(callback.message, em('megaphone') + " <b>Рассылка всем</b>\n\n" + em('write') + " Отправьте сообщение с HTML", reply_markup=get_back_keyboard("main_menu"))
    await state.set_state(AdminStates.waiting_for_broadcast); await callback.answer()

@dp.message(AdminStates.waiting_for_broadcast)
async def admin_broadcast_get_message_handler(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.update_data(admin_broadcast_message=message.html_text or message.text or "")
    builder = InlineKeyboardBuilder()
    builder.button(text="Отправить", callback_data="confirm_admin_broadcast", style="primary", icon_custom_emoji_id=eid("send"))
    builder.button(text="Отмена", callback_data="cancel_admin_broadcast", style="danger", icon_custom_emoji_id=eid("cross"))
    builder.adjust(2)
    await safe_send(message.chat.id, em('send') + " <b>Подтвердите</b>", reply_markup=builder.as_markup())
    await state.set_state(AdminStates.waiting_for_confirm)

@dp.callback_query(F.data == "confirm_admin_broadcast", AdminStates.waiting_for_confirm)
async def admin_broadcast_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data(); msg = data.get("admin_broadcast_message", "")
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users")
    sent = 0
    for user in users:
        try: await safe_send(user['user_id'], msg); sent += 1
        except: pass
    await safe_edit(callback.message, em('check') + " Отправлено: " + str(sent), reply_markup=get_admin_keyboard())
    await state.clear(); await callback.answer("Готово!", show_alert=True)

@dp.callback_query(F.data == "cancel_admin_broadcast", AdminStates.waiting_for_confirm)
async def admin_broadcast_cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(callback.message, em('cross') + " Отменено", reply_markup=get_admin_keyboard()); await callback.answer()

# ============ MAIN ============

async def main():
    os.makedirs("sessions", exist_ok=True)
    os.makedirs("exports", exist_ok=True)
    os.makedirs("avatars", exist_ok=True)
    
    await init_db()
    
    try: await bot.delete_webhook(drop_pending_updates=True)
    except: pass
    
    logger.info("Бот запущен с PostgreSQL!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
