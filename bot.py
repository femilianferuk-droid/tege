import asyncio
import os
import json
from pathlib import Path
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from pyrogram import Client as PyroClient, filters
from pyrogram.errors import FloodWait, SessionPasswordNeeded, PhoneCodeInvalid
from pyrogram.handlers import MessageHandler
import re

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Не указан BOT_TOKEN в переменных окружения")

ADMIN_ID = 7973988177
API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
DATA_FILE = "accounts.json"

# Создаём папку для сессий
Path("sessions").mkdir(exist_ok=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Хранилище аккаунтов
accounts = {}
pending_accounts = {}

class AddAccount(StatesGroup):
    waiting_phone = State()
    waiting_code = State()
    waiting_password = State()

class FloodSetup(StatesGroup):
    waiting_account = State()
    waiting_command = State()

class StopFlood(StatesGroup):
    waiting_account = State()

def load_accounts():
    global accounts
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
            for phone in data:
                accounts[phone] = {
                    "client": None,
                    "flood_active": False,
                    "flood_task": None
                }
    except FileNotFoundError:
        pass

def save_accounts():
    with open(DATA_FILE, "w") as f:
        json.dump(list(accounts.keys()), f)

load_accounts()

async def start_client(phone):
    if phone not in accounts:
        return False
    
    try:
        session_path = f"sessions/{phone.replace('+', '')}"
        
        client = PyroClient(
            session_path,
            api_id=API_ID,
            api_hash=API_HASH,
            phone_number=phone
        )
        await client.start()
        accounts[phone]["client"] = client
        
        # Добавляем обработчик команд флуда
        async def flood_handler(client, message):
            if message.from_user and message.from_user.is_self:
                return
            
            if not message.outgoing:
                return
            
            if message.text and message.text.startswith(".флуд"):
                await handle_flood_command(client, message, phone)
            elif message.text and message.text == ".стопфлуд":
                await handle_stop_command(client, message, phone)
        
        client.add_handler(MessageHandler(flood_handler))
        
        return True
    except Exception as e:
        print(f"Ошибка запуска {phone}: {e}")
        return False

async def handle_flood_command(client, message, phone):
    try:
        if accounts[phone]["flood_active"]:
            await message.reply(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Флуд уже запущен! Остановите командой .стопфлуд',
                parse_mode=ParseMode.HTML
            )
            return
        
        parts = message.text.split(maxsplit=3)
        
        if len(parts) < 4:
            await message.reply(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Формат: .флуд количество задержка сообщение\nПример: .флуд 100 30 привет всем',
                parse_mode=ParseMode.HTML
            )
            return
        
        count = int(parts[1])
        delay = int(parts[2])
        text = parts[3]
        
        if count <= 0 or delay < 0:
            await message.reply(
                '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Неверные значения',
                parse_mode=ParseMode.HTML
            )
            return
        
        accounts[phone]["flood_active"] = True
        task = asyncio.create_task(flood_worker(client, count, delay, text, phone))
        accounts[phone]["flood_task"] = task
        
        await message.reply(
            f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Флуд запущен!</b>\n\n'
            f'<tg-emoji emoji-id="6039422865189638057">📣</tg-emoji> Сообщение: {text}\n'
            f'<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji> Кругов: {count}\n'
            f'<tg-emoji emoji-id="5983150113483134607">⏰</tg-emoji> Задержка: {delay} сек\n\n'
            f'<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Остановить: .стопфлуд',
            parse_mode=ParseMode.HTML
        )
        
    except ValueError:
        await message.reply(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Количество и задержка должны быть числами',
            parse_mode=ParseMode.HTML
        )

async def handle_stop_command(client, message, phone):
    if not accounts[phone]["flood_active"]:
        await message.reply(
            '<tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji> Флуд не запущен',
            parse_mode=ParseMode.HTML
        )
        return
    
    accounts[phone]["flood_active"] = False
    if accounts[phone]["flood_task"]:
        accounts[phone]["flood_task"].cancel()
    
    await message.reply(
        '<b><tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Флуд остановлен!</b>',
        parse_mode=ParseMode.HTML
    )

async def flood_worker(client, count, delay, message, phone):
    try:
        all_chats = []
        async for dialog in client.get_dialogs():
            all_chats.append(dialog.chat.id)
        
        for i in range(count):
            if not accounts[phone]["flood_active"]:
                return
            
            for chat_id in all_chats:
                if not accounts[phone]["flood_active"]:
                    return
                    
                try:
                    await client.send_message(chat_id, message)
                    
                    if not (i == count - 1 and chat_id == all_chats[-1]):
                        await asyncio.sleep(delay)
                    
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except:
                    await asyncio.sleep(delay)
                    continue
        
        accounts[phone]["flood_active"] = False
        
    except Exception as e:
        accounts[phone]["flood_active"] = False

# Клавиатуры с премиум эмодзи и цветными кнопками
def get_main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Добавить аккаунт",
        callback_data="add_account",
        icon_custom_emoji_id="5870994129244131212"
    ))
    builder.row(InlineKeyboardButton(
        text="Мои аккаунты",
        callback_data="my_accounts",
        icon_custom_emoji_id="5870772616305839506"
    ))
    builder.row(InlineKeyboardButton(
        text="Остановить флуд",
        callback_data="stop_flood_menu",
        style='danger',
        icon_custom_emoji_id="6037249452824072506"
    ))
    return InlineKeyboardMarkup(inline_keyboard=builder.export())

def get_back_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="Назад в меню",
        callback_data="back_to_menu",
        icon_custom_emoji_id="5893057118545646106"
    ))
    return InlineKeyboardMarkup(inline_keyboard=builder.export())

def get_accounts_keyboard():
    builder = InlineKeyboardBuilder()
    for phone in accounts:
        if accounts[phone]["client"]:
            status_emoji = "5891207662678317861"
            style = 'success'
        else:
            status_emoji = "5893192487324880883"
            style = 'default'
        
        flood_text = " 🔄" if accounts[phone]["flood_active"] else ""
        
        builder.row(InlineKeyboardButton(
            text=f"{phone}{flood_text}",
            callback_data=f"account_{phone}",
            icon_custom_emoji_id=status_emoji,
            style=style
        ))
    
    builder.row(InlineKeyboardButton(
        text="Добавить аккаунт",
        callback_data="add_account",
        icon_custom_emoji_id="5870994129244131212"
    ))
    builder.row(InlineKeyboardButton(
        text="Назад в меню",
        callback_data="back_to_menu",
        icon_custom_emoji_id="5893057118545646106"
    ))
    return InlineKeyboardMarkup(inline_keyboard=builder.export())

def get_account_actions_keyboard(phone):
    builder = InlineKeyboardBuilder()
    
    if not accounts[phone]["client"]:
        builder.row(InlineKeyboardButton(
            text="Подключить",
            callback_data=f"connect_{phone}",
            style='success',
            icon_custom_emoji_id="5870633910337015697"
        ))
    else:
        builder.row(InlineKeyboardButton(
            text="Отключить",
            callback_data=f"disconnect_{phone}",
            style='danger',
            icon_custom_emoji_id="5870657884844462243"
        ))
    
    builder.row(InlineKeyboardButton(
        text="Удалить аккаунт",
        callback_data=f"delete_{phone}",
        style='danger',
        icon_custom_emoji_id="5870875489362513438"
    ))
    builder.row(InlineKeyboardButton(
        text="Назад к списку",
        callback_data="my_accounts",
        icon_custom_emoji_id="5893057118545646106"
    ))
    return InlineKeyboardMarkup(inline_keyboard=builder.export())

def get_flooding_accounts_keyboard():
    builder = InlineKeyboardBuilder()
    active_found = False
    for phone in accounts:
        if accounts[phone]["client"] and accounts[phone]["flood_active"]:
            active_found = True
            builder.row(InlineKeyboardButton(
                text=f"⏹ Остановить {phone}",
                callback_data=f"dostop_{phone}",
                style='danger',
                icon_custom_emoji_id="6037249452824072506"
            ))
    
    if not active_found:
        return None
    
    builder.row(InlineKeyboardButton(
        text="Назад в меню",
        callback_data="back_to_menu",
        icon_custom_emoji_id="5893057118545646106"
    ))
    return InlineKeyboardMarkup(inline_keyboard=builder.export())

# Обработчики команд
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    await message.answer(
        '<b><tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji> Бот для управления флуд-аккаунтами</b>\n\n'
        '<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Добавляйте Telegram аккаунты\n'
        '<tg-emoji emoji-id="5770753782874246579">✍</tg-emoji> Запускайте флуд на аккаунте командой <code>.флуд 100 30 текст</code>\n'
        '<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Останавливайте флуд здесь или командой <code>.стопфлуд</code>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await callback.message.edit_text(
        '<b><tg-emoji emoji-id="6030400221232501136">🤖</tg-emoji> Бот для управления флуд-аккаунтами</b>\n\n'
        '<tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Добавляйте Telegram аккаунты\n'
        '<tg-emoji emoji-id="5770753782874246579">✍</tg-emoji> Запускайте флуд командой .флуд\n'
        '<tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Останавливайте здесь или командой .стопфлуд',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# Добавление аккаунта
@dp.callback_query(F.data == "add_account")
async def add_account_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await callback.message.edit_text(
        '<b><tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Добавление аккаунта</b>\n\n'
        'Отправьте номер телефона в формате: <code>+79123456789</code>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AddAccount.waiting_phone)
    await callback.answer()

@dp.message(AddAccount.waiting_phone)
async def process_phone(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    phone = message.text.strip()
    
    if not re.match(r'^\+[0-9]{10,15}$', phone):
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Неверный формат номера. Используйте: +79123456789',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    if phone in accounts:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Этот аккаунт уже добавлен',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    try:
        session_path = f"sessions/{phone.replace('+', '')}"
        
        client = PyroClient(
            session_path,
            api_id=API_ID,
            api_hash=API_HASH,
            phone_number=phone
        )
        await client.connect()
        sent_code = await client.send_code(phone)
        
        pending_accounts[message.from_user.id] = {
            "client": client,
            "phone": phone,
            "phone_code_hash": sent_code.phone_code_hash
        }
        
        await state.set_state(AddAccount.waiting_code)
        await message.answer(
            f'<b><tg-emoji emoji-id="6037397706505195857">👁</tg-emoji> Код подтверждения отправлен на {phone}</b>\n\n'
            'Отправьте код из Telegram:',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        
    except Exception as e:
        await message.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {e}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )

@dp.message(AddAccount.waiting_code)
async def process_code(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.from_user.id not in pending_accounts:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сессия истекла. Начните заново',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    code = message.text.strip()
    data = pending_accounts[message.from_user.id]
    client = data["client"]
    phone = data["phone"]
    
    try:
        await client.sign_in(phone, data["phone_code_hash"], code)
        
        # Добавляем обработчики команд
        async def flood_handler(client, message):
            if message.text and message.text.startswith(".флуд"):
                await handle_flood_command(client, message, phone)
            elif message.text and message.text == ".стопфлуд":
                await handle_stop_command(client, message, phone)
        
        client.add_handler(MessageHandler(flood_handler))
        
        accounts[phone] = {
            "client": client,
            "flood_active": False,
            "flood_task": None
        }
        save_accounts()
        del pending_accounts[message.from_user.id]
        
        await state.clear()
        await message.answer(
            f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Аккаунт {phone} успешно добавлен!</b>\n\n'
            f'<tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji> Теперь можете запускать флуд на этом аккаунте командой:\n'
            f'<code>.флуд 100 30 ваш текст</code>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        
    except SessionPasswordNeeded:
        await state.set_state(AddAccount.waiting_password)
        await message.answer(
            '<b><tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Требуется пароль 2FA</b>\n\n'
            'Введите пароль от облачного пароля:',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        
    except PhoneCodeInvalid:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Неверный код. Попробуйте снова',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        
    except Exception as e:
        await state.clear()
        if message.from_user.id in pending_accounts:
            del pending_accounts[message.from_user.id]
        await message.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {e}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )

@dp.message(AddAccount.waiting_password)
async def process_password(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.from_user.id not in pending_accounts:
        await message.answer(
            '<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Сессия истекла',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    password = message.text.strip()
    data = pending_accounts[message.from_user.id]
    client = data["client"]
    phone = data["phone"]
    
    try:
        await client.check_password(password)
        
        async def flood_handler(client, message):
            if message.text and message.text.startswith(".флуд"):
                await handle_flood_command(client, message, phone)
            elif message.text and message.text == ".стопфлуд":
                await handle_stop_command(client, message, phone)
        
        client.add_handler(MessageHandler(flood_handler))
        
        accounts[phone] = {
            "client": client,
            "flood_active": False,
            "flood_task": None
        }
        save_accounts()
        del pending_accounts[message.from_user.id]
        
        await state.clear()
        await message.answer(
            f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Аккаунт {phone} успешно добавлен!</b>\n\n'
            f'<tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji> Теперь можете запускать флуд командой:\n'
            f'<code>.флуд 100 30 ваш текст</code>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        
    except Exception as e:
        await message.answer(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Ошибка: {e}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )

# Просмотр аккаунтов
@dp.callback_query(F.data == "my_accounts")
async def my_accounts(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    if not accounts:
        await callback.message.edit_text(
            '<b><tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> Нет добавленных аккаунтов</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_accounts_keyboard()
        )
    else:
        text = '<b><tg-emoji emoji-id="5870772616305839506">👥</tg-emoji> Ваши аккаунты:</b>\n'
        for phone, data in accounts.items():
            status = 'Подключен' if data["client"] else 'Отключен'
            flood_text = ' | 🔄 Флудит' if data["flood_active"] else ''
            text += f'\n<tg-emoji emoji-id="5891207662678317861">👤</tg-emoji> <code>{phone}</code> - {status}{flood_text}'
        
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_accounts_keyboard()
        )
    
    await callback.answer()

# Действия с аккаунтом
@dp.callback_query(F.data.startswith("account_"))
async def account_actions(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    phone = callback.data.replace("account_", "")
    
    if phone not in accounts:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    
    status = "подключен" if accounts[phone]["client"] else "отключен"
    flood_status = "флудит" if accounts[phone]["flood_active"] else "не флудит"
    
    await callback.message.edit_text(
        f'<b><tg-emoji emoji-id="5870994129244131212">👤</tg-emoji> Аккаунт <code>{phone}</code></b>\n\n'
        f'<tg-emoji emoji-id="6037397706505195857">👁</tg-emoji> Статус: {status}\n'
        f'<tg-emoji emoji-id="5345906554510012647">🔄</tg-emoji> Флуд: {flood_status}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_actions_keyboard(phone)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("connect_"))
async def connect_account(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    phone = callback.data.replace("connect_", "")
    
    if phone not in accounts:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    
    await callback.answer("Подключаем...")
    
    success = await start_client(phone)
    
    if success:
        await callback.message.edit_text(
            f'<b><tg-emoji emoji-id="5870633910337015697">✅</tg-emoji> Аккаунт {phone} подключен!</b>\n\n'
            f'<tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji> Запускайте флуд командой <code>.флуд 100 30 текст</code> на этом аккаунте',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_actions_keyboard(phone)
        )
    else:
        await callback.message.edit_text(
            f'<tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Не удалось подключить {phone}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_actions_keyboard(phone)
        )

@dp.callback_query(F.data.startswith("disconnect_"))
async def disconnect_account(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    phone = callback.data.replace("disconnect_", "")
    
    if phone not in accounts:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    
    if accounts[phone]["flood_active"]:
        accounts[phone]["flood_active"] = False
        if accounts[phone]["flood_task"]:
            accounts[phone]["flood_task"].cancel()
    
    if accounts[phone]["client"]:
        await accounts[phone]["client"].stop()
        accounts[phone]["client"] = None
    
    await callback.message.edit_text(
        f'<b><tg-emoji emoji-id="5870657884844462243">❌</tg-emoji> Аккаунт {phone} отключен</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_actions_keyboard(phone)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_"))
async def delete_account(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    phone = callback.data.replace("delete_", "")
    
    if phone not in accounts:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    
    if accounts[phone]["flood_active"]:
        accounts[phone]["flood_active"] = False
        if accounts[phone]["flood_task"]:
            accounts[phone]["flood_task"].cancel()
    
    if accounts[phone]["client"]:
        await accounts[phone]["client"].stop()
    
    del accounts[phone]
    save_accounts()
    
    await callback.message.edit_text(
        f'<b><tg-emoji emoji-id="5870875489362513438">🗑</tg-emoji> Аккаунт {phone} удален</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# Остановка флуда
@dp.callback_query(F.data == "stop_flood_menu")
async def stop_flood_menu(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    keyboard = get_flooding_accounts_keyboard()
    
    if not keyboard:
        await callback.message.edit_text(
            '<b><tg-emoji emoji-id="6028435952299413210">ℹ</tg-emoji> Нет активных флудов</b>\n\n'
            '<tg-emoji emoji-id="5770753782874246579">✍</tg-emoji> Запустите флуд на аккаунте командой <code>.флуд 100 30 текст</code>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await callback.answer()
        return
    
    await callback.message.edit_text(
        '<b><tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Выберите аккаунт для остановки флуда:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("dostop_"))
async def stop_flood_account(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    phone = callback.data.replace("dostop_", "")
    
    if phone not in accounts:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return
    
    if not accounts[phone]["flood_active"]:
        await callback.answer("Флуд не активен", show_alert=True)
        return
    
    accounts[phone]["flood_active"] = False
    if accounts[phone]["flood_task"]:
        accounts[phone]["flood_task"].cancel()
    
    await callback.message.edit_text(
        f'<b><tg-emoji emoji-id="6037249452824072506">🔒</tg-emoji> Флуд на {phone} остановлен!</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# Запуск бота
async def main():
    # Подключаем сохранённые аккаунты
    for phone in accounts:
        try:
            await start_client(phone)
            print(f"Подключен аккаунт: {phone}")
        except:
            print(f"Не удалось подключить: {phone}")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
