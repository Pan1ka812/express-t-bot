import asyncio
import html
import json
import logging
import os
import platform
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from brands_data import BRAND_ALIASES, GENERIC_VEHICLE_WORDS

import aiohttp
import asyncpg
import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    Contact,
    FSInputFile,
    InlineKeyboardButton,
    KeyboardButton,
    Message,
    ReplyKeyboardRemove,
    WebAppInfo,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
REVIEWS_CHAT_ID = int(os.getenv("REVIEWS_CHAT_ID", "0"))
SUPPORT_PHONE = os.getenv("SUPPORT_PHONE", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS", "")

BASE_DIR = Path(__file__).resolve().parent
BANNED_USERS_FILE = BASE_DIR / "banned_users.json"
MAP_WEBAPP_URL = os.getenv("MAP_WEBAPP_URL", "https://Pan1ka812.github.io/express-t-map/map.html")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# =========================
# CONSTANTS
# =========================
SERVICE_TYPES = [
    "Евакуатор",
    "Гідравлічна платформа",
    "Кран-маніпулятор",
    "Вантажні перевезення",
]

CAR_TYPES = [
    "Легковий",
    "Джип",
    "Мікроавтобус",
    "Автобус",
    "Вантажний авто",
    "Інше",
]

URGENCY_TYPES = [
    "На зараз",
    "На інший день",
]

PAYER_TYPES = [
    "Готівка",
    "Картка",
    "БН",
]

LOADING_PHONE_OPTIONS = [
    "Цей самий номер",
    "Інший номер",
]

UNLOADING_PHONE_OPTIONS = [
    "Цей самий номер",
    "Номер із завантаження",
    "Інший номер",
]


ADDRESS_HINT_WORDS = {
    "вул", "вулиця", "ул", "улица", "просп", "проспект", "пр", "пров",
    "переулок", "пер", "бульвар", "бул", "площа", "пл", "шосе", "дорога",
    "наб", "набережна", "буд", "дом", "д", "house", "street", "st",
    "avenue", "ave", "road", "rd", "lane", "ln",
    "київ", "киев", "львів", "львов", "одеса", "одесса",
    "дніпро", "днепр", "харків", "харьков",
}


ORDER_STATUS_CREATED = "Створено"
ORDER_STATUS_IN_PROGRESS = "В обробці"
ORDER_STATUS_ACCEPTED = "Прийнято в роботу"
ORDER_STATUS_REJECTED = "Не прийнято"

DECLINE_REASONS = ["Відмова клієнта", "Вже неактуально", "Дорого", "Некоректно"]

MANUAL_PHONE_INPUT_TEXT = "✍️ Ввести інший номер вручну"
PAGE_SIZE = 5

# =========================
# FSM
# =========================
class Form(StatesGroup):
    service_type = State()
    cargo_name = State()
    custom_cargo_description = State()
    car_brand_model = State()
    dimensions = State()
    weight = State()
    urgency_type = State()
    scheduled_date = State()
    scheduled_time = State()
    loading_address = State()
    unloading_address = State()
    client_phone_input_choice = State()
    client_phone = State()
    customer_name = State()
    additional_phones = State()
    loading_phone_choice = State()
    loading_phone = State()
    unloading_phone_choice = State()
    unloading_phone = State()
    payer_type = State()
    payer_details = State()
    comment_choice = State()
    comment = State()
    oversize_support = State()
    confirmation = State()
    history_browse = State()
    photo = State()


# =========================
# DATABASE (asyncpg / PostgreSQL)
# =========================
_db_pool: Optional[asyncpg.Pool] = None


async def init_db():
    global _db_pool
    _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _db_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            telegram_full_name TEXT,
            telegram_username TEXT,
            customer_name TEXT,
            phone TEXT,
            note TEXT DEFAULT '',
            orders_count INTEGER DEFAULT 0,
            created_at TEXT
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            customer_name TEXT,
            service_type TEXT,
            cargo_name TEXT,
            custom_cargo_description TEXT,
            car_brand_model TEXT,
            dimensions TEXT,
            weight TEXT,
            urgency_type TEXT,
            scheduled_date TEXT,
            scheduled_time TEXT,
            loading_address TEXT,
            unloading_address TEXT,
            client_phone TEXT,
            loading_phone TEXT,
            unloading_phone TEXT,
            payer_type TEXT,
            payer_details TEXT,
            comment TEXT,
            support_required TEXT,
            price TEXT,
            status TEXT,
            created_at TEXT
        )
        """)
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL UNIQUE,
            telegram_id BIGINT NOT NULL,
            stars INTEGER NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_telegram_id ON reviews(telegram_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_telegram_id ON orders(telegram_id)")
        await conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS dispatcher_username TEXT")
        await conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS responded_at TEXT")
        await conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS decline_reason TEXT")


async def upsert_user(
    telegram_id: int,
    telegram_full_name: str,
    telegram_username: str,
    customer_name: Optional[str] = None,
    phone: Optional[str] = None,
):
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT customer_name, phone FROM users WHERE telegram_id = $1",
            telegram_id,
        )
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        if row is None:
            await conn.execute("""
            INSERT INTO users (telegram_id, telegram_full_name, telegram_username,
                customer_name, phone, note, orders_count, created_at)
            VALUES ($1,$2,$3,$4,$5,'',0,$6)
            """, telegram_id, telegram_full_name, telegram_username,
                customer_name or "", phone or "", now)
        else:
            current_name = row["customer_name"] or ""
            current_phone = row["phone"] or ""
            await conn.execute("""
            UPDATE users SET telegram_full_name=$1, telegram_username=$2,
                customer_name=$3, phone=$4
            WHERE telegram_id=$5
            """, telegram_full_name, telegram_username,
                customer_name or current_name, phone or current_phone, telegram_id)


async def increment_user_orders_count(telegram_id: int):
    async with _db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET orders_count = COALESCE(orders_count,0)+1 WHERE telegram_id=$1",
            telegram_id,
        )


async def create_order(telegram_id: int, data: dict) -> int:
    async with _db_pool.acquire() as conn:
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        row = await conn.fetchrow("""
        INSERT INTO orders (
            telegram_id,customer_name,service_type,cargo_name,custom_cargo_description,
            car_brand_model,dimensions,weight,urgency_type,scheduled_date,scheduled_time,
            loading_address,unloading_address,client_phone,loading_phone,unloading_phone,
            payer_type,payer_details,comment,support_required,price,status,created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
        RETURNING id
        """,
            telegram_id, data.get("customer_name"), data.get("service_type"),
            data.get("cargo_name"), data.get("custom_cargo_description"), data.get("car_brand_model"),
            data.get("dimensions"), data.get("weight"), data.get("urgency_type"),
            data.get("scheduled_date"), data.get("scheduled_time"), data.get("loading_address"),
            data.get("unloading_address"), data.get("client_phone"), data.get("loading_phone"),
            data.get("unloading_phone"), data.get("payer_type"), data.get("payer_details"),
            data.get("comment"), data.get("support_required"), None, ORDER_STATUS_CREATED, now,
        )
        return row["id"]


async def get_user_profile(telegram_id: int) -> Optional[dict]:
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT telegram_id,telegram_full_name,telegram_username,customer_name,phone,
               note,orders_count,created_at
        FROM users WHERE telegram_id=$1
        """, telegram_id)
    return dict(row) if row else None


async def count_user_orders(telegram_id: int) -> int:
    async with _db_pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT COUNT(*) FROM orders WHERE telegram_id=$1", telegram_id
        )
    return val or 0


async def get_orders_page(telegram_id: int, offset: int, limit: int = 5) -> list[dict]:
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch("""
        SELECT id,telegram_id,customer_name,service_type,cargo_name,custom_cargo_description,
               loading_address,unloading_address,price,status,created_at
        FROM orders WHERE telegram_id=$1 ORDER BY id DESC LIMIT $2 OFFSET $3
        """, telegram_id, limit, offset)
    return [dict(r) for r in rows]


async def get_order_by_id_for_user(order_id: int, telegram_id: int) -> Optional[dict]:
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT id,telegram_id,customer_name,service_type,cargo_name,custom_cargo_description,
               loading_address,unloading_address,price,status,created_at
        FROM orders WHERE id=$1 AND telegram_id=$2
        """, order_id, telegram_id)
    return dict(row) if row else None


async def get_full_order_by_id(order_id: int, telegram_id: int) -> Optional[dict]:
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow("""
        SELECT id,telegram_id,customer_name,service_type,cargo_name,custom_cargo_description,
               car_brand_model,dimensions,weight,urgency_type,scheduled_date,scheduled_time,
               loading_address,unloading_address,client_phone,loading_phone,unloading_phone,
               payer_type,payer_details,comment,support_required,price,status,created_at
        FROM orders WHERE id=$1 AND telegram_id=$2
        """, order_id, telegram_id)
    return dict(row) if row else None


async def update_order_status_and_price(order_id: int, status: Optional[str] = None, price: Optional[str] = None):
    async with _db_pool.acquire() as conn:
        if status is not None and price is not None:
            await conn.execute("UPDATE orders SET status=$1,price=$2 WHERE id=$3", status, price, order_id)
        elif status is not None:
            await conn.execute("UPDATE orders SET status=$1 WHERE id=$2", status, order_id)
        elif price is not None:
            await conn.execute("UPDATE orders SET price=$1 WHERE id=$2", price, order_id)
    asyncio.create_task(update_order_in_sheets(order_id, status=status, price=price))


def format_response_time(sent_str: str, responded_str: str) -> str:
    try:
        fmt = "%d.%m.%Y %H:%M"
        sent = datetime.strptime(sent_str.strip(), fmt)
        responded = datetime.strptime(responded_str.strip(), fmt)
        diff = int((responded - sent).total_seconds())
        if diff < 0:
            return ""
        hours, rem = divmod(diff, 3600)
        minutes = rem // 60
        if hours > 0:
            return f"{hours} год {minutes} хв"
        if minutes > 0:
            return f"{minutes} хв"
        return "< 1 хв"
    except Exception:
        return ""


async def update_order_dispatcher(
    order_id: int,
    status: str,
    dispatcher_username: str,
    responded_at: str,
    decline_reason: Optional[str] = None,
):
    async with _db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET status=$1, dispatcher_username=$2, responded_at=$3, decline_reason=$4 WHERE id=$5",
            status, dispatcher_username, responded_at, decline_reason, order_id,
        )
        row = await conn.fetchrow("SELECT created_at FROM orders WHERE id=$1", order_id)
    sent_str = str(row["created_at"]) if row and row["created_at"] else ""
    response_time = format_response_time(sent_str, responded_at) if sent_str else ""
    asyncio.create_task(update_order_in_sheets(
        order_id,
        status=status,
        dispatcher_username=dispatcher_username,
        responded_at=responded_at,
        decline_reason=decline_reason,
        response_time=response_time,
    ))


async def set_user_note(telegram_id: int, note: str):
    async with _db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET note=$1 WHERE telegram_id=$2", note, telegram_id
        )


# =========================
# GOOGLE SHEETS
# =========================
SHEETS_HEADERS = [
    "№ Замовлення", "Дата", "Ім'я клієнта", "Тел. замовника",
    "Тип послуги", "Вантаж / Авто", "Марка / Модель",
    "Габарити", "Вага", "Терміновість", "Дата перевезення", "Час",
    "Адреса завантаження", "Адреса розвантаження",
    "Тел. завантаження", "Тел. розвантаження",
    "Платник", "Деталі платника", "Коментар",
    "Статус", "Telegram ID", "Username",
    "Час відправки в групу", "Диспетчер", "Час відповіді диспетчера", "Причина відмови", "Час реагування",
]

_sheets_client: Optional[gspread.Client] = None

# chat_id -> last message_id that has inline keyboard (to remove buttons before sending new ones)
_user_last_kb_msg: dict[int, int] = {}


async def _clear_user_kb(chat_id: int) -> None:
    msg_id = _user_last_kb_msg.pop(chat_id, None)
    if msg_id:
        try:
            await bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
        except Exception:
            pass


async def _send_with_kb(chat_id: int, text: str, keyboard, parse_mode: str = "HTML") -> None:
    await _clear_user_kb(chat_id)
    msg = await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=keyboard)
    _user_last_kb_msg[chat_id] = msg.message_id


async def _send_photo_with_kb(chat_id: int, photo, caption: str, keyboard, parse_mode: str = "HTML") -> None:
    await _clear_user_kb(chat_id)
    msg = await bot.send_photo(chat_id, photo, caption=caption, parse_mode=parse_mode, reply_markup=keyboard)
    _user_last_kb_msg[chat_id] = msg.message_id


def _get_sheets_client() -> Optional[gspread.Client]:
    global _sheets_client
    credentials_json = os.getenv("GOOGLE_CREDENTIALS", "")
    if not credentials_json:
        return None
    if _sheets_client is None:
        try:
            creds_dict = json.loads(credentials_json)
            scopes = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            _sheets_client = gspread.authorize(creds)
        except Exception:
            logging.exception("Failed to init Google Sheets client")
    return _sheets_client


def _apply_status_conditional_formatting(spreadsheet, sheet_id: int, status_col: int):
    """Applies green/red/yellow conditional formatting to the Status column."""
    col_letter = chr(ord("A") + status_col)
    rules = [
        # Зелений — Прийнято в роботу
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startColumnIndex": status_col, "endColumnIndex": status_col + 1, "startRowIndex": 1}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": ORDER_STATUS_ACCEPTED}]},
                        "format": {"backgroundColor": {"red": 0.71, "green": 0.89, "blue": 0.71}},
                    },
                },
                "index": 0,
            }
        },
        # Червоний — Не прийнято
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startColumnIndex": status_col, "endColumnIndex": status_col + 1, "startRowIndex": 1}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": ORDER_STATUS_REJECTED}]},
                        "format": {"backgroundColor": {"red": 0.93, "green": 0.60, "blue": 0.60}},
                    },
                },
                "index": 1,
            }
        },
        # Жовтий — Створено
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startColumnIndex": status_col, "endColumnIndex": status_col + 1, "startRowIndex": 1}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": ORDER_STATUS_CREATED}]},
                        "format": {"backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.60}},
                    },
                },
                "index": 2,
            }
        },
        # Помаранчевий — В обробці
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startColumnIndex": status_col, "endColumnIndex": status_col + 1, "startRowIndex": 1}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": ORDER_STATUS_IN_PROGRESS}]},
                        "format": {"backgroundColor": {"red": 1.0, "green": 0.75, "blue": 0.40}},
                    },
                },
                "index": 3,
            }
        },
    ]
    spreadsheet.batch_update({"requests": rules})


def _setup_sheets_sync():
    client = _get_sheets_client()
    if client is None:
        return
    try:
        spreadsheet = client.open_by_key(os.getenv("GOOGLE_SPREADSHEET_ID", ""))
        sheet = spreadsheet.sheet1
        try:
            first_row = sheet.row_values(1)
        except Exception:
            first_row = []
        if not first_row:
            sheet.append_row(SHEETS_HEADERS)
            sheet.format("A1:AB1", {
                "textFormat": {"bold": True, "fontSize": 10},
                "backgroundColor": {"red": 0.26, "green": 0.52, "blue": 0.96},
                "horizontalAlignment": "CENTER",
            })
            sheet.freeze(rows=1)
        # Apply status column formatting (always, idempotent enough for startup)
        status_col = SHEETS_HEADERS.index("Статус")
        try:
            _apply_status_conditional_formatting(spreadsheet, sheet.id, status_col)
        except Exception:
            logging.warning("Could not apply conditional formatting")
    except Exception:
        logging.exception("Failed to setup Google Sheets")


async def setup_sheets():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _setup_sheets_sync)


PRICE_SERVICES = ["Евакуатор", "Гідравлічна платформа", "Кран-маніпулятор", "Вантажні перевезення"]
PRICE_CAR_TYPES = ["Легковий", "Джип", "Мікроавтобус", "Автобус", "Вантажний авто", "Інше"]
PRICE_SHEET_NAME = "Ціни"


def _setup_price_sheet_sync():
    client = _get_sheets_client()
    if client is None:
        return
    try:
        spreadsheet = client.open_by_key(os.getenv("GOOGLE_SPREADSHEET_ID", ""))
        try:
            ws = spreadsheet.worksheet(PRICE_SHEET_NAME)
        except Exception:
            ws = spreadsheet.add_worksheet(title=PRICE_SHEET_NAME, rows=40, cols=10)

        header_row = [""] + PRICE_SERVICES

        existing = ws.get_all_values()
        if existing and existing[0] and existing[0][0] == "Ціна подачі від (грн)":
            return  # already set up

        ws.clear()
        rows = []
        rows.append(["Ціна подачі від (грн)"] + [""] * len(PRICE_SERVICES))
        rows.append(header_row)
        for car in PRICE_CAR_TYPES:
            rows.append([car] + [""] * len(PRICE_SERVICES))
        rows.append([""] * (len(PRICE_SERVICES) + 1))
        rows.append(["Ціна км від (грн/км)"] + [""] * len(PRICE_SERVICES))
        rows.append(header_row)
        for car in PRICE_CAR_TYPES:
            rows.append([car] + [""] * len(PRICE_SERVICES))

        ws.update("A1", rows)

        # Format title rows bold+color
        blue = {"red": 0.26, "green": 0.52, "blue": 0.96}
        light = {"red": 0.85, "green": 0.92, "blue": 1.0}
        n_cols = len(PRICE_SERVICES) + 1
        col_letter = chr(ord("A") + n_cols - 1)

        ws.format(f"A1:{col_letter}1", {"textFormat": {"bold": True}, "backgroundColor": blue})
        ws.format(f"A2:{col_letter}2", {"textFormat": {"bold": True}, "backgroundColor": light})
        row10 = 2 + len(PRICE_CAR_TYPES) + 2
        row11 = row10 + 1
        ws.format(f"A{row10}:{col_letter}{row10}", {"textFormat": {"bold": True}, "backgroundColor": blue})
        ws.format(f"A{row11}:{col_letter}{row11}", {"textFormat": {"bold": True}, "backgroundColor": light})
        ws.freeze(rows=0, cols=1)
    except Exception:
        logging.exception("Failed to setup price sheet")


async def setup_price_sheet():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _setup_price_sheet_sync)


def _read_prices_sync(service_type: str, car_type: str):
    """Returns (dispatch_price, km_price) strings or (None, None)."""
    client = _get_sheets_client()
    if client is None:
        return None, None
    try:
        spreadsheet = client.open_by_key(os.getenv("GOOGLE_SPREADSHEET_ID", ""))
        ws = spreadsheet.worksheet(PRICE_SHEET_NAME)
        all_vals = ws.get_all_values()
        if not all_vals:
            return None, None

        def find_price(title_keyword, service, car):
            # Find title row
            title_row_idx = None
            for i, row in enumerate(all_vals):
                if row and title_keyword in row[0]:
                    title_row_idx = i
                    break
            if title_row_idx is None:
                return None
            header_row = all_vals[title_row_idx + 1] if title_row_idx + 1 < len(all_vals) else []
            try:
                col_idx = header_row.index(service)
            except ValueError:
                return None
            for row in all_vals[title_row_idx + 2:]:
                if row and row[0] == car:
                    val = row[col_idx] if col_idx < len(row) else ""
                    return val.strip() if val.strip() else None
            return None

        dispatch = find_price("Ціна подачі від", service_type, car_type)
        km = find_price("Ціна км від", service_type, car_type)
        return dispatch, km
    except Exception:
        logging.exception("Failed to read prices")
        return None, None


async def read_prices(service_type: str, car_type: str):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _read_prices_sync, service_type, car_type)


def _write_order_to_sheets_sync(order_id: int, user: types.User, data: dict, profile: Optional[dict]):
    client = _get_sheets_client()
    if client is None:
        return
    try:
        sheet = client.open_by_key(os.getenv("GOOGLE_SPREADSHEET_ID", "")).sheet1
        cargo_label = data.get("cargo_name") or ""
        if data.get("cargo_name") == "Інше" and data.get("custom_cargo_description"):
            cargo_label = f"Інше ({data.get('custom_cargo_description')})"
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
        sheet.append_row([
            order_id,
            now_str,
            data.get("customer_name") or user.full_name or "",
            data.get("client_phone") or "",
            data.get("service_type") or "",
            cargo_label,
            data.get("car_brand_model") or "",
            data.get("dimensions") or "",
            data.get("weight") or "",
            data.get("urgency_type") or "",
            data.get("scheduled_date") or "",
            data.get("scheduled_time") or "",
            data.get("loading_address") or "",
            data.get("unloading_address") or "",
            data.get("loading_phone") or "",
            data.get("unloading_phone") or "",
            data.get("payer_type") or "",
            data.get("payer_details") or "",
            data.get("comment") or "",
            ORDER_STATUS_CREATED,
            user.id,
            f"@{user.username}" if user.username else "",
            now_str,  # Час відправки в групу
            "",       # Диспетчер
            "",       # Час відповіді диспетчера
            "",       # Причина відмови
            "",       # Час реагування
        ])
    except Exception:
        logging.exception("Failed to write order to Google Sheets")


def _update_order_in_sheets_sync(
    order_id: int,
    status: Optional[str] = None,
    price: Optional[str] = None,
    dispatcher_username: Optional[str] = None,
    responded_at: Optional[str] = None,
    decline_reason: Optional[str] = None,
    response_time: Optional[str] = None,
):
    client = _get_sheets_client()
    if client is None:
        return
    try:
        sheet = client.open_by_key(os.getenv("GOOGLE_SPREADSHEET_ID", "")).sheet1
        try:
            cell = sheet.find(str(order_id), in_column=1)
        except Exception:
            return
        row = cell.row
        if status is not None:
            sheet.update_cell(row, SHEETS_HEADERS.index("Статус") + 1, status)
        if dispatcher_username is not None:
            sheet.update_cell(row, SHEETS_HEADERS.index("Диспетчер") + 1, dispatcher_username)
        if responded_at is not None:
            sheet.update_cell(row, SHEETS_HEADERS.index("Час відповіді диспетчера") + 1, responded_at)
        if decline_reason is not None:
            sheet.update_cell(row, SHEETS_HEADERS.index("Причина відмови") + 1, decline_reason)
        if response_time is not None:
            sheet.update_cell(row, SHEETS_HEADERS.index("Час реагування") + 1, response_time)
    except Exception:
        logging.exception("Failed to update order in Google Sheets")


async def write_order_to_sheets(order_id: int, user: types.User, data: dict, profile: Optional[dict]):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _write_order_to_sheets_sync, order_id, user, data, profile)


async def update_order_in_sheets(
    order_id: int,
    status: Optional[str] = None,
    price: Optional[str] = None,
    dispatcher_username: Optional[str] = None,
    responded_at: Optional[str] = None,
    decline_reason: Optional[str] = None,
    response_time: Optional[str] = None,
):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        lambda: _update_order_in_sheets_sync(order_id, status, price, dispatcher_username, responded_at, decline_reason, response_time),
    )

# =========================
# BAN STORAGE
# =========================
def _load_banned_from_file() -> set[int]:
    if not BANNED_USERS_FILE.exists():
        return set()
    try:
        data = json.loads(BANNED_USERS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return {int(x) for x in data}
    except Exception:
        pass
    return set()


# Кэш банов в памяти — читаем файл только один раз при старте
_banned_users_cache: set[int] = _load_banned_from_file()


def load_banned_users() -> set[int]:
    return _banned_users_cache


def save_banned_users(banned_users: set[int]) -> None:
    global _banned_users_cache
    _banned_users_cache = banned_users
    BANNED_USERS_FILE.write_text(
        json.dumps(sorted(list(banned_users)), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def is_user_banned(user_id: int) -> bool:
    return user_id in _banned_users_cache


async def deny_if_banned_message(message: Message) -> bool:
    user = message.from_user
    if user and is_user_banned(user.id):
        await message.answer("❌ Доступ до бота обмежено.\nЗверніться до оператора.")
        return True
    return False


async def deny_if_banned_callback(call: CallbackQuery) -> bool:
    user = call.from_user
    if user and is_user_banned(user.id):
        await call.answer("Доступ до бота обмежено.", show_alert=True)
        try:
            await call.message.answer("❌ Доступ до бота обмежено.\nЗверніться до оператора.")
        except Exception:
            pass
        return True
    return False

# =========================
# CHAT HELPERS
# =========================
def is_private_chat_message(message: Message) -> bool:
    return message.chat.type == "private"


def is_private_chat_callback(call: CallbackQuery) -> bool:
    return call.message.chat.type == "private"


def is_admin_chat_message(message: Message) -> bool:
    return message.chat.id == ADMIN_CHAT_ID


async def deny_if_not_private_message(message: Message) -> bool:
    return not is_private_chat_message(message)


async def deny_if_not_private_callback(call: CallbackQuery) -> bool:
    return not is_private_chat_callback(call)
async def replace_callback_message(
    call: CallbackQuery,
    text: str,
    reply_markup=None,
    parse_mode: str | None = "HTML",
):
    try:
        # Если текущее сообщение — фото
        if call.message.photo:
            current_caption = (call.message.caption or "").strip()
            new_text = (text or "").strip()

            # Caption у Telegram ограничен 1024 символами
            if len(new_text) <= 1024:
                if current_caption == new_text:
                    try:
                        await call.message.edit_reply_markup(reply_markup=reply_markup)
                    except Exception:
                        pass
                    return

                await call.message.edit_caption(
                    caption=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                )
                return

            # Если текст слишком длинный для caption — удаляем фото и отправляем обычный текст
            try:
                await call.message.delete()
            except Exception:
                pass

            await call.message.answer(
                text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            return

        # Если текущее сообщение — обычный текст
        current_text = (call.message.text or "").strip()
        new_text = (text or "").strip()

        if current_text == new_text:
            try:
                await call.message.edit_reply_markup(reply_markup=reply_markup)
            except Exception:
                pass
            return

        await call.message.edit_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )

    except Exception as e:
        error_text = str(e).lower()

        if (
            "message is not modified" in error_text
            or "message to edit not found" in error_text
            or "message can't be edited" in error_text
            or "there is no text in the message to edit" in error_text
            or "there is no caption in the message to edit" in error_text
        ):
            return

        logging.exception("replace_callback_message failed")

        try:
            await call.message.answer(
                text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
        except Exception:
            pass
# =========================
# HELPERS / VALIDATORS
# =========================
def safe_text(value: Optional[str], default: str = "-") -> str:
    if value is None:
        return default
    value = str(value).strip()
    if not value:
        return default
    return html.escape(value)


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _digits_only(text: str) -> str:
    return re.sub(r"\D", "", text or "")


def _is_consecutive_sequence(text: str) -> bool:
    if len(text) < 4 or not text.isdigit():
        return False

    inc = True
    dec = True

    for i in range(1, len(text)):
        prev_digit = int(text[i - 1])
        cur_digit = int(text[i])
        if cur_digit - prev_digit != 1:
            inc = False
        if cur_digit - prev_digit != -1:
            dec = False

    return inc or dec


def is_suspicious_ukrainian_phone(normalized_phone: str) -> bool:
    digits = _digits_only(normalized_phone)

    if len(digits) != 12 or not digits.startswith("380"):
        return True

    national = "0" + digits[3:]  # 0XXXXXXXXX
    subscriber = national[1:]    # XXXXXXXXX

    obvious_fake_numbers = {
        "0000000000",
        "0111111111",
        "0123456789",
        "0987654321",
        "0999999999",
    }

    if national in obvious_fake_numbers:
        return True

    if len(set(national)) == 1 or len(set(subscriber)) == 1:
        return True

    digit_counts = {d: national.count(d) for d in set(national)}
    if digit_counts and max(digit_counts.values()) >= 9:
        return True

    if _is_consecutive_sequence(national) or _is_consecutive_sequence(subscriber):
        return True

    repeated_tail_patterns = {
        "123123123",
        "321321321",
        "111111111",
        "222222222",
        "333333333",
        "444444444",
        "555555555",
        "666666666",
        "777777777",
        "888888888",
        "999999999",
        "000000000",
    }
    if subscriber in repeated_tail_patterns:
        return True

    return False


def normalize_phone(phone: str) -> Optional[str]:
    if not phone:
        return None

    raw = phone.strip()
    digits = _digits_only(raw)

    if raw.startswith("+"):
        if not re.fullmatch(r"\+\d{12}", raw.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")):
            raw_digits = _digits_only(raw)
            if len(raw_digits) != 12:
                return None

    normalized = None

    if len(digits) == 10 and digits.startswith("0"):
        normalized = "+38" + digits
    elif len(digits) == 12 and digits.startswith("380"):
        normalized = "+" + digits
    else:
        return None

    if not re.fullmatch(r"\+380\d{9}", normalized):
        return None

    if is_suspicious_ukrainian_phone(normalized):
        return None

    return normalized


def extract_phone_from_contact(contact: Contact) -> Optional[str]:
    return normalize_phone(contact.phone_number or "")


def is_valid_address(address: str) -> bool:
    if not address:
        return False

    text = address.strip()
    lowered = text.lower()

    if len(text) < 8:
        return False

    if re.fullmatch(r"[\W_]+", text):
        return False

    meaningful = re.findall(r"[A-Za-zА-Яа-яІіЇїЄє0-9]", text)
    if len(meaningful) < 6:
        return False

    has_digit = bool(re.search(r"\d", text))
    has_hint_word = any(word in lowered for word in ADDRESS_HINT_WORDS)

    if not has_digit and not has_hint_word:
        return False

    banned_single_words = {
        "машина", "авто", "дом", "улица", "вулиця", "город", "місто",
        "киев", "київ", "днепр", "дніпро", "харьков", "харків"
    }

    if lowered in banned_single_words:
        return False

    parts = re.findall(r"[A-Za-zА-Яа-яІіЇїЄє0-9]+", lowered)
    if len(parts) == 1 and not has_digit:
        return False

    # Відхиляємо якщо немає жодного слова з літер (лише цифри/пробіли)
    has_word = bool(re.search(r"[A-Za-zА-Яа-яІіЇїЄє]{2,}", text))
    if not has_word:
        return False

    return True


def parse_weight(weight_text: str) -> Optional[str]:
    if not weight_text:
        return None

    text = weight_text.strip().lower().replace(",", ".")
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(кг|kg|т|тон|тонн|tonnes?)?", text)
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2)

    if value <= 0:
        return None

    # якщо ввели кг — конвертуємо в тони
    if unit in {"кг", "kg"}:
        value = value / 1000

    # зберігаємо в тонах
    formatted = f"{value:g}"
    return f"{formatted} т"


def normalize_dimensions(dimensions_text: str) -> Optional[str]:
    if not dimensions_text:
        return None

    text = dimensions_text.strip().replace("x", "*").replace("X", "*").replace("х", "*").replace("Х", "*")
    parts = text.split("*")

    if len(parts) != 3:
        return None

    try:
        length = float(parts[0].replace(",", "."))
        width = float(parts[1].replace(",", "."))
        height = float(parts[2].replace(",", "."))
    except ValueError:
        return None

    if length <= 0 or width <= 0 or height <= 0:
        return None

    return f"{length:g}*{width:g}*{height:g}"


def extract_height(dimensions_text: str) -> Optional[float]:
    normalized = normalize_dimensions(dimensions_text)
    if not normalized:
        return None
    return float(normalized.split("*")[2])


def detect_brand_alias(text: str) -> Optional[str]:
    lowered = normalize_spaces(text.lower())
    parts = lowered.split(" ")

    if not parts:
        return None

    first = parts[0]
    first_two = " ".join(parts[:2]) if len(parts) >= 2 else first

    if first_two in BRAND_ALIASES:
        return BRAND_ALIASES[first_two]

    if first in BRAND_ALIASES:
        return BRAND_ALIASES[first]

    return None


def is_valid_car_brand_model(text: str) -> bool:
    if not text:
        return False

    raw = normalize_spaces(text)
    lowered = raw.lower()

    if len(raw) < 2 or len(raw) > 60:
        return False

    if re.fullmatch(r"[\W_]+", raw):
        return False

    if not re.search(r"[A-Za-zА-Яа-яІіЇїЄє]", raw):
        return False

    parts = lowered.split()
    if not parts:
        return False

    first = parts[0]

    banned_words = set(GENERIC_VEHICLE_WORDS) | {
        "привіт",
        "hello",
        "hi",
        "test",
        "тест",
    }

    if lowered in banned_words or first in banned_words:
        return False

    brand = detect_brand_alias(raw)
    if not brand:
        return False

    if len(parts) == 1:
        return True

    if len(parts) >= 2 and " ".join(parts[:2]) in BRAND_ALIASES:
        model_part = " ".join(parts[2:]).strip()
    else:
        model_part = " ".join(parts[1:]).strip()

    if not model_part:
        return True

    return bool(re.search(r"[A-Za-zА-Яа-яІіЇїЄє0-9]", model_part))

def is_valid_customer_name(text: str) -> bool:
    if not text:
        return False

    raw = normalize_spaces(text)

    if len(raw) < 2 or len(raw) > 30:
        return False
    if not re.search(r"[A-Za-zА-Яа-яІіЇїЄє]", raw):
        return False
    if re.fullmatch(r"[\W_0-9]+", raw):
        return False

    return True


def needs_dimensions(service_type: str, cargo_name: str) -> bool:
    if service_type == "Вантажні перевезення":
        return True
    return cargo_name == "Інше"


def build_reply_keyboard(options: list[str], adjust: int = 2):
    kb = ReplyKeyboardBuilder()
    for item in options:
        kb.add(KeyboardButton(text=item))
    kb.adjust(adjust)
    return kb.as_markup(resize_keyboard=True)


async def reverse_geocode(lat: float, lng: float) -> str:
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lng}&format=json&accept-language=uk"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"User-Agent": "TelegramBot/1.0"}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                address = data.get("display_name", "")
                return clean_map_address(address) if address else f"геолокація ({lat:.5f}, {lng:.5f})"
    except Exception:
        return f"геолокація ({lat:.5f}, {lng:.5f})"


def clean_map_address(address: str) -> str:
    address = re.sub(r",?\s*Україна\s*", "", address, flags=re.IGNORECASE)
    address = re.sub(r",?\s*\d{5}\s*", "", address)
    address = re.sub(r"\s*,\s*,", ",", address)
    return address.strip(", ").strip()


def build_address_keyboard(mode: str, show_location: bool = True):
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(
        text="🗺️ Відкрити карту",
        web_app=WebAppInfo(url=f"{MAP_WEBAPP_URL}?mode={mode}"),
    ))
    if show_location:
        kb.add(KeyboardButton(
            text="📍 Моє місцезнаходження",
            request_location=True,
        ))
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


def build_phone_input_keyboard():
    kb = ReplyKeyboardBuilder()
    kb.add(KeyboardButton(text="📱 Надіслати мій номер", request_contact=True))
    kb.add(KeyboardButton(text=MANUAL_PHONE_INPUT_TEXT))
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True, one_time_keyboard=True)


def get_main_edit_keyboard():
    kb = InlineKeyboardBuilder()
    kb.add(InlineKeyboardButton(text="✏️ Редагувати заявку", callback_data="edit_main"))
    return kb.as_markup()


def get_edit_fields_keyboard(data: dict):
    service_type = data.get("service_type", "")
    cargo_name = data.get("cargo_name", "")
    kb = InlineKeyboardBuilder()
    kb.add(InlineKeyboardButton(text="👤 Як звертатися", callback_data="edit_customer_name"))
    # Марка/модель — тільки для авто (не "Інше")
    if service_type in {"Евакуатор", "Гідравлічна платформа", "Кран-маніпулятор"} and cargo_name != "Інше":
        kb.add(InlineKeyboardButton(text="🚗 Марка/модель авто", callback_data="edit_car_brand_model"))
    # Опис вантажу — тільки якщо "Інше"
    if cargo_name == "Інше":
        kb.add(InlineKeyboardButton(text="📦 Опис вантажу", callback_data="edit_custom_cargo_description"))
    # Габарити і вага — тільки якщо питались
    if needs_dimensions(service_type, cargo_name):
        kb.add(InlineKeyboardButton(text="📏 Габарити", callback_data="edit_dimensions"))
        kb.add(InlineKeyboardButton(text="⚖️ Вага", callback_data="edit_weight"))
    kb.add(InlineKeyboardButton(text="⏰ Терміновість", callback_data="edit_urgency_type"))
    kb.add(InlineKeyboardButton(text="📍 Адреса завантаження", callback_data="edit_loading_address"))
    kb.add(InlineKeyboardButton(text="📍 Адреса розвантаження", callback_data="edit_unloading_address"))
    kb.add(InlineKeyboardButton(text="📞 Тел. замовника", callback_data="edit_client_phone"))
    kb.add(InlineKeyboardButton(text="📞 Тел. завантаження", callback_data="edit_loading_phone_choice"))
    kb.add(InlineKeyboardButton(text="📞 Тел. розвантаження", callback_data="edit_unloading_phone_choice"))
    kb.add(InlineKeyboardButton(text="⬅️ Назад до заявки", callback_data="edit_cancel"))
    kb.adjust(2)
    return kb.as_markup()


def get_profile_keyboard():
    kb = InlineKeyboardBuilder()
    kb.add(InlineKeyboardButton(text="📦 Мої замовлення", callback_data="profile_orders"))
    kb.add(InlineKeyboardButton(text="🚀 Нове замовлення", callback_data="profile_new_order"))
    kb.add(InlineKeyboardButton(text="📞 Підтримка", callback_data="profile_support"))
    kb.adjust(2)
    return kb.as_markup()

def build_client_summary(data: dict) -> str:
    cargo_label = safe_text(data.get("cargo_name"))
    if data.get("cargo_name") == "Інше" and data.get("custom_cargo_description"):
        cargo_label = f"Інше ({safe_text(data.get('custom_cargo_description'))})"

    lines = [
        "<b>📋 Ваша заявка на перевезення:</b>",
        "",
        f"<b>👤 Ім'я:</b> {safe_text(data.get('customer_name'))}",
        f"<b>🚛 Тип послуги:</b> {safe_text(data.get('service_type'))}",
        f"<b>📦 Вантаж:</b> {cargo_label}",
    ]

    if data.get("car_brand_model"):
        lines.append(f"<b>🚗 Марка/модель авто:</b> {safe_text(data.get('car_brand_model'))}")

    if data.get("dimensions"):
        lines.append(f"<b>📏 Габарити:</b> {safe_text(data.get('dimensions'))}")

    if data.get("weight"):
        lines.append(f"<b>⚖️ Вага:</b> {safe_text(data.get('weight'))}")

    lines.append(f"<b>⏰ Терміновість:</b> {safe_text(data.get('urgency_type'))}")

    if data.get("urgency_type") == "На інший день":
        lines.append(f"<b>📅 Дата:</b> {safe_text(data.get('scheduled_date'))}")
        lines.append(f"<b>🕐 Час:</b> {safe_text(data.get('scheduled_time'))}")

    lines.extend([
        f"<b>📍 Адреса завантаження:</b> {safe_text(data.get('loading_address'))}",
        f"<b>📍 Адреса розвантаження:</b> {safe_text(data.get('unloading_address'))}",
        f"<b>📞 Тел. замовника:</b> {safe_text(data.get('client_phone'))}",
        f"<b>📞 Тел. завантаження:</b> {safe_text(data.get('loading_phone'))}",
        f"<b>📞 Тел. розвантаження:</b> {safe_text(data.get('unloading_phone'))}",
    ])

    payer_line = f"<b>💰 Платник:</b> {safe_text(data.get('payer_type'))}"
    if data.get("payer_type") == "БН" and data.get("payer_details"):
        payer_line += f" - {safe_text(data.get('payer_details'))}"
    lines.append(payer_line)

    if data.get("comment"):
        lines.append(f"<b>📝 Коментар:</b> {safe_text(data.get('comment'))}")

    photo_ids = data.get("photo_file_ids") or []
    if photo_ids:
        lines.append(f"<b>📷 Фото:</b> {len(photo_ids)} шт.")

    lines.append("")
    lines.append("<b>Підтверджуєте заявку?</b>")

    return "\n".join(lines)


def build_admin_summary(user: types.User, data: dict, order_id: int, profile: Optional[dict] = None) -> str:
    cargo_label = safe_text(data.get("cargo_name"))
    if data.get("cargo_name") == "Інше" and data.get("custom_cargo_description"):
        cargo_label = f"Інше ({safe_text(data.get('custom_cargo_description'))})"

    orders_count = profile["orders_count"] if profile else 0
    note = profile["note"] if profile else ""

    lines = [
        f"<b>🚨 НОВА ЗАЯВКА #{order_id}</b>",
        "",
        f"<b>👤 Клієнт:</b> {safe_text(data.get('customer_name') or user.full_name, 'Не вказано')}",
        f"<b>📞 Тел. замовника:</b> {safe_text(data.get('client_phone'))}",
        f"<b>📞 Тел. завантаження:</b> {safe_text(data.get('loading_phone'))}",
        f"<b>📞 Тел. розвантаження:</b> {safe_text(data.get('unloading_phone'))}",
        f"<b>📦 Вантаж:</b> {cargo_label}",
    ]

    if data.get("car_brand_model"):
        lines.append(f"<b>🚗 Марка/модель авто:</b> {safe_text(data.get('car_brand_model'))}")

    if data.get("dimensions"):
        lines.append(f"<b>📏 Габарити:</b> {safe_text(data.get('dimensions'))}")

    if data.get("weight"):
        lines.append(f"<b>⚖️ Вага:</b> {safe_text(data.get('weight'))}")

    lines.extend([
        f"<b>📍 Адреса завантаження:</b> {safe_text(data.get('loading_address'))}",
        f"<b>📍 Адреса розвантаження:</b> {safe_text(data.get('unloading_address'))}",
    ])

    lines.append(f"<b>⏰ Терміновість:</b> {safe_text(data.get('urgency_type'))}")

    if data.get("urgency_type") == "На інший день":
        lines.append(f"<b>📅 Дата:</b> {safe_text(data.get('scheduled_date'))}")
        lines.append(f"<b>🕐 Час:</b> {safe_text(data.get('scheduled_time'))}")

    payer_line = f"<b>💰 Платник:</b> {safe_text(data.get('payer_type'))}"
    if data.get("payer_type") == "БН" and data.get("payer_details"):
        payer_line += f" - {safe_text(data.get('payer_details'))}"
    lines.append(payer_line)

    lines.extend([
        "",
        f"<b>🚛 Тип послуги:</b> {safe_text(data.get('service_type'))}",
        f"<b>🆔 ID:</b> {user.id}",
        f"<b>📱 Username:</b> @{html.escape(user.username) if user.username else 'немає'}",
        f"<b>🔗 Посилання:</b> <a href='tg://user?id={user.id}'>Написати клієнту</a>",
        f"<b>📦 Замовлень через бота:</b> {orders_count}",
    ])

    if note:
        lines.append(f"<b>📝 Внутрішня примітка:</b> {safe_text(note)}")

    if data.get("support_required") == "потрібен":
        lines.append("<b>🚨 Супровід:</b> потрібен")

    if data.get("comment"):
        lines.append(f"<b>📝 Коментар:</b> {safe_text(data.get('comment'))}")

    loading_lat = data.get("loading_lat")
    loading_lng = data.get("loading_lng")
    unloading_lat = data.get("unloading_lat")
    unloading_lng = data.get("unloading_lng")

    if loading_lat and loading_lng and unloading_lat and unloading_lng:
        route_url = (
            f"https://www.openstreetmap.org/directions"
            f"?from={loading_lat},{loading_lng}&to={unloading_lat},{unloading_lng}"
        )
        lines.append("")
        lines.append(f"🗺️ <a href='{route_url}'>Маршрут на OpenStreetMap</a>")

    return "\n".join(lines)


def build_profile_text(profile: dict) -> str:
    customer_name = profile.get("customer_name") or profile.get("telegram_full_name") or "Клієнт"

    lines = [
        "<b>👤 ПРОФІЛЬ КЛІЄНТА</b>",
        "",
        f"<b>Ім'я:</b> {safe_text(customer_name)}",
        f"<b>📞 Телефон:</b> {safe_text(profile.get('phone'))}",
        f"<b>📦 Замовлень через бота:</b> {profile.get('orders_count', 0)}",
    ]

    return "\n".join(lines)


async def ask_for_client_phone_input(message: Message, edit_mode: bool = False):
    prompt = (
        "Оберіть спосіб введення номера телефону замовника/платника/відповідального:"
        if not edit_mode
        else "Оберіть спосіб введення нового номера телефону замовника:"
    )
    await message.answer(
        f"{prompt}\n\n"
        "• Надішліть свій номер через Telegram\n"
        "• Або введіть інший номер вручну",
        reply_markup=build_phone_input_keyboard(),
    )


async def finalize_client_phone(message: Message, state: FSMContext, phone: str):
    await state.update_data(client_phone=phone)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "Оберіть тип послуги:",
        reply_markup=build_reply_keyboard(SERVICE_TYPES),
    )
    await state.set_state(Form.service_type)

BOT_COMMANDS = [
    types.BotCommand(command="start", description="Головне меню"),
    types.BotCommand(command="order", description="Нове замовлення"),
    types.BotCommand(command="profile", description="Мій профіль"),
    types.BotCommand(command="help", description="Допомога"),
]


async def hide_commands(user_id: int):
    try:
        await bot.set_my_commands([], scope=types.BotCommandScopeChat(chat_id=user_id))
    except Exception:
        pass


async def restore_commands(user_id: int):
    try:
        await bot.set_my_commands(BOT_COMMANDS, scope=types.BotCommandScopeChat(chat_id=user_id))
    except Exception:
        pass


# =========================
# PROFILE RENDER
# =========================
async def send_profile(message: Message, telegram_id: int):
    profile = await get_user_profile(telegram_id)
    if profile is None:
        await message.answer("Профіль поки що порожній. Створіть перше замовлення через /order")
        return

    text = build_profile_text(profile)
    profile_banner = BASE_DIR / "profile_banner.jpg"

    if profile_banner.exists():
        await _send_photo_with_kb(
            message.chat.id,
            FSInputFile(str(profile_banner)),
            caption=text,
            keyboard=get_profile_keyboard(),
        )
    else:
        await _send_with_kb(message.chat.id, text, get_profile_keyboard())


async def send_profile_to_chat(chat_id: int, telegram_id: int):
    profile = await get_user_profile(telegram_id)
    if profile is None:
        await bot.send_message(chat_id, "Профіль поки що порожній. Створіть перше замовлення через /order")
        return

    text = build_profile_text(profile)
    profile_banner = BASE_DIR / "profile_banner.jpg"

    if profile_banner.exists():
        await _send_photo_with_kb(chat_id, FSInputFile(str(profile_banner)), caption=text, keyboard=get_profile_keyboard())
    else:
        await _send_with_kb(chat_id, text, get_profile_keyboard())

# =========================
# COMMANDS
# =========================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    await state.clear()

    user = message.from_user
    if user:
        await upsert_user(
            telegram_id=user.id,
            telegram_full_name=user.full_name or "",
            telegram_username=user.username or "",
        )

    photo_path = BASE_DIR / "welcome.jpg"
    caption_text = (
        "<b>Express T</b> — професійні вантажні та автомобільні перевезення по Україні.\n\n"
        "Наша компанія пропонує:\n"
        "• Вантажні перевезення\n"
        "• Евакуатор\n"
        "• Гідравлічна платформа\n"
        "• Кран-маніпулятор\n\n"
        "✅ Надійність\n"
        "✅ Оперативність\n"
        "✅ Індивідуальний підхід\n\n"
        "ℹ️ Для інструкції натисніть /help\n"
        "👤 Профіль: /profile\n"
        "🚀 Щоб зробити замовлення, натисніть /order"
    )

    try:
        if photo_path.exists():
            await message.answer_photo(
                FSInputFile(str(photo_path)),
                caption=caption_text,
                parse_mode="HTML",
            )
        else:
            await message.answer(caption_text, parse_mode="HTML")
    except Exception:
        await message.answer(caption_text, parse_mode="HTML")



@router.message(Command("help"))
async def help_command(message: Message):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    help_text = (
        "<b>Як зробити замовлення через Express T?</b>\n\n"
        "1️⃣ Натисніть <b>/order</b> — введіть своє ім'я та номер телефону.\n"
        "2️⃣ Оберіть тип послуги: Евакуатор, Гідравлічна платформа, Кран-маніпулятор або Вантажні перевезення.\n"
        "3️⃣ Вкажіть тип авто або вантажу, марку/модель, габарити та вагу (якщо потрібно).\n"
        "4️⃣ Оберіть терміновість — <b>На зараз</b> або <b>На інший день</b> (вкажіть дату і час).\n"
        "5️⃣ Вкажіть адреси завантаження та розвантаження — текстом або через карту.\n"
        "6️⃣ За потреби додайте окремі номери на завантаження/розвантаження.\n"
        "7️⃣ Додайте фото, коментар або пропустіть цей крок.\n"
        "8️⃣ Оберіть спосіб оплати: <b>Готівка</b> або <b>БН</b>.\n"
        "9️⃣ Перевірте заявку — за потреби відредагуйте — та натисніть <b>✅ Підтвердити</b>.\n\n"
        "Після підтвердження заявка надходить до диспетчера. "
        "Він зв'яжеться з вами найближчим часом для уточнення деталей.\n\n"
        "👤 Ваш профіль та історія замовлень: <b>/profile</b>\n\n"
        "Якщо залишились питання — зверніться до підтримки через профіль."
    )
    await message.answer(help_text, parse_mode="HTML")


@router.message(Command("profile"))
async def profile_command(message: Message):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    user = message.from_user
    if user is None:
        return

    await upsert_user(
        telegram_id=user.id,
        telegram_full_name=user.full_name or "",
        telegram_username=user.username or "",
    )
    await send_profile(message, user.id)


@router.message(Command("order"))
async def cmd_order(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    await state.clear()
    await hide_commands(message.from_user.id)
    await message.answer(
        "Як до вас звертатися?\nНаприклад: Андрій",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Form.customer_name)

# =========================
# ADMIN COMMANDS
# =========================
@router.message(Command("ban"))
async def ban_user_command(message: Message):
    if not is_admin_chat_message(message):
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Формат: /ban [ID_клієнта]")
        return

    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("Формат: /ban [ID_клієнта]")
        return

    banned = load_banned_users()
    if user_id in banned:
        await message.answer(f"⚠️ Користувач {user_id} вже заблокований.")
        return

    banned.add(user_id)
    save_banned_users(banned)
    await message.answer(f"🚫 Користувача {user_id} заблоковано.")


@router.message(Command("unban"))
async def unban_user_command(message: Message):
    if not is_admin_chat_message(message):
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Формат: /unban [ID_клієнта]")
        return

    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("Формат: /unban [ID_клієнта]")
        return

    banned = load_banned_users()
    if user_id not in banned:
        await message.answer(f"⚠️ Користувач {user_id} не знайдений у бані.")
        return

    banned.remove(user_id)
    save_banned_users(banned)
    await message.answer(f"✅ Користувача {user_id} розблоковано.")


@router.message(Command("clearall"))
async def clearall_command(message: Message):
    if not is_admin_chat_message(message):
        return
    async with _db_pool.acquire() as conn:
        await conn.execute("DELETE FROM orders")
        await conn.execute("DELETE FROM users")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _setup_sheets_sync)
    await message.answer("✅ Всі замовлення та користувачі видалені. Таблиця очищена.")


@router.message(Command("banlist"))
async def banlist_command(message: Message):
    if not is_admin_chat_message(message):
        return

    banned = sorted(load_banned_users())
    if not banned:
        await message.answer("Список бану порожній.")
        return

    text = "🚫 <b>Заблоковані користувачі:</b>\n\n" + "\n".join(str(uid) for uid in banned)
    await message.answer(text, parse_mode="HTML")



@router.message(Command("setnote"))
async def setnote_command(message: Message):
    if not is_admin_chat_message(message):
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Формат: /setnote [ID_клієнта] [примітка]")
        return

    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("Формат: /setnote [ID_клієнта] [примітка]")
        return

    note = parts[2].strip()
    await set_user_note(user_id, note)
    await message.answer(f"📝 Клієнту {user_id} оновлено примітку.")

# =========================
# ORDER FLOW
# =========================
@router.message(Form.service_type)
async def process_service_type(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""
    if text not in SERVICE_TYPES:
        await message.answer("Будь ласка, оберіть тип послуги, використовуючи кнопки.")
        return

    await state.update_data(service_type=text)

    if text in {"Евакуатор", "Гідравлічна платформа", "Кран-маніпулятор"}:
        await message.answer("Оберіть тип авто:", reply_markup=build_reply_keyboard(CAR_TYPES))
    else:
        await message.answer(
            "Вкажіть, будь ласка, назву вантажу:",
            reply_markup=ReplyKeyboardRemove(),
        )

    await state.set_state(Form.cargo_name)


@router.message(Form.cargo_name)
async def process_cargo_name(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()
    data = await state.get_data()
    service_type = data.get("service_type")

    if service_type in {"Евакуатор", "Гідравлічна платформа", "Кран-маніпулятор"}:
        if text not in CAR_TYPES:
            await message.answer("Будь ласка, оберіть тип авто, використовуючи кнопки.")
            return

        await state.update_data(cargo_name=text)

        if text == "Інше":
            await state.update_data(car_brand_model=None)
            await message.answer(
                "Вкажіть, будь ласка, що саме потрібно перевезти.\n"
                "Наприклад: кіоск, МАФ, генератор, навантажувач.",
                reply_markup=ReplyKeyboardRemove(),
            )
            await state.set_state(Form.custom_cargo_description)
            return

        await message.answer(
            "Вкажіть марку та модель авто.\n"
            "Наприклад: Toyota Camry, Mercedes Sprinter, Dodge RAM.",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.car_brand_model)
        return

    if len(text) < 2:
        await message.answer("Будь ласка, введіть коректну назву вантажу.")
        return

    await state.update_data(cargo_name=text)
    await message.answer(
        "Вкажіть габаритні розміри (Д*Ш*В, м):",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Form.dimensions)


@router.message(Form.custom_cargo_description)
async def process_custom_cargo_description(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer(
            "Будь ласка, коротко вкажіть, що саме потрібно перевезти.\n"
            "Наприклад: кіоск, МАФ, генератор, навантажувач."
        )
        return

    await state.update_data(custom_cargo_description=text)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "Вкажіть габаритні розміри (Д*Ш*В, м):",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Form.dimensions)


@router.message(Form.car_brand_model)
async def process_car_brand_model(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("Будь ласка, вкажіть марку та модель авто.")
        return

    await state.update_data(car_brand_model=text)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    if needs_dimensions(data.get("service_type"), data.get("cargo_name")):
        await message.answer(
            "Вкажіть габаритні розміри (Д*Ш*В, м):",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.dimensions)
        return

    await message.answer(
        "На коли потрібно перевезення?",
        reply_markup=build_reply_keyboard(URGENCY_TYPES),
    )
    await state.set_state(Form.urgency_type)


@router.message(Form.dimensions)
async def process_dimensions(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()
    normalized = normalize_dimensions(text)

    if not normalized:
        await message.answer(
            "Введіть габарити у форматі: Д*Ш*В (наприклад, 4.2*2.1*1.8)."
        )
        return

    await state.update_data(dimensions=normalized)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    height = extract_height(normalized)
    if height is not None and height > 3:
        await message.answer(
            "🚨 <b>Негабарит!</b> Висота вантажу більше 3 м. Для такого вантажу потрібен супровід. Чи забезпечуєте супровід?",
            reply_markup=build_reply_keyboard(["Супроводжуємо", "Не супроводжуємо"]),
            parse_mode="HTML",
        )
        await state.set_state(Form.oversize_support)
        return

    await message.answer("Вкажіть вагу (т):", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Form.weight)


@router.message(Form.oversize_support)
async def process_oversize_support(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""

    if text == "Супроводжуємо":
        await state.update_data(support_required="потрібен")
        await message.answer("Вкажіть вагу (т):", reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.weight)
        return

    if text == "Не супроводжуємо":
        await restore_commands(message.from_user.id)
        await message.answer(
            "❌ На жаль, ми не можемо виконати перевезення без супроводу для негабаритного вантажу.\n\n"
            "Якщо бажаєте оформити нову заявку — натисніть /order",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.clear()
        return

    await message.answer("Будь ласка, використовуйте кнопки.")


@router.message(Form.weight)
async def process_weight(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()
    weight = parse_weight(text)
    if weight is None:
        await message.answer("Будь ласка, введіть коректну вагу. Наприклад: 5 або 1.5 т")
        return

    await state.update_data(weight=weight)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "На коли потрібно перевезення?",
        reply_markup=build_reply_keyboard(URGENCY_TYPES),
    )
    await state.set_state(Form.urgency_type)


@router.message(Form.urgency_type)
async def process_urgency_type(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""
    if text not in URGENCY_TYPES:
        await message.answer("Будь ласка, оберіть терміновість, використовуючи кнопки.")
        return

    await state.update_data(urgency_type=text)
    data = await state.get_data()

    if data.get("edit_mode"):
        if text == "На інший день":
            await message.answer(
                "Вкажіть нову дату (формат: ДД.ММ.РРРР):",
                reply_markup=ReplyKeyboardRemove(),
            )
            await state.set_state(Form.scheduled_date)
            return

        await state.update_data(
            scheduled_date="На зараз",
            scheduled_time="На зараз",
            edit_mode=False,
        )
        await show_confirmation(message, state)
        return

    if text == "На інший день":
        await message.answer(
            "Вкажіть дату (формат: ДД.ММ.РРРР):",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.scheduled_date)
        return

    await state.update_data(scheduled_date="На зараз", scheduled_time="На зараз")
    await message.answer(
        "🗺️ Вкажіть адресу завантаження.\nВідкрийте карту або введіть адресу текстом.\nНаприклад: Київ, вул. Бориспільська, 12",
        reply_markup=build_address_keyboard("loading"),
    )
    await state.set_state(Form.loading_address)


@router.message(Form.scheduled_date)
async def process_scheduled_date(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()

    try:
        date_obj = datetime.strptime(text, "%d.%m.%Y")
        if date_obj.date() < datetime.now().date():
            await message.answer("Дата не може бути в минулому. Введіть коректну дату (ДД.ММ.РРРР).")
            return
    except ValueError:
        await message.answer("Невірний формат дати. Введіть у форматі ДД.ММ.РРРР.")
        return

    await state.update_data(scheduled_date=text)
    await message.answer("Вкажіть час (формат: ГГ:ХХ):")
    await state.set_state(Form.scheduled_time)


@router.message(Form.scheduled_time)
async def process_scheduled_time(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = re.sub(r"[.·\-]", ":", (message.text or "").strip())

    if not re.fullmatch(r"\d{1,2}:\d{2}", text):
        await message.answer("Невірний формат часу. Введіть у форматі ГГ:ХХ (наприклад, 14:30).")
        return

    try:
        parsed = datetime.strptime(text.zfill(5), "%H:%M")
        text = parsed.strftime("%H:%M")
    except ValueError:
        await message.answer("Невірний час. Введіть у форматі ГГ:ХХ (наприклад, 14:30).")
        return

    await state.update_data(scheduled_time=text)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "🗺️ Вкажіть адресу завантаження.\nВідкрийте карту або введіть адресу текстом.\nНаприклад: Київ, вул. Бориспільська, 12",
        reply_markup=build_address_keyboard("loading"),
    )
    await state.set_state(Form.loading_address)


@router.message(Form.loading_address)
async def process_loading_address(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    # Данные с карты
    if message.web_app_data is not None:
        try:
            payload = json.loads(message.web_app_data.data)
            address = clean_map_address(payload.get("address") or "")
            lat = payload.get("lat")
            lng = payload.get("lng")
        except Exception:
            await message.answer("Помилка даних з карти. Спробуйте ще раз.")
            return

        await state.update_data(loading_address=address, loading_lat=lat, loading_lng=lng)
        data = await state.get_data()

        if data.get("edit_mode"):
            await state.update_data(edit_mode=False)
            await show_confirmation(message, state)
            return

        await message.answer(
            f"✅ Адресу завантаження збережено:\n<b>{html.escape(address)}</b>\n\n"
            "🗺️ Тепер вкажіть адресу розвантаження.\nВідкрийте карту або введіть адресу текстом.",
            reply_markup=build_address_keyboard("unloading"),
            parse_mode="HTML",
        )
        await state.set_state(Form.unloading_address)
        return

    # Геолокація
    if message.location:
        lat = message.location.latitude
        lng = message.location.longitude
        address = await reverse_geocode(lat, lng)
        await state.update_data(loading_address=address, loading_lat=lat, loading_lng=lng)
        data = await state.get_data()
        if data.get("edit_mode"):
            await state.update_data(edit_mode=False)
            await show_confirmation(message, state)
            return
        await message.answer(
            f"✅ Адресу завантаження збережено:\n<b>{html.escape(address)}</b>\n\n"
            "🗺️ Тепер вкажіть адресу розвантаження.\nВідкрийте карту або введіть адресу текстом.",
            reply_markup=build_address_keyboard("unloading", show_location=False),
            parse_mode="HTML",
        )
        await state.set_state(Form.unloading_address)
        return

    # Ручний ввід тексту
    text = (message.text or "").strip()
    if not is_valid_address(text):
        await message.answer(
            "Будь ласка, введіть повну адресу завантаження.\n"
            "Наприклад: Київ, вул. Бориспільська, 12\n\n"
            "Або скористайтеся картою чи геолокацією:",
            reply_markup=build_address_keyboard("loading"),
        )
        return

    await state.update_data(loading_address=text, loading_lat=None, loading_lng=None)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "🗺️ Вкажіть адресу розвантаження.\nВідкрийте карту або введіть адресу текстом.\nНаприклад: Київ, вул. Січових Стрільців, 18",
        reply_markup=build_address_keyboard("unloading"),
    )
    await state.set_state(Form.unloading_address)


@router.message(Form.unloading_address)
async def process_unloading_address(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    # Данные с карты
    if message.web_app_data is not None:
        try:
            payload = json.loads(message.web_app_data.data)
            address = clean_map_address(payload.get("address") or "")
            lat = payload.get("lat")
            lng = payload.get("lng")
        except Exception:
            await message.answer("Помилка даних з карти. Спробуйте ще раз.")
            return

        await state.update_data(unloading_address=address, unloading_lat=lat, unloading_lng=lng)
        data = await state.get_data()

        if data.get("edit_mode"):
            await state.update_data(edit_mode=False)
            await show_confirmation(message, state)
            return

        await message.answer(
            "Чи потрібні додаткові номери на завантаження або вивантаження?",
            reply_markup=build_reply_keyboard(["✅ Так", "❌ Ні"]),
        )
        await state.set_state(Form.additional_phones)
        return

    # Геолокація
    if message.location:
        lat = message.location.latitude
        lng = message.location.longitude
        address = await reverse_geocode(lat, lng)
        await state.update_data(unloading_address=address, unloading_lat=lat, unloading_lng=lng)
        data = await state.get_data()
        if data.get("edit_mode"):
            await state.update_data(edit_mode=False)
            await show_confirmation(message, state)
            return
        await message.answer(
            "Чи потрібні додаткові номери на завантаження або вивантаження?",
            reply_markup=build_reply_keyboard(["✅ Так", "❌ Ні"]),
        )
        await state.set_state(Form.additional_phones)
        return

    # Ручний ввід тексту
    text = (message.text or "").strip()
    if not is_valid_address(text):
        await message.answer(
            "Будь ласка, введіть повну адресу розвантаження.\n"
            "Наприклад: Київ, вул. Січових Стрільців, 18\n\n"
            "Або скористайтеся картою чи геолокацією:",
            reply_markup=build_address_keyboard("unloading"),
        )
        return

    await state.update_data(unloading_address=text, unloading_lat=None, unloading_lng=None)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "Чи потрібні додаткові номери на завантаження або вивантаження?",
        reply_markup=build_reply_keyboard(["✅ Так", "❌ Ні"]),
    )
    await state.set_state(Form.additional_phones)


@router.message(Form.client_phone_input_choice)
async def process_client_phone_input_choice(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    if message.contact:
        contact = message.contact
        user = message.from_user

        if user is None:
            await message.answer("Не вдалося визначити користувача. Спробуйте ще раз.")
            return

        if contact.user_id and contact.user_id != user.id:
            await message.answer(
                "Будь ласка, надішліть саме свій номер через Telegram або введіть інший номер вручну.",
                reply_markup=build_phone_input_keyboard(),
            )
            return

        phone = extract_phone_from_contact(contact)
        if not phone:
            await message.answer(
                "Не вдалося прийняти цей номер. Перевірте номер або введіть його вручну у форматі 0XXXXXXXXX, 380XXXXXXXXX або +380XXXXXXXXX.",
                reply_markup=build_phone_input_keyboard(),
            )
            return

        await finalize_client_phone(message, state, phone)
        return

    text = (message.text or "").strip()

    if text == MANUAL_PHONE_INPUT_TEXT:
        await message.answer(
            "Введіть номер телефону у форматі 0XXXXXXXXX, 380XXXXXXXXX або +380XXXXXXXXX.",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.client_phone)
        return

    await message.answer(
        "Будь ласка, оберіть один зі способів: надішліть свій номер через Telegram або введіть номер вручну.",
        reply_markup=build_phone_input_keyboard(),
    )


@router.message(Form.client_phone)
async def process_client_phone(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    phone = normalize_phone(message.text or "")
    if not phone:
        await message.answer(
            "Будь ласка, введіть коректний український номер телефону.\n"
            "Підійдуть формати: 0XXXXXXXXX, 380XXXXXXXXX або +380XXXXXXXXX.\n"
            "Номер не повинен бути очевидно фейковим."
        )
        return

    await finalize_client_phone(message, state, phone)


@router.message(Form.customer_name)
async def process_customer_name(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()
    if not is_valid_customer_name(text):
        await message.answer(
            "Будь ласка, вкажіть ім'я або ім'я звернення.\n"
            "Наприклад: Андрій"
        )
        return

    await state.update_data(customer_name=text)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await ask_for_client_phone_input(message)
    await state.set_state(Form.client_phone_input_choice)


@router.message(Form.additional_phones)
async def process_additional_phones(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""
    data = await state.get_data()
    client_phone = data.get("client_phone")

    if text == "❌ Ні":
        await state.update_data(loading_phone=client_phone, unloading_phone=client_phone)
        await message.answer(
            "Бажаєте додати додаткову інформацію до замовлення?",
            reply_markup=build_reply_keyboard(["📷 Фото", "💬 Коментар", "⏭️ Пропустити"]),
        )
        await state.set_state(Form.comment_choice)
        return

    if text == "✅ Так":
        await message.answer(
            "Хто буде на завантаженні?",
            reply_markup=build_reply_keyboard(LOADING_PHONE_OPTIONS),
        )
        await state.set_state(Form.loading_phone_choice)
        return

    await message.answer("Будь ласка, використовуйте кнопки.", reply_markup=build_reply_keyboard(["✅ Так", "❌ Ні"]))


@router.message(Form.loading_phone_choice)
async def process_loading_phone_choice(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""
    data = await state.get_data()
    client_phone = data.get("client_phone")

    if text == "Цей самий номер":
        await state.update_data(loading_phone=client_phone)
        if data.get("edit_mode"):
            await state.update_data(edit_mode=False)
            await show_confirmation(message, state)
            return

        await message.answer(
            "Хто буде на розвантаженні?",
            reply_markup=build_reply_keyboard(UNLOADING_PHONE_OPTIONS),
        )
        await state.set_state(Form.unloading_phone_choice)
        return

    if text == "Інший номер":
        await message.answer(
            "Вкажіть номер телефону відповідального за завантаження:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.loading_phone)
        return

    await message.answer("Будь ласка, використовуйте кнопки.")


@router.message(Form.loading_phone)
async def process_loading_phone(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    phone = normalize_phone(message.text or "")
    if not phone:
        await message.answer(
            "Будь ласка, введіть коректний український номер телефону у форматі 0XXXXXXXXX, 380XXXXXXXXX або +380XXXXXXXXX."
        )
        return

    await state.update_data(loading_phone=phone)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "Хто буде на розвантаженні?",
        reply_markup=build_reply_keyboard(UNLOADING_PHONE_OPTIONS),
    )
    await state.set_state(Form.unloading_phone_choice)


@router.message(Form.unloading_phone_choice)
async def process_unloading_phone_choice(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""
    data = await state.get_data()
    client_phone = data.get("client_phone")
    loading_phone = data.get("loading_phone")

    if text == "Цей самий номер":
        await state.update_data(unloading_phone=client_phone)
        if data.get("edit_mode"):
            await state.update_data(edit_mode=False)
            await show_confirmation(message, state)
            return

        await message.answer(
            "Бажаєте додати додаткову інформацію до замовлення?",
            reply_markup=build_reply_keyboard(["📷 Фото", "💬 Коментар", "⏭️ Пропустити"]),
        )
        await state.set_state(Form.comment_choice)
        return

    if text == "Номер із завантаження":
        await state.update_data(unloading_phone=loading_phone)
        if data.get("edit_mode"):
            await state.update_data(edit_mode=False)
            await show_confirmation(message, state)
            return

        await message.answer(
            "Бажаєте додати додаткову інформацію до замовлення?",
            reply_markup=build_reply_keyboard(["📷 Фото", "💬 Коментар", "⏭️ Пропустити"]),
        )
        await state.set_state(Form.comment_choice)
        return

    if text == "Інший номер":
        await message.answer(
            "Вкажіть номер телефону відповідального за розвантаження:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.unloading_phone)
        return

    await message.answer("Будь ласка, використовуйте кнопки.")


@router.message(Form.unloading_phone)
async def process_unloading_phone(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    phone = normalize_phone(message.text or "")
    if not phone:
        await message.answer(
            "Будь ласка, введіть коректний український номер телефону у форматі 0XXXXXXXXX, 380XXXXXXXXX або +380XXXXXXXXX."
        )
        return

    await state.update_data(unloading_phone=phone)
    data = await state.get_data()

    if data.get("edit_mode"):
        await state.update_data(edit_mode=False)
        await show_confirmation(message, state)
        return

    await message.answer(
        "Бажаєте додати додаткову інформацію до замовлення?",
        reply_markup=build_reply_keyboard(["📷 Фото", "💬 Коментар", "⏭️ Пропустити"]),
    )
    await state.set_state(Form.comment_choice)


async def ask_for_payer_with_price(message: Message, state: FSMContext):
    data = await state.get_data()
    service_type = data.get("service_type", "")
    car_type = data.get("cargo_name", "")

    dispatch_price, km_price = await read_prices(service_type, car_type)

    if dispatch_price or km_price:
        parts = [f"💰 <b>Орієнтовні ціни для вашого замовлення:</b>"]
        if dispatch_price:
            parts.append(f"• Подача: від <b>{dispatch_price} грн</b>")
        if km_price:
            parts.append(f"• Ціна км: від <b>{km_price} грн/км</b>")
        parts.append("")
        await message.answer("\n".join(parts), parse_mode="HTML")

    await message.answer(
        "Оберіть спосіб оплати:",
        reply_markup=build_reply_keyboard(PAYER_TYPES),
    )
    await state.set_state(Form.payer_type)


@router.message(Form.payer_type)
async def process_payer_type(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""

    if text in {"Готівка", "Картка"}:
        await state.update_data(payer_type=text, payer_details="-")
        await show_confirmation(message, state)
        return

    if text == "БН":
        await state.update_data(payer_type="БН")
        await message.answer(
            "Вкажіть назву платника та код ЄДРПОУ або ІНН через кому.\n"
            "Приклад: ТОВ Компанія, 12345678",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.payer_details)
        return

    await message.answer("Будь ласка, оберіть тип платника, використовуючи кнопки.")


@router.message(Form.payer_details)
async def process_payer_details(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Будь ласка, введіть коректні дані платника.")
        return

    digit_sequences = re.findall(r"\d+", text)
    valid_code = any(len(d) in (8, 10) for d in digit_sequences)
    if not valid_code:
        await message.answer(
            "❌ Не знайдено коректний код ЄДРПОУ або ІНН.\n\n"
            "• ЄДРПОУ — 8 цифр (для юридичних осіб)\n"
            "• ІНН — 11 цифр (для фізичних осіб)\n\n"
            "Спробуйте ще раз. Приклад: ТОВ Компанія, 12345678"
        )
        return

    await state.update_data(payer_details=text)
    await show_confirmation(message, state)


@router.message(Form.comment_choice)
async def process_comment_choice(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""

    if text == "⏭️ Пропустити":
        await state.update_data(comment="", photo_file_ids=[])
        await ask_for_payer_with_price(message, state)
        return

    if text == "💬 Коментар":
        await message.answer(
            "Введіть ваш коментар до замовлення:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.comment)
        return

    if text == "📷 Фото":
        await message.answer(
            "Надішліть фото до замовлення:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(Form.photo)
        return

    await message.answer(
        "Будь ласка, використовуйте кнопки.",
        reply_markup=build_reply_keyboard(["📷 Фото", "💬 Коментар", "⏭️ Пропустити"]),
    )


@router.message(Form.photo)
async def process_photo(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""

    if text == "✅ Готово":
        data = await state.get_data()
        if not data.get("photo_file_ids"):
            await message.answer("Ви ще не надіслали жодного фото. Надішліть фото або натисніть «Пропустити».")
            return
        await ask_for_payer_with_price(message, state)
        return

    if text == "🗑 Очистити фото":
        await state.update_data(photo_file_ids=[])
        await message.answer(
            "Фото видалено. Надішліть нове фото.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    if not message.photo:
        await message.answer(
            "Будь ласка, надішліть фото.",
            reply_markup=build_reply_keyboard(["✅ Готово"], adjust=1),
        )
        return

    data = await state.get_data()
    photo_ids: list = list(data.get("photo_file_ids") or [])

    if len(photo_ids) >= 4:
        await message.answer("Максимум 4 фото. Натисніть «✅ Готово» щоб продовжити.")
        return

    photo_ids.append(message.photo[-1].file_id)
    await state.update_data(photo_file_ids=photo_ids, comment="")

    if len(photo_ids) >= 4:
        await message.answer("Досягнуто максимум 4 фото.")
        await ask_for_payer_with_price(message, state)
    else:
        await message.answer(
            f"📷 Фото {len(photo_ids)}/4 додано. Можете надіслати ще або завершити.",
            reply_markup=build_reply_keyboard(["✅ Готово", "🗑 Очистити фото"], adjust=2),
        )


@router.message(Form.comment)
async def process_comment(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = (message.text or "").strip()

    if len(text) > 300:
        await message.answer(
            f"❌ Коментар занадто довгий ({len(text)} символів).\n"
            "Будь ласка, вкажіть коротко — до 300 символів."
        )
        return

    await state.update_data(comment=text)
    await ask_for_payer_with_price(message, state)

# =========================
# EDIT FLOW
# =========================
@router.callback_query(lambda c: c.data == "edit_main")
async def edit_main_callback(call: CallbackQuery, state: FSMContext):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    await call.answer()
    data = await state.get_data()
    edit_msg = await call.message.answer("Що бажаєте змінити?", reply_markup=get_edit_fields_keyboard(data))
    await state.update_data(edit_menu_msg_id=edit_msg.message_id)


@router.callback_query(lambda c: c.data == "edit_cancel")
async def edit_cancel_callback(call: CallbackQuery, state: FSMContext):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    await call.answer()
    data = await state.get_data()
    edit_menu_msg_id = data.get("edit_menu_msg_id")
    if edit_menu_msg_id:
        try:
            await bot.delete_message(call.message.chat.id, edit_menu_msg_id)
        except Exception:
            pass
        await state.update_data(edit_menu_msg_id=None)
    await show_confirmation(call.message, state)


@router.callback_query(lambda c: c.data.startswith("edit_") and c.data not in {"edit_main", "edit_cancel"})
async def edit_field_callback(call: CallbackQuery, state: FSMContext):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    await call.answer()
    field = call.data
    data = await state.get_data()
    edit_menu_msg_id = data.get("edit_menu_msg_id")
    if edit_menu_msg_id:
        try:
            await bot.delete_message(call.message.chat.id, edit_menu_msg_id)
        except Exception:
            pass
    await state.update_data(edit_mode=True, edit_menu_msg_id=None)

    if field == "edit_customer_name":
        await state.set_state(Form.customer_name)
        await call.message.answer("Введіть, як до вас звертатися:", reply_markup=ReplyKeyboardRemove())
        return

    if field == "edit_car_brand_model":
        await state.set_state(Form.car_brand_model)
        await call.message.answer("Введіть нову марку та модель авто:", reply_markup=ReplyKeyboardRemove())
        return

    if field == "edit_custom_cargo_description":
        await state.set_state(Form.custom_cargo_description)
        await call.message.answer("Введіть новий опис вантажу:", reply_markup=ReplyKeyboardRemove())
        return

    if field == "edit_dimensions":
        await state.set_state(Form.dimensions)
        await call.message.answer("Введіть нові габарити (Д*Ш*В, м):", reply_markup=ReplyKeyboardRemove())
        return

    if field == "edit_weight":
        await state.set_state(Form.weight)
        await call.message.answer("Введіть нову вагу (кг):", reply_markup=ReplyKeyboardRemove())
        return

    if field == "edit_urgency_type":
        await state.set_state(Form.urgency_type)
        await call.message.answer(
            "Оберіть нову терміновість:",
            reply_markup=build_reply_keyboard(URGENCY_TYPES),
        )
        return

    if field == "edit_loading_address":
        await state.set_state(Form.loading_address)
        await call.message.answer(
            "🗺️ Вкажіть нову адресу завантаження.\nВідкрийте карту або введіть адресу текстом.",
            reply_markup=build_address_keyboard("loading"),
        )
        return

    if field == "edit_unloading_address":
        await state.set_state(Form.unloading_address)
        await call.message.answer(
            "🗺️ Вкажіть нову адресу розвантаження.\nВідкрийте карту або введіть адресу текстом.",
            reply_markup=build_address_keyboard("unloading"),
        )
        return

    if field == "edit_client_phone":
        await ask_for_client_phone_input(call.message, edit_mode=True)
        await state.set_state(Form.client_phone_input_choice)
        return

    if field == "edit_loading_phone_choice":
        await state.set_state(Form.loading_phone_choice)
        await call.message.answer(
            "Хто буде на завантаженні?",
            reply_markup=build_reply_keyboard(LOADING_PHONE_OPTIONS),
        )
        return

    if field == "edit_unloading_phone_choice":
        await state.set_state(Form.unloading_phone_choice)
        await call.message.answer(
            "Хто буде на розвантаженні?",
            reply_markup=build_reply_keyboard(UNLOADING_PHONE_OPTIONS),
        )
        return

# =========================
# PROFILE CALLBACKS
# =========================
async def _delete_history_messages(chat_id: int, state: FSMContext):
    data = await state.get_data()
    for msg_id in data.get("history_msg_ids", []):
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
    await state.update_data(history_msg_ids=[])


def _build_history_nav_keyboard(offset: int, total: int) -> types.ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    if offset > 0:
        kb.add(KeyboardButton(text="◀️"))
    if offset + PAGE_SIZE < total:
        kb.add(KeyboardButton(text="▶️"))
    kb.add(KeyboardButton(text="👤 Профіль"))
    kb.adjust(2 if (offset > 0 and offset + PAGE_SIZE < total) else 1)
    return kb.as_markup(resize_keyboard=True)


async def _render_orders_page(chat_id: int, user_id: int, state: FSMContext, offset: int):
    await _delete_history_messages(chat_id, state)

    total = await count_user_orders(user_id)

    if total == 0:
        await _clear_user_kb(chat_id)
        msg = await bot.send_message(
            chat_id,
            "📦 У вас поки що немає заявок через бота.",
            reply_markup=get_profile_keyboard(),
        )
        _user_last_kb_msg[chat_id] = msg.message_id
        await state.update_data(history_msg_ids=[msg.message_id])
        await state.clear()
        return

    orders = await get_orders_page(user_id, offset=offset, limit=PAGE_SIZE)
    page_num = offset // PAGE_SIZE + 1
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE

    sent_ids = []

    for idx, order in enumerate(orders, start=offset + 1):
        service = safe_text(order.get("service_type"), "не вказано")
        loading = safe_text(order.get("loading_address"))
        unloading = safe_text(order.get("unloading_address"))
        route = f"📍 {loading} → {unloading}" if (loading != "-" or unloading != "-") else "📍 не вказано"

        status = order.get("status")
        if status == ORDER_STATUS_ACCEPTED:
            status_text = "✅ Прийнято в роботу"
        elif status == ORDER_STATUS_REJECTED:
            status_text = "❌ Не прийнято"
        elif status == ORDER_STATUS_IN_PROGRESS:
            status_text = "🔄 В обробці"
        elif status == ORDER_STATUS_CREATED:
            status_text = "🆕 Нове"
        else:
            status_text = f"🔄 {status}" if status else "🆕 Нове"

        text = (
            f"<b>{idx}.</b> 🚚 <b>#{order['id']}</b> — {service}\n"
            f"{route}\n"
            f"📊 {status_text}  🕒 {safe_text(order.get('created_at'))}"
        )
        if order.get("price"):
            text += f"\n💰 {safe_text(order.get('price'))}"

        repeat_kb = InlineKeyboardBuilder()
        repeat_kb.add(InlineKeyboardButton(
            text="🔄 Повторити",
            callback_data=f"repeat_order:{order['id']}",
        ))

        msg = await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=repeat_kb.as_markup())
        sent_ids.append(msg.message_id)

    nav_msg = await bot.send_message(
        chat_id,
        f"📄 Стор. {page_num}/{total_pages}",
        reply_markup=_build_history_nav_keyboard(offset, total),
    )
    sent_ids.append(nav_msg.message_id)

    await state.update_data(history_msg_ids=sent_ids, history_offset=offset, history_total=total)
    await state.set_state(Form.history_browse)


async def _show_orders_page(call: CallbackQuery, state: FSMContext, offset: int):
    chat_id = call.message.chat.id
    user = call.from_user
    if user is None:
        return
    try:
        await call.message.delete()
    except Exception:
        pass
    await _render_orders_page(chat_id, user.id, state, offset)


@router.callback_query(lambda c: c.data == "profile_orders")
async def profile_orders_callback(call: CallbackQuery, state: FSMContext):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return
    await call.answer()
    await _show_orders_page(call, state, offset=0)


@router.message(Form.history_browse, lambda m: m.text in ("◀️", "▶️", "👤 Профіль"))
async def history_nav_message(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return

    try:
        await message.delete()
    except Exception:
        pass

    data = await state.get_data()
    offset = data.get("history_offset", 0)
    total = data.get("history_total", 0)

    if message.text == "👤 Профіль":
        await _delete_history_messages(message.chat.id, state)
        await state.clear()
        await send_profile_to_chat(message.chat.id, message.from_user.id)
        return

    if message.text == "◀️":
        new_offset = max(0, offset - PAGE_SIZE)
    else:
        new_offset = offset + PAGE_SIZE

    await _render_orders_page(message.chat.id, message.from_user.id, state, new_offset)


@router.callback_query(lambda c: c.data.startswith("repeat_order:"))
async def repeat_order_callback(call: CallbackQuery, state: FSMContext):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    user = call.from_user
    if user is None:
        await call.answer("Не вдалося визначити користувача.", show_alert=True)
        return

    try:
        order_id = int(call.data.split(":")[1])
    except (IndexError, ValueError):
        await call.answer("Помилка даних.", show_alert=True)
        return

    order = await get_full_order_by_id(order_id, user.id)
    if order is None:
        await call.answer("Замовлення не знайдено.", show_alert=True)
        return

    await state.clear()
    await state.update_data(
        customer_name=order.get("customer_name"),
        client_phone=order.get("client_phone"),
        service_type=order.get("service_type"),
        cargo_name=order.get("cargo_name"),
        custom_cargo_description=order.get("custom_cargo_description"),
        car_brand_model=order.get("car_brand_model"),
        dimensions=order.get("dimensions"),
        weight=order.get("weight"),
        urgency_type=order.get("urgency_type"),
        scheduled_date=order.get("scheduled_date"),
        scheduled_time=order.get("scheduled_time"),
        loading_address=order.get("loading_address"),
        unloading_address=order.get("unloading_address"),
        loading_phone=order.get("loading_phone"),
        unloading_phone=order.get("unloading_phone"),
        payer_type=order.get("payer_type"),
        payer_details=order.get("payer_details"),
        comment=order.get("comment"),
        support_required=order.get("support_required"),
    )

    await call.answer()
    await hide_commands(call.from_user.id)
    await show_confirmation(call.message, state)


@router.callback_query(lambda c: c.data == "profile_new_order")
async def profile_new_order_callback(call: CallbackQuery, state: FSMContext):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    await call.answer()
    await state.clear()
    await hide_commands(call.from_user.id)
    try:
        await call.message.delete()
        _user_last_kb_msg.pop(call.message.chat.id, None)
    except Exception:
        pass
    await call.message.answer(
        "Як до вас звертатися?\nНаприклад: Андрій",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Form.customer_name)


@router.callback_query(lambda c: c.data == "profile_support")
async def profile_support_callback(call: CallbackQuery):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    await call.answer()

    await replace_callback_message(
        call,
        "📞 <b>Підтримка Express T</b>\n\n"
        "Зв'яжіться з диспетчером будь-яким зручним способом:\n\n"
        "📱 <a href=\"tel:333\">333</a>\n"
        "📱 <a href=\"tel:+380665833333\">(066) 583-33-33</a>  <i>Lifecell</i>\n"
        "📱 <a href=\"tel:+380965833333\">(096) 583-33-33</a>  <i>Kyivstar</i>",
        reply_markup=get_profile_keyboard(),
        parse_mode="HTML",
    )
@router.callback_query(lambda c: c.data == "history_close")
async def history_close_callback(call: CallbackQuery):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    await call.answer()

    try:
        await call.message.delete()
    except Exception:
        pass


@router.callback_query(lambda c: c.data == "history_back_to_profile")
async def history_back_to_profile_callback(call: CallbackQuery, state: FSMContext):
    if await deny_if_not_private_callback(call):
        return
    if await deny_if_banned_callback(call):
        return

    await call.answer()
    chat_id = call.message.chat.id
    await _delete_history_messages(chat_id, state)
    await state.clear()
    try:
        await call.message.delete()
    except Exception:
        pass

    user = call.from_user
    if user is None:
        return

    await send_profile_to_chat(chat_id, user.id)

# =========================
# FINAL CONFIRMATION
# =========================
async def show_confirmation(message: Message, state: FSMContext):
    data = await state.get_data()
    summary = build_client_summary(data)

    confirm_kb = ReplyKeyboardBuilder()
    confirm_kb.add(KeyboardButton(text="✅ Підтвердити"))
    confirm_kb.add(KeyboardButton(text="❌ Скасувати"))
    confirm_kb.adjust(2)

    summary_msg = await message.answer(
        summary,
        reply_markup=get_main_edit_keyboard(),
        parse_mode="HTML",
    )
    await state.update_data(summary_msg_id=summary_msg.message_id)
    await message.answer(
        "Використовуйте кнопки нижче для підтвердження.",
        reply_markup=confirm_kb.as_markup(resize_keyboard=True),
    )
    await state.set_state(Form.confirmation)


@router.message(Form.confirmation)
async def process_confirmation(message: Message, state: FSMContext):
    if await deny_if_not_private_message(message):
        return
    if await deny_if_banned_message(message):
        return

    text = message.text or ""

    if text == "✅ Підтвердити":
        data = await state.get_data()
        user = message.from_user
        if user is None:
            await message.answer("Помилка: не вдалося отримати інформацію про користувача.")
            return

        await upsert_user(
            telegram_id=user.id,
            telegram_full_name=user.full_name or "",
            telegram_username=user.username or "",
            customer_name=data.get("customer_name"),
            phone=data.get("client_phone"),
        )

        order_id = await create_order(user.id, data)
        await increment_user_orders_count(user.id)

        # Прибираємо кнопку редагування з повідомлення заявки
        summary_msg_id = data.get("summary_msg_id")
        if summary_msg_id:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=message.chat.id,
                    message_id=summary_msg_id,
                    reply_markup=None,
                )
            except Exception:
                pass

        profile = await get_user_profile(user.id)
        admin_text = build_admin_summary(user, data, order_id, profile=profile)

        asyncio.create_task(write_order_to_sheets(order_id, user, data, profile))

        disp_kb = InlineKeyboardBuilder()
        disp_kb.add(InlineKeyboardButton(
            text="🔄 Взяти в обробку",
            callback_data=f"disp_take:{order_id}:{user.id}",
        ))
        disp_kb.adjust(1)

        order_msg = await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=disp_kb.as_markup(),
        )

        photo_ids = data.get("photo_file_ids") or []
        if photo_ids:
            try:
                from aiogram.types import InputMediaPhoto
                media = [InputMediaPhoto(media=fid) for fid in photo_ids]
                media[0] = InputMediaPhoto(media=photo_ids[0], caption=f"📷 Фото до заявки #{order_id}")
                await bot.send_media_group(ADMIN_CHAT_ID, media=media, reply_to_message_id=order_msg.message_id)
            except Exception:
                logging.warning("Failed to send photos for order %s", order_id)

        await restore_commands(user.id)
        await message.answer(
            "🎉 <b>Дякуємо за ваше замовлення!</b>\n\n"
            "✅ Вашу заявку успішно прийнято.\n"
            "📞 Наш менеджер зателефонує вам найближчим часом для уточнення деталей.\n\n"
            "<i>Очікуйте дзвінка!</i>",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="HTML",
        )
        await state.clear()
        return

    if text == "❌ Скасувати":
        data = await state.get_data()
        summary_msg_id = data.get("summary_msg_id")
        if summary_msg_id:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=message.chat.id,
                    message_id=summary_msg_id,
                    reply_markup=None,
                )
            except Exception:
                pass
        await restore_commands(message.from_user.id)
        await message.answer(
            "❌ Заявку скасовано. Щоб створити нову заявку, натисніть /order",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.clear()
        return

    await message.answer("Будь ласка, використовуйте кнопки для підтвердження.")

# =========================
# DISPATCHER FLOW
# =========================
@router.callback_query(lambda c: c.data and c.data.startswith("disp_take:"))
async def disp_take_callback(call: CallbackQuery):
    await call.answer()
    parts = call.data.split(":")
    if len(parts) != 3:
        return
    order_id = int(parts[1])
    client_id = parts[2]

    dispatcher = call.from_user
    dispatcher_tag = f"@{dispatcher.username}" if dispatcher.username else dispatcher.full_name or str(dispatcher.id)

    await update_order_status_and_price(order_id, status=ORDER_STATUS_IN_PROGRESS)

    accept_reject_kb = InlineKeyboardBuilder()
    accept_reject_kb.add(InlineKeyboardButton(
        text="✅ Прийнято в роботу",
        callback_data=f"disp_accept:{order_id}:{client_id}",
    ))
    accept_reject_kb.add(InlineKeyboardButton(
        text="❌ Не прийнято",
        callback_data=f"disp_reject:{order_id}:{client_id}",
    ))
    accept_reject_kb.adjust(2)

    try:
        await call.message.edit_text(
            (call.message.text or call.message.caption or "") +
            f"\n\n🔄 <b>В обробці.</b> {html.escape(dispatcher_tag)}",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=accept_reject_kb.as_markup(),
        )
    except Exception:
        pass


@router.callback_query(lambda c: c.data and c.data.startswith("disp_accept:"))
async def disp_accept_callback(call: CallbackQuery):
    await call.answer()
    parts = call.data.split(":")
    if len(parts) != 3:
        return
    order_id = int(parts[1])
    client_id = int(parts[2])

    dispatcher = call.from_user
    dispatcher_tag = f"@{dispatcher.username}" if dispatcher.username else dispatcher.full_name or str(dispatcher.id)
    responded_at = datetime.now().strftime("%d.%m.%Y %H:%M")

    await update_order_dispatcher(order_id, ORDER_STATUS_ACCEPTED, dispatcher_tag, responded_at)

    try:
        await call.message.edit_text(
            (call.message.text or call.message.caption or "") +
            f"\n\n✅ <b>Прийнято в роботу.</b> {html.escape(dispatcher_tag)}",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=None,
        )
    except Exception:
        pass

    client_kb = InlineKeyboardBuilder()
    client_kb.add(InlineKeyboardButton(text="🚀 Нове замовлення", callback_data="profile_new_order"))
    client_kb.add(InlineKeyboardButton(text="👤 Мій профіль", callback_data="client_go_profile"))
    client_kb.adjust(1)

    try:
        await _send_with_kb(
            client_id,
            "✅ <b>Ваше замовлення прийнято в роботу!</b>\n\n"
            "Дякуємо, що скористалися нашим сервісом.\n"
            "Менеджер зателефонує вам найближчим часом.\n\n"
            "Бажаєте зробити нове замовлення?",
            client_kb.as_markup(),
        )
    except Exception:
        logging.warning("Could not notify client %s about accepted order %s", client_id, order_id)


@router.callback_query(lambda c: c.data and c.data.startswith("disp_reject:"))
async def disp_reject_callback(call: CallbackQuery):
    await call.answer()
    parts = call.data.split(":")
    if len(parts) != 3:
        return
    order_id = parts[1]
    client_id = parts[2]

    reason_kb = InlineKeyboardBuilder()
    for i, reason in enumerate(DECLINE_REASONS):
        reason_kb.add(InlineKeyboardButton(
            text=reason,
            callback_data=f"disp_reason:{order_id}:{client_id}:{i}",
        ))
    reason_kb.adjust(1)

    try:
        await call.message.edit_reply_markup(reply_markup=reason_kb.as_markup())
    except Exception:
        pass


@router.callback_query(lambda c: c.data and c.data.startswith("disp_reason:"))
async def disp_reason_callback(call: CallbackQuery):
    await call.answer()
    parts = call.data.split(":")
    if len(parts) != 4:
        return
    order_id = int(parts[1])
    client_id = int(parts[2])
    reason_idx = int(parts[3])

    reason = DECLINE_REASONS[reason_idx] if 0 <= reason_idx < len(DECLINE_REASONS) else "Інше"

    dispatcher = call.from_user
    dispatcher_tag = f"@{dispatcher.username}" if dispatcher.username else dispatcher.full_name or str(dispatcher.id)
    responded_at = datetime.now().strftime("%d.%m.%Y %H:%M")

    await update_order_dispatcher(order_id, ORDER_STATUS_REJECTED, dispatcher_tag, responded_at, decline_reason=reason)

    try:
        await call.message.edit_text(
            (call.message.text or call.message.caption or "") +
            f"\n\n❌ <b>Не прийнято.</b> Причина: {html.escape(reason)}. {html.escape(dispatcher_tag)}",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=None,
        )
    except Exception:
        pass

    client_kb = InlineKeyboardBuilder()
    client_kb.add(InlineKeyboardButton(text="🚀 Нове замовлення", callback_data="profile_new_order"))
    client_kb.add(InlineKeyboardButton(text="👤 Мій профіль", callback_data="client_go_profile"))
    client_kb.adjust(1)

    try:
        await _send_with_kb(
            int(client_id),
            "Замовлення скасовано. Будемо раді допомогти наступного разу!\n\n"
            "Бажаєте зробити нове замовлення?",
            client_kb.as_markup(),
        )
    except Exception:
        logging.warning("Could not notify client %s about rejected order %s", client_id, order_id)


@router.callback_query(lambda c: c.data == "client_go_profile")
async def client_go_profile_callback(call: CallbackQuery):
    if await deny_if_not_private_callback(call):
        return
    await call.answer()
    user = call.from_user
    if user is None:
        return
    await send_profile_to_chat(call.message.chat.id, user.id)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    if BOT_TOKEN == "PASTE_NEW_BOT_TOKEN_HERE":
        print("Вкажіть новий BOT_TOKEN у коді або через змінну середовища BOT_TOKEN.")
        sys.exit(1)

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    async def main():
        await init_db()
        await setup_sheets()
        await setup_price_sheet()
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_my_commands(BOT_COMMANDS)
        print("Бот запущено...")
        await dp.start_polling(bot)

    asyncio.run(main())