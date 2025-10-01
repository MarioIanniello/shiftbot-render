#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShiftBot – Gestione cambi turni su Telegram
Versione: 5.7  (album + import + persistence + menu in gruppo)
"""

import os
import re
import sqlite3
import shutil
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, Message,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.constants import ChatType
from telegram.error import Forbidden, BadRequest
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler, ChatMemberHandler,
    ApplicationHandlerStop
)

# -------------------- Persistence helpers --------------------
def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def migrate_sqlite_if_needed(persistent_path: str, legacy_path: str = "shiftbot.sqlite3"):
    """Se /data/shiftbot.sqlite3 non esiste ma c'è il vecchio DB locale, copialo una sola volta."""
    if os.path.abspath(persistent_path) == os.path.abspath(legacy_path):
        return
    if not os.path.exists(persistent_path) and os.path.exists(legacy_path):
        try:
            shutil.copy2(legacy_path, persistent_path)
            print(f"[ShiftBot] Migrato DB da {legacy_path} → {persistent_path}")
        except Exception as e:
            print(f"[ShiftBot] Migrazione DB fallita: {e}")

# -------------------- Config --------------------
VERSION = "ShiftBot 5.7"
DB_PATH = os.environ.get("SHIFTBOT_DB", "shiftbot.sqlite3")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

WELCOME_TEXT = (
    "👋 Benvenuto/a nel gruppo *Cambi Servizi*!\n\n"
    "Per caricare i turni:\n"
    "• Invia l’immagine del turno con una breve descrizione (es. data, note)\n\n"
    "Per cercare i turni (in privato con il bot):\n"
    "• /cerca → calendario e ricerca\n"
    "• /date → elenco date\n"
    "• /miei → i tuoi turni\n"
    "• /version (solo admin nel gruppo)\n"
)

DATE_PATTERNS = [
    r'(?P<d>\d{1,2})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<y>\d{4})',
    r'(?P<y>\d{4})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<d>\d{1,2})',
]

# -------------------- Stato volatile --------------------
PENDING: Dict[int, Dict[str, Any]] = {}      # per SETDATE singolo
MEDIA_GROUPS: Dict[str, Dict[str, Any]] = {} # gestione album a data unica

# -------------------- Tastiera privata persistente --------------------
PRIVATE_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("I miei turni")],                  # alto centro
        [KeyboardButton("Cerca"), KeyboardButton("Date")]  # basso sx/dx
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    is_persistent=True,
    input_field_placeholder="Usa i pulsanti qui sotto 👇"
)

# -------------------- DB --------------------
def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            user_id INTEGER,
            username TEXT,
            date_iso TEXT NOT NULL,
            caption TEXT,
            photo_file_id TEXT,
            status TEXT DEFAULT 'open',
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    # soft migrations
    cur.execute("PRAGMA table_info(shifts);")
    cols = [r[1] for r in cur.fetchall()]
    if "status" not in cols:
        try: cur.execute("ALTER TABLE shifts ADD COLUMN status TEXT DEFAULT 'open';")
        except Exception: pass
    if "photo_file_id" not in cols:
        try: cur.execute("ALTER TABLE shifts ADD COLUMN photo_file_id TEXT;")
        except Exception: pass
    conn.commit()
    conn.close()

def parse_date(text: str) -> Optional[str]:
    if not text:
        return None
    for pat in DATE_PATTERNS:
        m = re.search(pat, text)
        if m:
            try:
                d = int(m.group('d')); mth = int(m.group('m')); y = int(m.group('y'))
                return datetime(y, mth, d).strftime('%Y-%m-%d')
            except Exception:
                continue
    return None

# -------------------- Utils --------------------
async def is_user_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        cm = await ctx.bot.get_chat_member(update.effective_chat.id, user_id)
        return cm.status in ("administrator", "creator")
    except Exception:
        return False

def mention_html(user_id: Optional[int], username: Optional[str]) -> str:
    if username and isinstance(username, str) and username.startswith("@") and len(username) > 1:
        return username
    if user_id:
        return f'<a href="tg://user?id={user_id}">utente</a>'
    return "utente"

async def dm_or_prompt_private(ctx: ContextTypes.DEFAULT_TYPE, user_id: int, group_message: Message, text: str):
    try:
        await ctx.bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown", reply_markup=PRIVATE_KB)
    except Forbidden:
        bot_username = ctx.bot.username or "this_bot"
        url = f"https://t.me/{bot_username}?start=start"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
        await group_message.reply_text("ℹ️ Apri la chat privata con me:", reply_markup=kb)

def has_open_on_date(user_id: int, date_iso: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT 1 FROM shifts
                   WHERE user_id=? AND date_iso=? AND status='open'
                   LIMIT 1""", (user_id, date_iso))
    row = cur.fetchone()
    conn.close()
    return row is not None

def save_shift_raw(chat_id: int, message_id: int, user_id: Optional[int],
                   username: Optional[str], caption: str, date_iso: str,
                   file_id: Optional[str] = None) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO shifts(chat_id, message_id, user_id, username, date_iso, caption, photo_file_id, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'open')""",
        (chat_id, message_id, user_id, (username or ""), date_iso, caption or "", file_id)
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id

async def save_shift(msg: Message, date_iso: str) -> int:
    username = ""
    if msg.from_user:
        username = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.full_name
    file_id = None
    if msg.photo:
        file_id = msg.photo[-1].file_id
    elif getattr(msg, "document", None) and getattr(msg.document, "mime_type", "").startswith("image/"):
        file_id = msg.document.file_id
    return save_shift_raw(
        chat_id=msg.chat.id,
        message_id=msg.message_id,
        user_id=(msg.from_user.id if msg.from_user else None),
        username=username,
        caption=(msg.caption or ""),
        date_iso=date_iso,
        file_id=file_id
    )

async def ensure_private_menu(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, text: Optional[str] = None):
    try:
        await ctx.bot.send_message(chat_id=chat_id, text=text or "Scegli un’azione:", reply_markup=PRIVATE_KB)
    except Exception:
        pass

# -------------------- Import helpers --------------------
async def _save_import_from_message(ctx: ContextTypes.DEFAULT_TYPE, src_msg: Message, date_iso: Optional[str]) -> Optional[int]:
    """
    Salva un turno partendo da un messaggio esistente (foto nel gruppo o inoltro),
    usando chat_id/message_id originali quando disponibili.
    Ritorna l'ID del record creato, -1 se già presente, None se non possibile.
    """
    src_chat_id = src_msg.chat.id
    src_message_id = src_msg.message_id
    if getattr(src_msg, "forward_from_chat", None) and getattr(src_msg, "forward_from_message_id", None):
        src_chat_id = src_msg.forward_from_chat.id
        src_message_id = src_msg.forward_from_message_id

    caption = (getattr(src_msg, "caption", "") or "").strip()
    if not date_iso:
        date_iso = parse_date(caption)
    if not date_iso:
        return None

    owner_id = src_msg.from_user.id if src_msg.from_user else None
    owner_username = (f"@{src_msg.from_user.username}" if src_msg.from_user and src_msg.from_user.username
                      else (src_msg.from_user.full_name if src_msg.from_user else ""))

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT 1 FROM shifts
                   WHERE chat_id=? AND message_id=? AND date_iso=? AND status='open'
                   LIMIT 1""", (src_chat_id, src_message_id, date_iso))
    already = cur.fetchone()
    conn.close()
    if already:
        return -1

    file_id = None
    if getattr(src_msg, "photo", None):
        file_id = src_msg.photo[-1].file_id
    elif getattr(src_msg, "document", None) and getattr(src_msg.document, "mime_type", "").startswith("image/"):
        file_id = src_msg.document.file_id

    return save_shift_raw(
        chat_id=src_chat_id,
        message_id=src_message_id,
        user_id=owner_id,
        username=owner_username,
        caption=caption,
        date_iso=date_iso,
        file_id=file_id
    )

# -------------------- Guardiano comandi nel gruppo --------------------
async def group_command_guard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    text = (msg.text or "").strip()
    if not text.startswith("/"):
        return

    cmd = text.split()[0].lower()
    cmd_base = cmd.split("@")[0]

    admin_allowed = {"/start", "/version", "/import"}      # SOLO admin
    allowed_for_all = {"/menu"}                            # consentito a tutti

    if cmd_base in allowed_for_all:
        return  # lascio al CommandHandler("/menu", ...) senza bloccarlo

    if cmd_base in admin_allowed:
        is_admin = await is_user_admin(update, ctx, user.id) if user else False
        if not is_admin:
            try: await ctx.bot.delete_message(chat.id, msg.message_id)
            except Exception: pass
            await dm_or_prompt_private(ctx, user.id, msg,
                "ℹ️ Nel gruppo solo gli *admin* possono usare /start, /version e /import.\n"
                "Per le ricerche usa i pulsanti in privato.")
            raise ApplicationHandlerStop()
        return

    # altri comandi nel gruppo → cancella e reindirizza in DM
    try: await ctx.bot.delete_message(chat.id, msg.message_id)
    except Exception: pass
    bot_username = ctx.bot.username or "this_bot"
    payload = "search" if cmd.startswith("/cerca") else ("miei" if cmd.startswith("/miei") else "start")
    url = f"https://t.me/{bot_username}?start={payload}"
    try:
        await ctx.bot.send_message(
            chat_id=user.id,
            text=("🛡️ I comandi vanno usati in *privato*.\n\n"
                  "Apri la chat con me e usa:\n"
                  "• /cerca → calendario e ricerca\n"
                  "• /date → elenco date\n"
                  "• /miei → i tuoi turni\n"),
            parse_mode="Markdown",
            reply_markup=PRIVATE_KB
        )
        await ctx.bot.send_message(chat_id=user.id, text="Apri qui la chat privata:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]]))
    except Forbidden:
        pass
    raise ApplicationHandlerStop()

# -------------------- Base handlers --------------------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    payload = None
    if update.message and update.message.text:
        parts = update.message.text.split(maxsplit=1)
        if len(parts) > 1:
            payload = parts[1].strip()

    if update.effective_chat.type == ChatType.PRIVATE:
        if payload:
            if payload.startswith("search"):
                date_iso = None
                if "-" in payload:
                    try:
                        maybe = payload.split("search-", 1)[1]
                        datetime.strptime(maybe, "%Y-%m-%d"); date_iso = maybe
                    except Exception:
                        date_iso = None
                if date_iso:
                    await show_shifts(update, ctx, date_iso)
                else:
                    kb = build_calendar(datetime.today(), mode="SEARCH")
                    await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
                await ensure_private_menu(ctx, update.effective_chat.id)
                return
            if payload == "miei" and update.effective_user:
                await miei_list_dm(ctx, update.effective_user.id)
                await ensure_private_menu(ctx, update.effective_chat.id)
                return

        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown", reply_markup=PRIVATE_KB)
        return

    await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == ChatType.PRIVATE:
        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown", reply_markup=PRIVATE_KB)

async def version_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(VERSION)

async def welcome_new_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chm = update.chat_member
    try:
        old = chm.old_chat_member.status; new = chm.new_chat_member.status
    except Exception:
        return
    if old in ("left", "kicked") and new in ("member", "restricted"):
        await ctx.bot.send_message(chat_id=chm.chat.id, text=WELCOME_TEXT, parse_mode="Markdown")

# -------------------- Foto/Doc (singolo + album) --------------------
async def photo_or_doc_image_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    caption = (msg.caption or "").strip()
    date_iso = parse_date(caption)

    # --- Album ---
    if msg.media_group_id:
        gid = msg.media_group_id
        owner_id = msg.from_user.id if msg.from_user else None
        owner_username = (f"@{msg.from_user.username}" if msg.from_user and msg.from_user.username
                          else (msg.from_user.full_name if msg.from_user else ""))

        g = MEDIA_GROUPS.get(gid)
        if not g:
            g = MEDIA_GROUPS[gid] = {
                "photos": [],
                "caption": caption,
                "date": None,
                "owner_id": owner_id,
                "owner_username": owner_username,
                "src_chat_id": msg.chat.id,
                "calendar_msg_id": None,
                "notified": False,
            }

        if caption and not g["caption"]:
            g["caption"] = caption
        if date_iso and not g["date"]:
            g["date"] = date_iso

        g["photos"].append(msg)

        # Se la data è nota, salva a mano a mano
        if g["date"]:
            await save_shift(msg, g["date"])
            if not g["notified"]:
                human = datetime.strptime(g["date"], "%Y-%m-%d").strftime("%d/%m/%Y")
                try: await ctx.bot.send_message(chat_id=owner_id, text=f"✅ Turno (album) registrato per il {human}", reply_markup=PRIVATE_KB)
                except Exception: pass
                g["notified"] = True
            return

        # Un solo calendario per l'intero album (IMPORTANTE: includi gid nel mode!)
        if not g["calendar_msg_id"]:
            kb = build_calendar(datetime.today(), mode=f"SETDATEALBUM|{gid}")
            cal = await msg.reply_text("📅 Seleziona la data per questo turno (album):", reply_markup=kb)
            g["calendar_msg_id"] = cal.message_id
        return

    # --- Singolo ---
    if not date_iso:
        kb = build_calendar(datetime.today(), mode="SETDATE")
        file_id = (msg.photo[-1].file_id if msg.photo else
                   (msg.document.file_id if getattr(msg, "document", None) and getattr(msg.document, "mime_type", "").startswith("image/") else None))
        cal = await msg.reply_text("📅 Seleziona la data per questo turno:", reply_markup=kb)
        PENDING[cal.message_id] = {
            "src_chat_id": msg.chat.id,
            "src_msg_id": msg.message_id,
            "owner_id": (msg.from_user.id if msg.from_user else None),
            "owner_username": (f"@{msg.from_user.username}" if msg.from_user and msg.from_user.username else (msg.from_user.full_name if msg.from_user else "")),
            "caption": caption,
            "file_id": file_id,
        }
        return

    owner_id = msg.from_user.id if msg.from_user else None
    if owner_id and has_open_on_date(owner_id, date_iso):
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await dm_or_prompt_private(
            ctx, owner_id, msg,
            f"⛔ Hai già un turno *aperto* per il {human}.\n"
            f"Chiudi quello esistente con *Risolto* oppure usa /miei per gestirli."
        )
        try: await ctx.bot.delete_message(msg.chat.id, msg.message_id)
        except Exception: pass
        return

    await save_shift(msg, date_iso)
    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await dm_or_prompt_private(ctx, owner_id, msg, f"✅ Turno registrato per il {human}")

# -------------------- /cerca --------------------
async def search_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    args = ctx.args
    date_iso = parse_date(" ".join(args)) if args else None

    if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    if date_iso:
        await show_shifts(update, ctx, date_iso)
        await ensure_private_menu(ctx, chat.id)
    else:
        kb = build_calendar(datetime.today(), mode="SEARCH")
        await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
        await ensure_private_menu(ctx, chat.id)

# -------------------- /miei --------------------
async def miei_list_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, date_iso, caption, photo_file_id
                   FROM shifts
                   WHERE user_id=? AND status='open'
                   ORDER BY created_at DESC
                   LIMIT 50""", (user_id,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await ctx.bot.send_message(chat_id=user_id, text="Non hai turni aperti al momento.", reply_markup=PRIVATE_KB)
        return

    await ctx.bot.send_message(chat_id=user_id, text="🧾 I tuoi turni aperti:", reply_markup=PRIVATE_KB)
    for sid, chat_id, message_id, date_iso, caption, file_id in rows:
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        # copia screen o fallback a file_id
        copied = False
        try:
            await ctx.bot.copy_message(chat_id=user_id, from_chat_id=chat_id, message_id=message_id)
            copied = True
        except Exception:
            if file_id:
                try:
                    await ctx.bot.send_photo(chat_id=user_id, photo=file_id)
                    copied = True
                except Exception:
                    pass
        if not copied:
            await ctx.bot.send_message(chat_id=user_id, text=f"📄 {human} (immagine non disponibile)")

        await ctx.bot.send_message(
            chat_id=user_id,
            text=f"📅 {human}\n{caption or ''}".strip(),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Risolto", callback_data=f"CLOSE|{sid}")]])
        )

async def miei_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    user = update.effective_user
    if user:
        await miei_list_dm(ctx, user.id)

# -------------------- show & dates --------------------
async def show_shifts(update: Update, ctx: ContextTypes.DEFAULT_TYPE, date_iso: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, user_id, username, caption, photo_file_id
                   FROM shifts
                   WHERE date_iso=? AND status='open'
                   ORDER BY created_at ASC""", (date_iso,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.effective_message.reply_text(
            "Nessun turno salvato per quella data.",
            reply_markup=PRIVATE_KB if update.effective_chat.type == ChatType.PRIVATE else None
        )
        return

    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await update.effective_message.reply_text(
        f"📅 Turni trovati per *{human}*: {len(rows)}",
        parse_mode="Markdown",
        reply_markup=PRIVATE_KB if update.effective_chat.type == ChatType.PRIVATE else None
    )

    for (sid, chat_id, message_id, _user_id, _username, _caption, file_id) in rows:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{sid}")]])
        sent_mid = None
        # prova a copiare e poi attacca i bottoni
        try:
            copied = await ctx.bot.copy_message(chat_id=update.effective_chat.id,
                                                from_chat_id=chat_id, message_id=message_id)
            sent_mid = getattr(copied, "message_id", None)
            if sent_mid:
                try:
                    await ctx.bot.edit_message_reply_markup(update.effective_chat.id, sent_mid, reply_markup=kb)
                    continue
                except BadRequest:
                    pass
        except Exception:
            pass
        # fallback file_id
        if file_id:
            try:
                await ctx.bot.send_photo(chat_id=update.effective_chat.id, photo=file_id, reply_markup=kb)
                continue
            except Exception:
                pass
        # fallback testo
        await ctx.bot.send_message(chat_id=update.effective_chat.id, text="(Immagine non disponibile)", reply_markup=kb)

async def dates_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    await dates_list_dm(ctx, update.effective_chat.id)

# ---- helper DM per /date richiamabile anche dal gruppo ----
async def dates_list_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT date_iso, COUNT(*) FROM shifts
                   WHERE status='open'
                   GROUP BY date_iso ORDER BY date_iso ASC""")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await ctx.bot.send_message(chat_id=user_id, text="Non ci sono turni aperti al momento.", reply_markup=PRIVATE_KB)
        return

    lines = ["📆 *Date con turni aperti:*", ""]
    for date_iso, count in rows:
        d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        lines.append(f"• {d}: {count}")
    await ctx.bot.send_message(chat_id=user_id, text="\n".join(lines), parse_mode="Markdown", reply_markup=PRIVATE_KB)

# ---- helper DM per aprire calendario ricerca ----
async def send_search_calendar_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    kb = build_calendar(datetime.today(), mode="SEARCH")
    await ctx.bot.send_message(chat_id=user_id, text="📅 Seleziona la data che vuoi consultare:", reply_markup=kb)

# -------------------- Calendario --------------------
def build_calendar(base_date: datetime, mode="SETDATE", extra="") -> InlineKeyboardMarkup:
    year, month = base_date.year, base_date.month
    first_day = datetime(year, month, 1)
    next_month = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    prev_month = (first_day - timedelta(days=1)).replace(day=1)

    keyboard = []
    keyboard.append([InlineKeyboardButton(f"{month:02d}/{year}", callback_data="IGNORE")])
    keyboard.append([InlineKeyboardButton(d, callback_data="IGNORE") for d in ["L","M","M","G","V","S","D"]])

    # riempi spazi
    week = []
    for _ in range(first_day.weekday()):
        week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))

    day = first_day
    while day.month == month:
        # Se mode contiene parametri (es. "SETDATEALBUM|<gid>") li lasciamo così
        cb = f"{mode}|{day.strftime('%Y-%m-%d')}"
        week.append(InlineKeyboardButton(str(day.day), callback_data=cb))
        if len(week) == 7:
            keyboard.append(week); week = []
        day += timedelta(days=1)

    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))
        keyboard.append(week)

    # frecce: manteniamo mode intero per non perdere parametri (es. gid)
    keyboard.append([
        InlineKeyboardButton("<", callback_data=f"NAV|{mode}|{prev_month.strftime('%Y-%m-%d')}"),
        InlineKeyboardButton(">", callback_data=f"NAV|{mode}|{next_month.strftime('%Y-%m-%d')}"),
    ])
    return InlineKeyboardMarkup(keyboard)

# -------------------- /import --------------------
async def import_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg  = update.effective_message
    chat = update.effective_chat

    def _has_image(m: Message) -> bool:
        return bool(
            getattr(m, "photo", None) or
            (getattr(m, "document", None) and getattr(m.document, "mime_type", "").startswith("image/"))
        )

    # /import usato nel GRUPPO, solo in reply a uno screenshot e solo admin
    if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        if not (user and await is_user_admin(update, ctx, user.id)):
            await msg.reply_text("Solo gli admin possono usare /import nel gruppo.")
            return
        if not msg.reply_to_message:
            await msg.reply_text("Rispondi al messaggio con lo screenshot e invia /import.")
            return

        target = msg.reply_to_message
        if not _has_image(target):
            await msg.reply_text("Quel messaggio non contiene un’immagine.")
            return

        # Se la data è già nella caption, importa subito
        date_iso = parse_date(target.caption or "")
        if date_iso:
            file_id = None
            if target.photo:
                file_id = target.photo[-1].file_id
            elif getattr(target, "document", None) and getattr(target.document, "mime_type", "").startswith("image/"):
                file_id = target.document.file_id

            new_id = save_shift_raw(
                chat_id=target.chat.id,
                message_id=target.message_id,
                user_id=(target.from_user.id if target.from_user else None),
                username=(f"@{target.from_user.username}" if target.from_user and target.from_user.username else (target.from_user.full_name if target.from_user else "")),
                caption=(target.caption or ""),
                date_iso=date_iso,
                file_id=file_id
            )
            if new_id:
                human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
                await msg.reply_text(f"✅ Importato per il {human}.")
            else:
                await msg.reply_text("Non sono riuscito a importare.")
            return

        # Altrimenti chiedi UNA data e metti tutto in PENDING
        kb = build_calendar(datetime.today(), mode="IMPORTSET")
        cal = await msg.reply_text("📅 Seleziona la data per questo turno:", reply_markup=kb)

        # Prepara dati completi per il callback
        file_id = None
        if target.photo:
            file_id = target.photo[-1].file_id
        elif getattr(target, "document", None) and getattr(target.document, "mime_type", "").startswith("image/"):
            file_id = target.document.file_id

        PENDING[cal.message_id] = {
            "src_chat_id": target.chat.id,
            "src_msg_id": target.message_id,
            "owner_id": (target.from_user.id if target.from_user else None),
            "owner_username": (f"@{target.from_user.username}" if target.from_user and target.from_user.username else (target.from_user.full_name if target.from_user else "")),
            "caption": (target.caption or ""),
            "file_id": file_id,
        }
        return

    # In privato
    if chat.type == ChatType.PRIVATE:
        target = msg.reply_to_message or msg
        if not _has_image(target):
            await msg.reply_text("Inoltra (o rispondi a) un messaggio con l’immagine del turno e invia /import.")
            return

        date_iso = parse_date(target.caption or "")
        if date_iso:
            new_id = await _save_import_from_message(ctx, target, date_iso)
            if new_id == -1:
                await msg.reply_text("Era già stato importato.")
            elif new_id:
                human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
                await msg.reply_text(f"✅ Importato per il {human}.")
            else:
                await msg.reply_text("Non sono riuscito a importare.")
            return

        src_chat_id = target.forward_from_chat.id if getattr(target, "forward_from_chat", None) else target.chat.id
        src_msg_id  = target.forward_from_message_id if getattr(target, "forward_from_message_id", None) else target.message_id
        kb = build_calendar(datetime.today(), mode=f"IMPORTSET|{src_chat_id}|{src_msg_id}|{user.id}")
        await msg.reply_text("📅 Seleziona la data per questo turno:", reply_markup=kb)
        return

# -------------------- Callback inline --------------------
async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = (query.data or "").split("|")

    # ----- SETDATE (singola immagine) -----
    if parts[0] == "SETDATE":
        date_iso = parts[1]
        cal_msg_id = query.message.message_id if query.message else None
        data = PENDING.pop(cal_msg_id, None)

        # fallback: dati passati direttamente nel callback
        if (not data) and len(parts) >= 5:
            try:
                data = {
                    "src_chat_id": int(parts[2]),
                    "src_msg_id": int(parts[3]),
                    "owner_id": int(parts[4]),
                    "owner_username": "",
                    "caption": "",
                    "file_id": None,
                }
            except Exception:
                data = None

        if not data:
            await query.edit_message_text("❌ Non riesco a collegare questo calendario al post originale. Rimanda la foto.")
            return

        owner_id = data["owner_id"]
        if owner_id and has_open_on_date(owner_id, date_iso):
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            try:
                await ctx.bot.send_message(
                    chat_id=owner_id,
                    text=(f"⛔ Hai già un turno *aperto* per il {human}.\n"
                          f"Chiudi quello esistente con *Risolto* oppure usa /miei per gestirli."),
                    parse_mode="Markdown",
                    reply_markup=PRIVATE_KB
                )
                try: await ctx.bot.delete_message(data["src_chat_id"], data["src_msg_id"])
                except Exception: pass
                try: await query.message.delete()
                except Exception: await query.edit_message_reply_markup(reply_markup=None)
            except Forbidden:
                bot_username = ctx.bot.username or "this_bot"
                url = f"https://t.me/{bot_username}?start=start"
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
                try: await ctx.bot.delete_message(data["src_chat_id"], data["src_msg_id"])
                except Exception: pass
                await query.edit_message_text(
                    "⛔ Già presente un tuo turno aperto per quella data.\nApri la chat privata per i dettagli.",
                    reply_markup=kb, parse_mode="Markdown"
                )
            return

        save_shift_raw(
            chat_id=data["src_chat_id"],
            message_id=data["src_msg_id"],
            user_id=owner_id,
            username=data.get("owner_username", ""),
            caption=data.get("caption", ""),
            date_iso=date_iso,
            file_id=data.get("file_id"),
        )

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        try:
            await ctx.bot.send_message(chat_id=owner_id, text=f"✅ Turno registrato per il {human}", reply_markup=PRIVATE_KB)
            try: await query.message.delete()
            except Exception: await query.edit_message_reply_markup(reply_markup=None)
        except Forbidden:
            bot_username = ctx.bot.username or "this_bot"
            url = f"https://t.me/{bot_username}?start=start"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
            await query.edit_message_text("✅ Turno registrato. Per conferme, apri la chat con me:", reply_markup=kb)
        return

    # ----- SETDATEALBUM (album) -----
    elif parts[0] == "SETDATEALBUM":
        # aspettati: ["SETDATEALBUM", "<gid>", "YYYY-MM-DD"]
        if len(parts) < 3:
            await query.edit_message_text("❌ Data non valida.")
            return
        gid = parts[1]; date_iso = parts[2]

        g = MEDIA_GROUPS.get(gid)
        if not g:
            await query.edit_message_text("❌ Album non trovato.")
            return

        g["date"] = date_iso
        for p in list(g["photos"]):
            await save_shift(p, date_iso)

        if not g["notified"]:
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            try: await ctx.bot.send_message(chat_id=g["owner_id"], text=f"✅ Turno (album) registrato per il {human}", reply_markup=PRIVATE_KB)
            except Exception: pass
            g["notified"] = True

        try: await query.message.delete()
        except Exception: await query.edit_message_reply_markup(reply_markup=None)
        MEDIA_GROUPS.pop(gid, None)
        return

    # ----- IMPORTSET (calendario di /import) -----
    elif parts[0] == "IMPORTSET":
        # Due formati supportati:
        # A) IMPORTSET|YYYY-MM-DD                 (GRUPPO: usa PENDING del calendario)
        # B) IMPORTSET|<src_chat_id>|<src_msg_id>|<requester_id>|YYYY-MM-DD  (DM/inoltro)

        if len(parts) == 2:
            date_iso = parts[1]
            cal_msg_id = query.message.message_id if query.message else None
            data = PENDING.pop(cal_msg_id, None)
            if not data:
                await query.edit_message_text("❌ Non riesco a collegare il calendario al messaggio originale.")
                return

            save_shift_raw(
                chat_id=data["src_chat_id"],
                message_id=data["src_msg_id"],
                user_id=data.get("owner_id"),
                username=data.get("owner_username", ""),
                caption=data.get("caption", ""),
                date_iso=date_iso,
                file_id=data.get("file_id"),
            )

            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            try:
                await query.edit_message_text(f"✅ Importato per il {human}.")
            except Exception:
                try: await query.edit_message_reply_markup(reply_markup=None)
                except Exception: pass
            return

        elif len(parts) >= 5:
            try:
                src_chat_id = int(parts[1])
                src_msg_id  = int(parts[2])
                date_iso    = parts[4]
            except Exception:
                await query.edit_message_text("❌ Parametri non validi.")
                return

            class _Fake: ...
            fake = _Fake()
            fake.chat = _Fake(); fake.chat.id = src_chat_id
            fake.message_id = src_msg_id
            fake.caption = ""
            fake.from_user = None
            fake.photo = None
            fake.document = None

            new_id = await _save_import_from_message(ctx, fake, date_iso)
            if new_id == -1:
                await query.edit_message_text("Era già stato importato.")
            elif new_id:
                human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
                await query.edit_message_text(f"✅ Importato per il {human}.")
            else:
                await query.edit_message_text("Non sono riuscito a importare.")
            return

        else:
            await query.answer()
            return

    # ----- SEARCH (dal calendario di ricerca) -----
    elif parts[0] == "SEARCH":
        date_iso = parts[1]
        fake_update = Update(update.update_id, message=query.message)
        await show_shifts(fake_update, ctx, date_iso)
        await query.edit_message_text(
            f"📅 Risultati mostrati per {datetime.strptime(date_iso, '%Y-%m-%d').strftime('%d/%m/%Y')}"
        )
        return

    # ----- NAV (navigazione mese calendario) -----
    elif parts[0] == "NAV":
        # parts: ["NAV", "<MODE>", "YYYY-MM-DD"]
        mode = parts[1]
        new_month = datetime.strptime(parts[2], "%Y-%m-%d")
        kb = build_calendar(new_month, mode)
        await query.edit_message_reply_markup(reply_markup=kb)
        return

    # ----- CLOSE -----
    elif parts[0] == "CLOSE":
        try:
            shift_id = int(parts[1])
        except Exception:
            await query.edit_message_text("❌ ID turno non valido.")
            return

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT user_id, status, date_iso, chat_id, message_id FROM shifts WHERE id=?", (shift_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            await query.edit_message_text("❌ Turno non trovato (forse già rimosso).")
            return
        owner_id, status, date_iso, src_chat_id, src_msg_id = row

        user = update.effective_user
        is_admin = await is_user_admin(update, ctx, user.id) if user else False
        if not user or (user.id != owner_id and not is_admin):
            conn.close()
            await query.answer("Non hai i permessi per chiudere questo turno.", show_alert=True)
            return

        if status == "closed":
            conn.close()
            await query.edit_message_text("ℹ️ Turno già segnato come risolto.")
            return

        try:
            await ctx.bot.delete_message(chat_id=src_chat_id, message_id=src_msg_id)
        except Exception:
            pass

        cur.execute("DELETE FROM shifts WHERE id=?", (shift_id,))
        conn.commit(); conn.close()

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await query.edit_message_text(f"✅ Turno segnato come *Risolto* e rimosso ({human}).", parse_mode="Markdown")
        return

    # ----- CONTACT -----
    elif parts[0] == "CONTACT":
        try:
            shift_id = int(parts[1])
        except Exception:
            await query.answer("ID turno non valido.", show_alert=True)
            return

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""SELECT chat_id, message_id, user_id, username, date_iso
                       FROM shifts WHERE id=?""", (shift_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            await query.answer("Turno non trovato.", show_alert=True)
            return

        src_chat_id, src_msg_id, owner_id, owner_username, date_iso = row
        requester = update.effective_user
        requester_name = mention_html(
            requester.id if requester else None,
            f"@{requester.username}" if requester and requester.username else None
        )

        try:
            await ctx.bot.copy_message(chat_id=owner_id, from_chat_id=src_chat_id, message_id=src_msg_id)
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y") if date_iso else ""
            text_html = (
                f'{requester_name} ti ha contattato per il tuo turno del <b>{human}</b>.\n\n'
                f'<b>Ciao, questo turno è ancora disponibile?</b>'
            )
            await ctx.bot.send_message(chat_id=owner_id, text=text_html, parse_mode="HTML")
            await query.message.reply_text("📬 Ho scritto all’autore in privato. Attendi la risposta.")
        except Forbidden:
            btns = None
            if owner_username and isinstance(owner_username, str) and owner_username.startswith("@"):
                handle = owner_username[1:]
                btns = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("👤 Apri profilo autore", url=f"https://t.me/{handle}")]]
                )
            await query.message.reply_text(
                "⚠️ Non posso scrivere all’autore in privato perché non ha avviato il bot.\n"
                "Contattalo direttamente dal profilo:",
                reply_markup=btns
            )
        except Exception:
            await query.answer("Impossibile inviare il messaggio all’autore.", show_alert=True)
        return

# -------------------- /menu (anche nel gruppo) --------------------
async def menu_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mostra la tastiera ‘I miei turni / Cerca / Date’ anche nel gruppo."""
    await update.effective_message.reply_text("📌 Comandi rapidi:", reply_markup=PRIVATE_KB)

# -------------------- Router pulsanti di testo nel GRUPPO --------------------
async def group_buttons_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Gestisce i testi ‘I miei turni’, ‘Cerca’, ‘Date’ cliccati in gruppo, aprendo il DM."""
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    user = update.effective_user
    if not user:
        return

    text = (update.effective_message.text or "").strip().lower()
    bot_username = ctx.bot.username or "this_bot"

    # (opzionale) tieni il gruppo pulito
    try:
        await ctx.bot.delete_message(update.effective_chat.id, update.effective_message.message_id)
    except Exception:
        pass

    if text == "i miei turni":
        try:
            await miei_list_dm(ctx, user.id)
            await ctx.bot.send_message(chat_id=user.id, text="—", reply_markup=PRIVATE_KB)
            await update.effective_message.reply_text("✉️ Ti ho scritto in privato.", reply_markup=PRIVATE_KB)
        except Forbidden:
            url = f"https://t.me/{bot_username}?start=miei"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]])
            await update.effective_message.reply_text("Apri la chat privata per vedere i tuoi turni:", reply_markup=kb)
        return

    if text == "cerca":
        try:
            await send_search_calendar_dm(ctx, user.id)
            await update.effective_message.reply_text("✉️ Ti ho scritto in privato.", reply_markup=PRIVATE_KB)
        except Forbidden:
            url = f"https://t.me/{bot_username}?start=search"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]])
            await update.effective_message.reply_text("Apri la chat privata per cercare nei turni:", reply_markup=kb)
        return

    if text == "date":
        try:
            await dates_list_dm(ctx, user.id)
            await update.effective_message.reply_text("✉️ Ti ho scritto in privato.", reply_markup=PRIVATE_KB)
        except Forbidden:
            url = f"https://t.me/{bot_username}?start=start"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]])
            await update.effective_message.reply_text("Apri la chat privata per vedere le date con turni aperti:", reply_markup=kb)
        return

# -------------------- DM text router / block --------------------
async def private_text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    text = (update.effective_message.text or "").strip().lower()
    if text in ("/cerca", "cerca"):
        await search_cmd(update, ctx); return
    if text in ("/date", "date"):
        await dates_cmd(update, ctx); return
    if text in ("/miei", "miei", "i miei turni"):
        await miei_cmd(update, ctx); return
    await update.effective_message.reply_text("Usa i pulsanti qui sotto 👇", reply_markup=PRIVATE_KB)

async def block_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == ChatType.PRIVATE:
        await update.effective_message.reply_text("Usa i pulsanti 👇", reply_markup=PRIVATE_KB)

# -------------------- MAIN --------------------
def main():
    if not TOKEN:
        raise SystemExit("Errore: variabile d'ambiente TELEGRAM_BOT_TOKEN mancante.")

    ensure_parent_dir(DB_PATH)
    migrate_sqlite_if_needed(DB_PATH)
    print(f"[ShiftBot] DB_PATH = {DB_PATH}")

    ensure_db()
    app = ApplicationBuilder().token(TOKEN).build()

    # Guardiano nel gruppo
    app.add_handler(MessageHandler(filters.COMMAND, group_command_guard), group=0)

    # Comandi
    app.add_handler(CommandHandler("start", start), group=1)
    app.add_handler(CommandHandler("help", help_cmd), group=1)
    app.add_handler(CommandHandler("version", version_cmd), group=1)
    app.add_handler(CommandHandler("cerca", search_cmd), group=1)
    app.add_handler(CommandHandler("date", dates_cmd), group=1)
    app.add_handler(CommandHandler("miei", miei_cmd), group=1)
    app.add_handler(CommandHandler("import", import_cmd), group=1)
    app.add_handler(CommandHandler("menu", menu_cmd), group=1)  # NEW

    # Alias tastiera privata (DM)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^I miei turni$"), miei_cmd), group=1)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Cerca$"), search_cmd), group=1)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Date$"),  dates_cmd),  group=1)

    # Pulsanti di testo anche nel GRUPPO (NEW)
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & filters.TEXT & filters.Regex("^(I miei turni|Cerca|Date)$"),
            group_buttons_router
        ),
        group=1
    )

    # Media (foto/documento immagine)
    img_doc_filter = filters.Document.IMAGE if hasattr(filters.Document, "IMAGE") else filters.Document.MimeType("image/")
    app.add_handler(MessageHandler(filters.PHOTO | img_doc_filter, photo_or_doc_image_handler), group=1)

    # Callback inline
    app.add_handler(CallbackQueryHandler(button_handler), group=1)

    # Benvenuto membri
    app.add_handler(ChatMemberHandler(welcome_new_member, ChatMemberHandler.CHAT_MEMBER), group=1)

    # Blocca altro testo in DM
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.Regex("^(I miei turni|Cerca|Date)$"), block_text), group=3)

    print("ShiftBot avviato. Premi Ctrl+C per uscire.")
    app.run_polling()

if __name__ == "__main__":
    main()