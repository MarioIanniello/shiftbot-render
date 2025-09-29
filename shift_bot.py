#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShiftBot – Gestione cambi turni su Telegram
Versione: 3.8.2 (album support)
"""

import os
import re
import sqlite3
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

VERSION = "ShiftBot 3.8.2 (album)"
DB_PATH = os.environ.get("SHIFTBOT_DB", "shiftbot.sqlite3")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

WELCOME_TEXT = (
    "👋 Benvenuto/a nel gruppo *Cambi Servizi*!\n\n"
    "Per caricare i turni:\n"
    "• Invia l’immagine del turno con una breve descrizione (es. data, note)\n\n"
    "Per cercare i turni:\n"
    "• Digita i comandi in *privato* con il bot:\n"
    "   `/cerca`, `/date`, `/miei`\n"
    "• `/version` (solo admin nel gruppo)\n"
)

DATE_PATTERNS = [
    r'(?P<d>\d{1,2})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<y>\d{4})',
    r'(?P<y>\d{4})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<d>\d{1,2})',
]

# ====== Stato in memoria (resettato a ogni riavvio) ======
PENDING: Dict[int, Dict[str, Any]] = {}  # key = calendar_message_id

# NEW: stato album in arrivo (key = media_group_id)
MEDIA_GROUPS: Dict[str, Dict[str, Any]] = {
    # media_group_id: {
    #   "photos": [Message, ...],
    #   "caption": str,
    #   "date": "YYYY-MM-DD" | None,
    #   "owner_id": int | None,
    #   "owner_username": str,
    #   "src_chat_id": int,
    #   "calendar_msg_id": int | None,  # id del messaggio calendario creato
    #   "notified": bool,               # per evitare doppi messaggi
    # }
}

# ====== Tastiera persistente in DM ======
PRIVATE_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("/cerca")],
        [KeyboardButton("/date"), KeyboardButton("/miei")],
    ],
    resize_keyboard=True,
    is_persistent=True,
    selective=True,
)

# ============== DB ==============
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
    # soft-migrations
    cur.execute("PRAGMA table_info(shifts);")
    cols = [r[1] for r in cur.fetchall()]
    if "status" not in cols:
        try:
            cur.execute("ALTER TABLE shifts ADD COLUMN status TEXT DEFAULT 'open';")
        except Exception:
            pass
    if "photo_file_id" not in cols:
        try:
            cur.execute("ALTER TABLE shifts ADD COLUMN photo_file_id TEXT;")
        except Exception:
            pass
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

# ============== UTILS ==============
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

def contact_buttons(shift_id: int, owner_username: Optional[str]) -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{shift_id}")]
    if owner_username and owner_username.startswith("@") and len(owner_username) > 1:
        handle = owner_username[1:]
        row.append(InlineKeyboardButton("👤 Profilo autore", url=f"https://t.me/{handle}"))
    return InlineKeyboardMarkup([row])

async def dm_or_prompt_private(ctx: ContextTypes.DEFAULT_TYPE, user_id: int, group_message: Message, text: str):
    """Prova DM; altrimenti in gruppo mostra bottone per aprire DM."""
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
    # file_id per fallback (foto o documento immagine)
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

async def ensure_private_menu(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str = None):
    try:
        await ctx.bot.send_message(chat_id=chat_id, text=text or "Scegli un’azione:", reply_markup=PRIVATE_KB)
    except Exception:
        pass

# ============== GUARDIANO COMANDI IN GRUPPO ==============
async def group_command_guard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Nel gruppo: solo admin /start /version. Tutti gli altri comandi cancellati e reindirizzati in DM."""
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    text = (msg.text or "").strip()
    if not text.startswith("/"):
        return

    cmd = text.split()[0].lower()
    admin_allowed = {"/start", "/version"}

    if cmd in admin_allowed:
        is_admin = await is_user_admin(update, ctx, user.id) if user else False
        if not is_admin:
            try:
                await ctx.bot.delete_message(chat_id=chat.id, message_id=msg.message_id)
            except Exception:
                pass
            await dm_or_prompt_private(
                ctx, user.id, msg,
                "ℹ️ Nel gruppo solo gli *admin* possono usare /start e /version.\n"
                "Per le ricerche usa i pulsanti in privato."
            )
            raise ApplicationHandlerStop()
        return

    # Qualsiasi altro comando nel gruppo → cancella e rimanda in DM
    try:
        await ctx.bot.delete_message(chat_id=chat.id, message_id=msg.message_id)
    except Exception:
        pass

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
        await ctx.bot.send_message(chat_id=user.id, text="Apri qui la chat privata:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]]))
    except Forbidden:
        pass
    raise ApplicationHandlerStop()

# ============== HANDLERS BASE ==============
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
                        datetime.strptime(maybe, "%Y-%m-%d")
                        date_iso = maybe
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
        old = chm.old_chat_member.status
        new = chm.new_chat_member.status
    except Exception:
        return
    if old in ("left", "kicked") and new in ("member", "restricted"):
        await ctx.bot.send_message(chat_id=chm.chat.id, text=WELCOME_TEXT, parse_mode="Markdown")

# ============== FOTO/DOC (invio turno) ==============
async def photo_or_doc_image_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    caption = (msg.caption or "").strip()
    date_iso = parse_date(caption)

    # --- GESTIONE ALBUM (più immagini inviate insieme) ---
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

        # aggiorna caption se arriva una con testo
        if caption and not g["caption"]:
            g["caption"] = caption

        # se la caption di QUALSIASI foto contiene data, usala per tutto l'album
        if date_iso and not g["date"]:
            g["date"] = date_iso

        # accumula il messaggio foto/documento
        g["photos"].append(msg)

        # Se abbiamo già la data (in didascalia), salviamo ogni elemento man mano
        if g["date"]:
            # (opzionale) NON applicare blocco duplicati per album: è un'unica richiesta
            await save_shift(msg, g["date"])
            if not g["notified"]:
                human = datetime.strptime(g["date"], "%Y-%m-%d").strftime("%d/%m/%Y")
                try:
                    await ctx.bot.send_message(chat_id=owner_id, text=f"✅ Turno (album) registrato per il {human}", reply_markup=PRIVATE_KB)
                except Exception:
                    pass
                g["notified"] = True
            return

        # Se NON abbiamo ancora la data, mostriamo un SOLO calendario (una volta)
        if not g["calendar_msg_id"]:
            kb = build_calendar(datetime.today(), mode=f"SETDATEALBUM|{gid}")
            cal = await msg.reply_text("📅 Seleziona la data per questo turno (album):", reply_markup=kb)
            g["calendar_msg_id"] = cal.message_id
        return

    # --- SINGOLA IMMAGINE ---
    if not date_iso:
        kb = build_calendar(datetime.today(), mode="SETDATE")
        # prepara dati per PENDING
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
        try:
            await ctx.bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
        except Exception:
            pass
        return

    await save_shift(msg, date_iso)
    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await dm_or_prompt_private(ctx, owner_id, msg, f"✅ Turno registrato per il {human}")

# ============== CERCA (solo DM) ==============
async def search_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    args = ctx.args

    date_iso = None
    if args:
        date_iso = parse_date(" ".join(args))

    if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return  # intercettato dal guard

    if date_iso:
        await show_shifts(update, ctx, date_iso)
        await ensure_private_menu(ctx, chat.id)
    else:
        kb = build_calendar(datetime.today(), mode="SEARCH")
        await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
        await ensure_private_menu(ctx, chat.id)

# ============== /MIEI (solo DM) ==============
async def miei_list_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, date_iso, caption, photo_file_id
                   FROM shifts
                   WHERE user_id=? AND status='open'
                   ORDER BY created_at DESC
                   LIMIT 50""", (user_id,))  # aumento a 50 per sicurezza
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await ctx.bot.send_message(chat_id=user_id, text="Non hai turni aperti al momento.", reply_markup=PRIVATE_KB)
        return

    await ctx.bot.send_message(chat_id=user_id, text="🧾 I tuoi turni aperti:", reply_markup=PRIVATE_KB)
    for sid, chat_id, message_id, date_iso, caption, file_id in rows:
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        sent_mid = None
        # 1) prova a copiare lo screenshot originale
        try:
            res = await ctx.bot.copy_message(chat_id=user_id, from_chat_id=chat_id, message_id=message_id)
            sent_mid = getattr(res, "message_id", None)
        except Exception:
            sent_mid = None
        # 2) fallback con file_id
        if sent_mid is None and file_id:
            try:
                m = await ctx.bot.send_photo(chat_id=user_id, photo=file_id)
                sent_mid = m.message_id
            except Exception:
                pass
        # 3) invia i bottoni (Risolto + Contatta autore) sotto
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Risolto", callback_data=f"CLOSE|{sid}")]])
        # piccolo trucco: bottoni sotto uno screenshot copiato → invia messaggio “non vuoto” con NBSP
        try:
            await ctx.bot.send_message(chat_id=user_id, text="\u00A0", reply_markup=kb, reply_to_message_id=sent_mid or None)
        except BadRequest:
            await ctx.bot.send_message(chat_id=user_id, text="\u00A0", reply_markup=kb)

async def miei_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    user = update.effective_user
    if not user:
        return
    await miei_list_dm(ctx, user.id)

# ============== SHOW & DATES ==============
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
        await update.effective_message.reply_text("Nessun turno salvato per quella data.", reply_markup=PRIVATE_KB if update.effective_chat.type==ChatType.PRIVATE else None)
        return

    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await update.effective_message.reply_text(
        f"📅 Turni trovati per *{human}*: {len(rows)}",
        parse_mode="Markdown",
        reply_markup=PRIVATE_KB if update.effective_chat.type==ChatType.PRIVATE else None
    )

    for (sid, chat_id, message_id, user_id, username, caption, file_id) in rows:
        sent_mid = None
        # 1) prova copia
        try:
            res = await ctx.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=chat_id,
                message_id=message_id
            )
            sent_mid = getattr(res, "message_id", None)
        except Exception:
            sent_mid = None
        # 2) fallback con file_id
        if sent_mid is None and file_id:
            try:
                m = await ctx.bot.send_photo(chat_id=update.effective_chat.id, photo=file_id)
                sent_mid = m.message_id
            except Exception:
                sent_mid = None

        # 3) bottoni “Contatta autore” + (opzionale) “Profilo”
        btns = [
            InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{sid}"),
        ]
        if username and isinstance(username, str) and username.startswith("@") and len(username) > 1:
            handle = username[1:]
            btns.append(InlineKeyboardButton("👤 Profilo autore", url=f"https://t.me/{handle}"))

        # metti i bottoni sotto allo screenshot (messaggio separato con NBSP)
        try:
            await ctx.bot.send_message(
                chat_id=update.effective_chat.id,
                text="\u00A0",
                reply_markup=InlineKeyboardMarkup([btns]),
                reply_to_message_id=sent_mid or None
            )
        except BadRequest:
            await ctx.bot.send_message(
                chat_id=update.effective_chat.id,
                text="\u00A0",
                reply_markup=InlineKeyboardMarkup([btns])
            )

async def dates_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT date_iso, COUNT(*) FROM shifts
                   WHERE status='open'
                   GROUP BY date_iso ORDER BY date_iso ASC""")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.effective_message.reply_text("Non ci sono turni aperti al momento.", reply_markup=PRIVATE_KB)
        return

    lines = ["📆 *Date con turni aperti:*", ""]
    for date_iso, count in rows:
        d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        lines.append(f"• {d}: {count}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=PRIVATE_KB)

# ============== CALENDARIO INLINE ==============
def build_calendar(base_date: datetime, mode="SETDATE", extra="") -> InlineKeyboardMarkup:
    year, month = base_date.year, base_date.month
    first_day = datetime(year, month, 1)
    next_month = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    prev_month = (first_day - timedelta(days=1)).replace(day=1)

    keyboard = []
    keyboard.append([InlineKeyboardButton(f"{month:02d}/{year}", callback_data="IGNORE")])
    keyboard.append([InlineKeyboardButton(d, callback_data="IGNORE") for d in ["L","M","M","G","V","S","D"]])

    week = []
    for _ in range(first_day.weekday()):
        week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))

    day = first_day
    while day.month == month:
        cb = f"{mode}|{day.strftime('%Y-%m-%d')}"
        week.append(InlineKeyboardButton(str(day.day), callback_data=cb))
        if len(week) == 7:
            keyboard.append(week); week = []
        day += timedelta(days=1)

    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))
        keyboard.append(week)

    keyboard.append([
        InlineKeyboardButton("<", callback_data=f"NAV|{mode}|{prev_month.strftime('%Y-%m-%d')}|"),
        InlineKeyboardButton(">", callback_data=f"NAV|{mode}|{next_month.strftime('%Y-%m-%d')}|")
    ])
    return InlineKeyboardMarkup(keyboard)

# ============== CALLBACK INLINE ==============
async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = (query.data or "").split("|")

    if parts[0] == "SETDATE":
        date_iso = parts[1]
        cal_msg_id = query.message.message_id if query.message else None
        data = PENDING.pop(cal_msg_id, None)

        if (not data) and len(parts) >= 5:
            try:
                data = {
                    "src_chat_id": int(parts[2]),
                    "src_msg_id": int(parts[3]),
                    "owner_id": int(parts[4]),
                    "owner_username": "",
                    "caption": "",
                    "file_id": None
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
                try:
                    await ctx.bot.delete_message(chat_id=data["src_chat_id"], message_id=data["src_msg_id"])
                except Exception:
                    pass
                try:
                    await query.message.delete()
                except Exception:
                    await query.edit_message_reply_markup(reply_markup=None)
            except Forbidden:
                bot_username = ctx.bot.username or "this_bot"
                url = f"https://t.me/{bot_username}?start=start"
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
                try:
                    await ctx.bot.delete_message(chat_id=data["src_chat_id"], message_id=data["src_msg_id"])
                except Exception:
                    pass
                await query.edit_message_text(
                    f"⛔ Questo turno non è stato salvato: c'è già un tuo turno *aperto* per quella data.\n"
                    f"Apri la chat privata per i dettagli.",
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
            file_id=data.get("file_id")
        )

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        try:
            await ctx.bot.send_message(chat_id=owner_id, text=f"✅ Turno registrato per il {human}", reply_markup=PRIVATE_KB)
            try:
                await query.message.delete()
            except Exception:
                await query.edit_message_reply_markup(reply_markup=None)
        except Forbidden:
            bot_username = ctx.bot.username or "this_bot"
            url = f"https://t.me/{bot_username}?start=start"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
            await query.edit_message_text(
                "✅ Turno registrato. Per ricevere le conferme in privato, apri la chat con me:",
                reply_markup=kb
            )
        return

    elif parts[0] == "SETDATEALBUM":
        # formato: SETDATEALBUM|<media_group_id>|<YYYY-MM-DD>
        if len(parts) < 3:
            await query.edit_message_text("❌ Data non valida.")
            return
        gid = parts[1]
        date_iso = parts[2]

        g = MEDIA_GROUPS.get(gid)
        if not g:
            await query.edit_message_text("❌ Album non trovato.")
            return

        g["date"] = date_iso

        # Salva TUTTE le foto accumulate per questo album
        for p in list(g["photos"]):
            await save_shift(p, date_iso)

        # notifica una sola volta al proprietario
        if not g["notified"]:
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            try:
                await ctx.bot.send_message(chat_id=g["owner_id"], text=f"✅ Turno (album) registrato per il {human}", reply_markup=PRIVATE_KB)
            except Exception:
                pass
            g["notified"] = True

        # chiudi calendario
        try:
            if query.message:
                await query.message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

        # pulizia memoria album
        MEDIA_GROUPS.pop(gid, None)
        return

    elif parts[0] == "SEARCH":
        date_iso = parts[1]
        fake_update = Update(update.update_id, message=query.message)
        await show_shifts(fake_update, ctx, date_iso)
        await query.edit_message_text(f"📅 Risultati mostrati per {datetime.strptime(date_iso, '%Y-%m-%d').strftime('%d/%m/%Y')}")

    elif parts[0] == "NAV":
        mode = parts[1]
        new_month = datetime.strptime(parts[2], "%Y-%m-%d")
        kb = build_calendar(new_month, mode)
        await query.edit_message_reply_markup(reply_markup=kb)

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

        # elimina dal gruppo se possibile (potrebbe essere già cancellato)
        try:
            await ctx.bot.delete_message(chat_id=src_chat_id, message_id=src_msg_id)
        except Exception:
            pass

        cur.execute("DELETE FROM shifts WHERE id=?", (shift_id,))
        conn.commit()
        conn.close()

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await query.edit_message_text(f"✅ Turno segnato come *Risolto* e rimosso ({human}).", parse_mode="Markdown")

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
        requester_name = mention_html(requester.id if requester else None,
                                      f"@{requester.username}" if requester and requester.username else None)

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
                btns = InlineKeyboardMarkup([[InlineKeyboardButton("👤 Apri profilo autore", url=f"https://t.me/{handle}")]])
            await query.message.reply_text(
                "⚠️ Non posso scrivere all’autore in privato perché non ha avviato il bot.\n"
                "Contattalo direttamente dal profilo:",
                reply_markup=btns
            )
        except Exception:
            await query.answer("Impossibile inviare il messaggio all’autore.", show_alert=True)

# ============== DM TEXT ROUTER ==============
async def private_text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """In DM accetta solo i pulsanti/alias; per il resto ricorda di usare i pulsanti."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    text = (update.effective_message.text or "").strip().lower()

    if text in ("/cerca", "cerca"):
        await search_cmd(update, ctx)
        return
    if text in ("/date", "date"):
        await dates_cmd(update, ctx)
        return
    if text in ("/miei", "miei"):
        await miei_cmd(update, ctx)
        return

    await update.effective_message.reply_text("Usa i pulsanti qui sotto 👇", reply_markup=PRIVATE_KB)

# ============== MAIN ==============
def main():
    if not TOKEN:
        raise SystemExit("Errore: variabile d'ambiente TELEGRAM_BOT_TOKEN mancante.")

    ensure_db()
    app = ApplicationBuilder().token(TOKEN).build()

    # Guardiano comandi in gruppo (prima dei CommandHandler)
    app.add_handler(MessageHandler(filters.COMMAND, group_command_guard), group=0)

    # Comandi
    app.add_handler(CommandHandler("start", start), group=1)
    app.add_handler(CommandHandler("help", help_cmd), group=1)
    app.add_handler(CommandHandler("version", version_cmd), group=1)
    app.add_handler(CommandHandler("cerca", search_cmd), group=1)
    app.add_handler(CommandHandler("date", dates_cmd), group=1)
    app.add_handler(CommandHandler("miei", miei_cmd), group=1)

    # Foto/immagini: screenshot turni (foto + document image)
    img_doc_filter = filters.Document.IMAGE if hasattr(filters.Document, "IMAGE") else filters.Document.MimeType("image/")
    app.add_handler(MessageHandler(filters.PHOTO | img_doc_filter, photo_or_doc_image_handler), group=1)

    # Inline
    app.add_handler(CallbackQueryHandler(button_handler), group=1)

    # Benvenuto nuovi membri
    app.add_handler(ChatMemberHandler(welcome_new_member, ChatMemberHandler.CHAT_MEMBER), group=1)

    # Router testi privati (dopo i command handler)
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), private_text_router), group=2)

    print("ShiftBot avviato. Premi Ctrl+C per uscire.")
    app.run_polling()

if __name__ == "__main__":
    main()