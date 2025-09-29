#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShiftBot – Gestione cambi turni (Telegram)
Versione: 4.2
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

# ====== Config ======
VERSION = "ShiftBot 4.2"
DB_PATH = os.environ.get("SHIFTBOT_DB", "shiftbot.sqlite3")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

WELCOME_TEXT = (
    "👋 Benvenuto/a nel gruppo <b>Cambi Servizi</b>!\n\n"
    "Per caricare i turni:\n"
    "• Invia l’immagine del turno con una breve descrizione "
    "(es. <i>Cambio per mattina</i>, <i>Cambio per intermedia</i>, <i>Cambio per pomeriggio</i>)\n\n"
    "Per cercare i turni (solo in <b>privato</b> col bot):\n"
    "• <code>/cerca</code> → calendario e ricerca\n"
    "• <code>/date</code> → elenco date\n"
    "• <code>/miei</code> → i tuoi turni\n"
    "• <code>/version</code> (solo admin nel gruppo)\n"
)

DATE_PATTERNS = [
    r'(?P<d>\d{1,2})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<y>\d{4})',  # 31/12/2025
    r'(?P<y>\d{4})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<d>\d{1,2})',  # 2025-12-31
]

# ====== Stato volatile ======
PENDING: Dict[int, Dict[str, Any]] = {}       # calendari SETDATE per messaggi singoli
MEDIA_GROUPS: Dict[str, Dict[str, Any]] = {}  # gestione album multipli

# ====== Tastiera persistente DM ======
PRIVATE_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("I miei turni")],
        [KeyboardButton("Cerca"), KeyboardButton("Date")],
    ],
    resize_keyboard=True,
    is_persistent=True,
    selective=True,
)

# ============== DB =================
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

def contact_only_buttons(shift_id: int) -> InlineKeyboardMarkup:
    # Bottone unico per /cerca
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{shift_id}")
    ]])

async def dm_or_prompt_private(ctx: ContextTypes.DEFAULT_TYPE, user_id: int, group_message: Message, text: str):
    try:
        await ctx.bot.send_message(chat_id=user_id, text=text, parse_mode="HTML", reply_markup=PRIVATE_KB)
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

    # Salva file_id per fallback
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

# Invia bottoni sotto un messaggio, con fallback
async def send_buttons_below(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int, replied_mid: Optional[int], markup: InlineKeyboardMarkup):
    text = "\u00A0"  # NBSP invisibile ma valido per Telegram
    if replied_mid:
        try:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=markup,
                reply_to_message_id=replied_mid,
                allow_sending_without_reply=True
            )
            return
        except BadRequest:
            pass
        except Exception:
            pass
    # Fallback: senza reply
    await ctx.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)

# ============== GUARD COMANDI IN GRUPPO ==============
async def group_command_guard(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Nel gruppo: solo admin /start /version. Altri comandi (cerca/date/miei) → DM."""
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if not msg or not (msg.text or "").startswith("/"):
        return

    cmd = (msg.text or "").split()[0].lower()
    admin_ok = {"/start", "/version"}

    if cmd in admin_ok:
        if not (user and await is_user_admin(update, ctx, user.id)):
            try: await ctx.bot.delete_message(chat.id, msg.message_id)
            except Exception: pass
            await dm_or_prompt_private(
                ctx, user.id, msg,
                "ℹ️ Nel gruppo solo gli <b>admin</b> possono usare /start e /version.\n"
                "Per le ricerche usa i pulsanti in privato."
            )
            raise ApplicationHandlerStop()
        return

    # altri comandi → cancella e reindirizza
    try: await ctx.bot.delete_message(chat.id, msg.message_id)
    except Exception: pass

    bot_username = ctx.bot.username or "this_bot"
    url = f"https://t.me/{bot_username}?start=start"
    try:
        await ctx.bot.send_message(
            chat_id=user.id,
            text=("🛡️ I comandi vanno usati in <b>privato</b>.\n\n"
                  "• <code>/cerca</code> → calendario e ricerca\n"
                  "• <code>/date</code> → elenco date\n"
                  "• <code>/miei</code> → i tuoi turni\n"),
            parse_mode="HTML", reply_markup=PRIVATE_KB
        )
        await ctx.bot.send_message(user.id, "Apri qui la chat privata:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]]))
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
        if payload == "miei" and update.effective_user:
            await miei_list_dm(ctx, update.effective_user.id)
            await ensure_private_menu(ctx, update.effective_chat.id)
            return
        if payload and payload.startswith("search"):
            kb = build_calendar(datetime.today(), mode="SEARCH")
            await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
            await ensure_private_menu(ctx, update.effective_chat.id)
            return

        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="HTML", reply_markup=PRIVATE_KB)
    else:
        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="HTML")

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == ChatType.PRIVATE:
        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="HTML", reply_markup=PRIVATE_KB)

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
        await ctx.bot.send_message(chat_id=chm.chat.id, text=WELCOME_TEXT, parse_mode="HTML")

# ============== FOTO/DOC (turno + album) ==============
async def photo_or_doc_image_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    caption = (msg.caption or "").strip()
    date_iso = parse_date(caption)
    owner_id = msg.from_user.id if msg.from_user else None
    owner_username = (f"@{msg.from_user.username}" if msg.from_user and msg.from_user.username else (msg.from_user.full_name if msg.from_user else ""))

    # ----- Album -----
    if msg.media_group_id:
        gid = msg.media_group_id
        g = MEDIA_GROUPS.get(gid)
        if not g:
            g = MEDIA_GROUPS[gid] = {
                "photos": [],
                "caption": caption,
                "src_chat_id": msg.chat.id,
                "owner_id": owner_id,
                "owner_username": owner_username,
                "date": None,
                "decision": None,
                "notified": False
            }
        g["photos"].append(msg)

        if (not g["date"]) and date_iso:
            g["date"] = date_iso

        if g["date"] and g["decision"] is None:
            if owner_id and has_open_on_date(owner_id, g["date"]):
                g["decision"] = "blocked"
                human = datetime.strptime(g["date"], "%Y-%m-%d").strftime("%d/%m/%Y")
                await dm_or_prompt_private(
                    ctx, owner_id, msg,
                    f"⛔ Hai già un turno <b>aperto</b> per il {human}.\n"
                    f"Chiudi quello esistente con <b>Risolto</b> oppure usa /miei."
                )
                for p in list(g["photos"]):
                    try: await ctx.bot.delete_message(p.chat.id, p.message_id)
                    except Exception: pass
                return
            else:
                g["decision"] = "allowed"
                if not g["notified"]:
                    human = datetime.strptime(g["date"], "%Y-%m-%d").strftime("%d/%m/%Y")
                    await dm_or_prompt_private(ctx, owner_id, msg, f"✅ Turno (album) registrato per il {human}")
                    g["notified"] = True

        # primo elemento senza data → chiedi UNA data
        if not g["date"] and len(g["photos"]) == 1:
            kb = build_calendar(datetime.today(), mode=f"SETDATEALBUM|{gid}")
            cal = await msg.reply_text("📅 Seleziona la data per questo turno (album):", reply_markup=kb)
            PENDING[cal.message_id] = {"album_id": gid}
            return

        if g["date"] and g["decision"] == "allowed":
            await save_shift(msg, g["date"])
        return

    # ----- Singolo -----
    if not date_iso:
        kb = build_calendar(datetime.today(), mode="SETDATE")
        file_id = (msg.photo[-1].file_id if msg.photo else
                   (msg.document.file_id if getattr(msg, "document", None) and getattr(msg.document, "mime_type", "").startswith("image/") else None))
        cal = await msg.reply_text("📅 Seleziona la data per questo turno:", reply_markup=kb)
        PENDING[cal.message_id] = {
            "src_chat_id": msg.chat.id,
            "src_msg_id": msg.message_id,
            "owner_id": owner_id,
            "owner_username": owner_username,
            "caption": caption,
            "file_id": file_id
        }
        return

    # duplicato stessa data stesso autore
    if owner_id and has_open_on_date(owner_id, date_iso):
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await dm_or_prompt_private(
            ctx, owner_id, msg,
            f"⛔ Hai già un turno <b>aperto</b> per il {human}.\n"
            f"Chiudi quello esistente con <b>Risolto</b> oppure usa /miei."
        )
        try: await ctx.bot.delete_message(msg.chat.id, msg.message_id)
        except Exception: pass
        return

    await save_shift(msg, date_iso)
    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await dm_or_prompt_private(ctx, owner_id, msg, f"✅ Turno registrato per il {human}")

# ============== CERCA (DM) ==============
async def search_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    args = ctx.args
    date_iso = parse_date(" ".join(args)) if args else None
    if date_iso:
        await show_shifts(update, ctx, date_iso)
        await ensure_private_menu(ctx, update.effective_chat.id)
    else:
        kb = build_calendar(datetime.today(), mode="SEARCH")
        await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
        await ensure_private_menu(ctx, update.effective_chat.id)

# ============== MIEI (DM) ==============
async def miei_list_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, date_iso, caption, photo_file_id
                   FROM shifts
                   WHERE user_id=? AND status='open'
                   ORDER BY created_at DESC
                   LIMIT 20""", (user_id,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await ctx.bot.send_message(chat_id=user_id, text="Non hai turni aperti al momento.", reply_markup=PRIVATE_KB)
        return

    await ctx.bot.send_message(chat_id=user_id, text="🧾 I tuoi turni aperti (max 20 più recenti):", reply_markup=PRIVATE_KB)

    for sid, src_chat_id, src_msg_id, date_iso, _caption, file_id in rows:
        sent_mid = None
        # prova copia
        try:
            copy_res = await ctx.bot.copy_message(chat_id=user_id, from_chat_id=src_chat_id, message_id=src_msg_id)
            sent_mid = getattr(copy_res, "message_id", copy_res)
        except Exception:
            sent_mid = None
        # fallback con file_id
        if sent_mid is None and file_id:
            try:
                m = await ctx.bot.send_photo(chat_id=user_id, photo=file_id)
                sent_mid = m.message_id
            except Exception:
                sent_mid = None
        # messaggio informativo se manca immagine
        if sent_mid is None:
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            m = await ctx.bot.send_message(chat_id=user_id, text=f"📄 Turno del {human}\n(Immagine non disponibile)")
            sent_mid = m.message_id

        # bottoni sotto allo screenshot: SOLO ✅ Risolto (in reply, con fallback)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Risolto", callback_data=f"CLOSE|{sid}")]])
        await send_buttons_below(ctx, user_id, sent_mid, kb)

async def miei_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    user = update.effective_user
    if user:
        await miei_list_dm(ctx, user.id)

# ============== SHOW & DATES (DM) ==============
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
        await update.effective_message.reply_text("Nessun turno salvato per quella data.", reply_markup=PRIVATE_KB)
        return

    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await update.effective_message.reply_text(
        f"📅 Turni trovati per <b>{human}</b>: {len(rows)}",
        parse_mode="HTML",
        reply_markup=PRIVATE_KB
    )

    for (sid, chat_id, message_id, _user_id, username, _caption, file_id) in rows:
        sent_mid = None

        # 1) prova copia
        try:
            copy_res = await ctx.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=chat_id,
                message_id=message_id
            )
            sent_mid = getattr(copy_res, "message_id", copy_res)
        except Exception:
            sent_mid = None

        # 2) fallback con file_id
        if sent_mid is None and file_id:
            try:
                m = await ctx.bot.send_photo(chat_id=update.effective_chat.id, photo=file_id)
                sent_mid = m.message_id
            except Exception:
                sent_mid = None

        # 3) bottoni: SOLO “📩 Contatta autore” sotto all’immagine (con fallback)
        kb = contact_only_buttons(shift_id=sid)
        await send_buttons_below(ctx, update.effective_chat.id, sent_mid, kb)

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

    lines = ["📆 <b>Date con turni aperti:</b>", ""]
    for date_iso, count in rows:
        d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        lines.append(f"• {d}: {count}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=PRIVATE_KB)

# ============== CALENDARIO ==============
def build_calendar(base_date: datetime, mode="SETDATE", extra="") -> InlineKeyboardMarkup:
    year, month = base_date.year, base_date.month
    first_day = datetime(year, month, 1)
    next_month = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    prev_month = (first_day - timedelta(days=1)).replace(day=1)

    kb = []
    kb.append([InlineKeyboardButton(f"{month:02d}/{year}", callback_data="IGNORE")])
    kb.append([InlineKeyboardButton(d, callback_data="IGNORE") for d in ["L","M","M","G","V","S","D"]])

    week = []
    for _ in range(first_day.weekday()):
        week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))

    day = first_day
    while day.month == month:
        cb = f"{mode}|{day.strftime('%Y-%m-%d')}"
        week.append(InlineKeyboardButton(str(day.day), callback_data=cb))
        if len(week) == 7:
            kb.append(week); week = []
        day += timedelta(days=1)

    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))
        kb.append(week)

    kb.append([
        InlineKeyboardButton("<", callback_data=f"NAV|{mode}|{prev_month.strftime('%Y-%m-%d')}|"),
        InlineKeyboardButton(">", callback_data=f"NAV|{mode}|{next_month.strftime('%Y-%m-%d')}|")
    ])
    return InlineKeyboardMarkup(kb)

# ============== CALLBACK ==============
async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = (query.data or "").split("|")

    if parts[0] == "SETDATE":
        date_iso = parts[1]
        cal_msg_id = query.message.message_id if query.message else None
        data = PENDING.pop(cal_msg_id, None)

        if not data and len(parts) >= 5:
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
                    text=(f"⛔ Hai già un turno <b>aperto</b> per il {human}.\n"
                          f"Chiudi quello esistente con <b>Risolto</b> oppure usa /miei."),
                    parse_mode="HTML", reply_markup=PRIVATE_KB
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
                    ("⛔ Questo turno non è stato salvato: c'è già un tuo turno <b>aperto</b> per quella data.\n"
                     "Apri la chat privata per i dettagli."),
                    reply_markup=kb, parse_mode="HTML"
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
            try: await query.message.delete()
            except Exception: await query.edit_message_reply_markup(reply_markup=None)
        except Forbidden:
            bot_username = ctx.bot.username or "this_bot"
            url = f"https://t.me/{bot_username}?start=start"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
            await query.edit_message_text("✅ Turno registrato. Per conferme, apri la chat con me:", reply_markup=kb)
        return

    elif parts[0] == "SETDATEALBUM":
        group_id = parts[1]
        date_iso = parts[2] if len(parts) > 2 else None
        if not date_iso:
            await query.edit_message_text("❌ Data non valida.")
            return

        g = MEDIA_GROUPS.get(group_id)
        if not g:
            await query.edit_message_text("❌ Album non trovato.")
            return

        g["date"] = date_iso
        owner_id = g["owner_id"]

        if owner_id and has_open_on_date(owner_id, date_iso):
            g["decision"] = "blocked"
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            try:
                await ctx.bot.send_message(
                    chat_id=owner_id,
                    text=(f"⛔ Hai già un turno <b>aperto</b> per il {human}.\n"
                          f"Chiudi quello esistente con <b>Risolto</b> oppure usa /miei."),
                    parse_mode="HTML", reply_markup=PRIVATE_KB
                )
            except Exception:
                pass
            for p in list(g["photos"]):
                try: await ctx.bot.delete_message(p.chat.id, p.message_id)
                except Exception: pass
            try: await query.message.delete()
            except Exception: await query.edit_message_reply_markup(reply_markup=None)
            return

        g["decision"] = "allowed"
        for p in list(g["photos"]):
            await save_shift(p, date_iso)
        if not g["notified"]:
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            try: await ctx.bot.send_message(chat_id=owner_id, text=f"✅ Turno (album) registrato per il {human}", reply_markup=PRIVATE_KB)
            except Exception: pass
            g["notified"] = True

        try: await query.message.delete()
        except Exception: await query.edit_message_reply_markup(reply_markup=None)
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
        # chiudi e RIMUOVI (post gruppo + riga DB)
        try:
            shift_id = int(parts[1])
        except Exception:
            await query.edit_message_text("❌ ID turno non valido.")
            return

        # recupero dati turno
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT chat_id, message_id, user_id, status, date_iso FROM shifts WHERE id=?", (shift_id,))
        row = cur.fetchone()
        conn.close()

        if not row:
            await query.edit_message_text("❌ Turno non trovato (forse già rimosso).")
            return

        grp_chat_id, grp_msg_id, owner_id, status, date_iso = row

        # permessi: proprietario o admin
        user = update.effective_user
        is_admin = await is_user_admin(update, ctx, user.id) if user else False
        if not user or (user.id != owner_id and not is_admin):
            await query.answer("Non hai i permessi per chiudere questo turno.", show_alert=True)
            return

        # elimina messaggio nel gruppo (se ancora presente)
        try:
            await ctx.bot.delete_message(chat_id=grp_chat_id, message_id=grp_msg_id)
        except Exception:
            pass  # magari già cancellato

        # elimina dal DB
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM shifts WHERE id=?", (shift_id,))
        conn.commit()
        conn.close()

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await query.edit_message_text(f"✅ Turno <b>Risolto</b> e rimosso ({human}).", parse_mode="HTML")

    elif parts[0] == "CONTACT":
        try:
            shift_id = int(parts[1])
        except Exception:
            await query.answer("ID turno non valido.", show_alert=True)
            return

        # recupera il messaggio originale per inoltrarlo in DM
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""SELECT chat_id, message_id, user_id, username, photo_file_id FROM shifts WHERE id=?""", (shift_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            await query.answer("Turno non trovato.", show_alert=True)
            return

        src_chat_id, src_msg_id, owner_id, owner_username, file_id = row
        requester = update.effective_user
        if not requester:
            await query.answer("Errore utente.", show_alert=True)
            return

        # 1) screenshot in DM al richiedente
        sent = False
        try:
            await ctx.bot.copy_message(chat_id=requester.id, from_chat_id=src_chat_id, message_id=src_msg_id)
            sent = True
        except Forbidden:
            bot_username = ctx.bot.username or "this_bot"
            url_bot = f"https://t.me/{bot_username}?start=start"
            kb_dm = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat con il bot", url=url_bot)]])
            await query.message.reply_text("Per contattare l’autore apri prima la chat privata con me:", reply_markup=kb_dm)
            return
        except Exception:
            sent = False

        if not sent and file_id:
            try:
                await ctx.bot.send_photo(chat_id=requester.id, photo=file_id)
                sent = True
            except Exception:
                sent = False

        # 2) pulsante per aprire chat con autore + messaggio pronto
        if owner_username and owner_username.startswith("@") and len(owner_username) > 1:
            url_author = f"https://t.me/{owner_username[1:]}"
            label = f"Apri chat con {owner_username}"
        else:
            url_author = f"tg://user?id={owner_id}"
            label = "Apri chat con l’autore"

        prompt = "Ciao 👋 , questo servizio è ancora disponibile ?"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"💬 {label}", url=url_author)]])
        await ctx.bot.send_message(
            chat_id=requester.id,
            text=("Tocca il pulsante per aprire la chat con l’autore.\n"
                  "Poi incolla questo messaggio:\n\n"
                  f"{prompt}"),
            reply_markup=kb
        )

        await query.message.reply_text("✅ Ti ho inviato in privato lo screenshot e il pulsante per scrivere all’autore.")

# ============== ROUTER DM TESTI ==============
async def private_text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    text = (update.effective_message.text or "").strip().lower()
    if text in ("/cerca", "cerca"):
        await search_cmd(update, ctx); return
    if text in ("/date", "date"):
        await dates_cmd(update, ctx); return
    if text in ("/miei", "i miei turni", "miei"):
        await miei_cmd(update, ctx); return
    await update.effective_message.reply_text("Usa i pulsanti qui sotto 👇", reply_markup=PRIVATE_KB)

# ============== CALENDARIO ==============
def build_calendar(base_date: datetime, mode="SETDATE", extra="") -> InlineKeyboardMarkup:
    year, month = base_date.year, base_date.month
    first_day = datetime(year, month, 1)
    next_month = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    prev_month = (first_day - timedelta(days=1)).replace(day=1)

    kb = []
    kb.append([InlineKeyboardButton(f"{month:02d}/{year}", callback_data="IGNORE")])
    kb.append([InlineKeyboardButton(d, callback_data="IGNORE") for d in ["L","M","M","G","V","S","D"]])

    week = []
    for _ in range(first_day.weekday()):
        week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))

    day = first_day
    while day.month == month:
        cb = f"{mode}|{day.strftime('%Y-%m-%d')}"
        week.append(InlineKeyboardButton(str(day.day), callback_data=cb))
        if len(week) == 7:
            kb.append(week); week = []
        day += timedelta(days=1)

    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))
        kb.append(week)

    kb.append([
        InlineKeyboardButton("<", callback_data=f"NAV|{mode}|{prev_month.strftime('%Y-%m-%d')}|"),
        InlineKeyboardButton(">", callback_data=f"NAV|{mode}|{next_month.strftime('%Y-%m-%d')}|")
    ])
    return InlineKeyboardMarkup(kb)

# ============== MAIN ========================
def main():
    if not TOKEN:
        raise SystemExit("Errore: variabile d'ambiente TELEGRAM_BOT_TOKEN mancante.")

    ensure_db()
    app = ApplicationBuilder().token(TOKEN).build()

    # Guardiano comandi nel gruppo
    app.add_handler(MessageHandler(filters.COMMAND, group_command_guard), group=0)

    # Comandi
    app.add_handler(CommandHandler("start", start), group=1)
    app.add_handler(CommandHandler("help", help_cmd), group=1)
    app.add_handler(CommandHandler("version", version_cmd), group=1)
    app.add_handler(CommandHandler("cerca", search_cmd), group=1)
    app.add_handler(CommandHandler("date", dates_cmd), group=1)
    app.add_handler(CommandHandler("miei", miei_cmd), group=1)

    # Immagini (foto + document image)
    img_doc_filter = filters.Document.IMAGE if hasattr(filters.Document, "IMAGE") else filters.Document.MimeType("image/")
    app.add_handler(MessageHandler(filters.PHOTO | img_doc_filter, photo_or_doc_image_handler), group=1)

    # Inline callbacks
    app.add_handler(CallbackQueryHandler(button_handler), group=1)

    # Benvenuto nuovi membri
    app.add_handler(ChatMemberHandler(welcome_new_member, ChatMemberHandler.CHAT_MEMBER), group=1)

    # Router testi in DM
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), private_text_router), group=2)

    print("ShiftBot avviato. Premi Ctrl+C per uscire.")
    app.run_polling()

if __name__ == "__main__":
    main()