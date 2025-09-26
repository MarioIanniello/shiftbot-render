#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShiftBot – Gestione cambi turni su Telegram
Versione: 3.7.1
- Blocco doppio inserimento: un utente non può avere 2 turni "aperti" nella stessa data
- In caso di duplicato: DM di avviso + rimozione dello screenshot dal gruppo
- Messaggi di conferma/errore in DM; nel gruppo solo bottone per aprire la chat se il DM è bloccato
- /cerca e /miei in privato (deep-link se necessario)
- Salvataggio robusto dopo scelta data (mappa PENDING)
- "Contatta autore": DM all’autore con screenshot + messaggio
- Stato turni open/closed + chiusura
- Benvenuto ai nuovi membri
"""

import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Message
from telegram.constants import ChatType
from telegram.error import Forbidden
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler, ChatMemberHandler
)

VERSION = "ShiftBot 3.7.1"
DB_PATH = os.environ.get("SHIFTBOT_DB", "shiftbot.sqlite3")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

WELCOME_TEXT = (
    "👋 Benvenuto/a nel gruppo *Cambi Servizi*!\n\n"
    "Per caricare i turni:\n"
    "• Invia l’immagine del turno con una breve descrizione (es. Cambio per mattina, Cambio per intermedia, Cambio per pomeriggio)\n\n"
    "Per cercare i turni:\n"
    "• `/cerca` → apre il calendario (in privato)\n"
    "• `/date` → elenco date con turni aperti\n"
    "• `/miei` → i tuoi turni aperti (risposta in privato)\n"
    "• `/version` → versione del bot\n"
)

DATE_PATTERNS = [
    r'(?P<d>\d{1,2})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<y>\d{4})',
    r'(?P<y>\d{4})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<d>\d{1,2})',
]

# ====== Stato in memoria (resettato a ogni riavvio) ======
PENDING: Dict[int, Dict[str, Any]] = {}  # key = calendar_message_id

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
    cur.execute("PRAGMA table_info(shifts);")
    cols = [r[1] for r in cur.fetchall()]
    if "status" not in cols:
        try:
            cur.execute("ALTER TABLE shifts ADD COLUMN status TEXT DEFAULT 'open';")
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

# ============== UTILS CONTATTO & DM ==============
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
    """Prova a mandare un DM, altrimenti in gruppo mostra solo il bottone per aprire il DM."""
    try:
        await ctx.bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown")
    except Forbidden:
        bot_username = ctx.bot.username or "this_bot"
        url = f"https://t.me/{bot_username}?start=start"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
        await group_message.reply_text("ℹ️ Non posso scriverti in privato finché non apri la chat con me:", reply_markup=kb)

# ============== PERMESSI ==============
async def is_user_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        cm = await ctx.bot.get_chat_member(update.effective_chat.id, user_id)
        return cm.status in ("administrator", "creator")
    except Exception:
        return False

# ============== CHECK DUPLICATI ==============
def has_open_on_date(user_id: int, date_iso: str) -> bool:
    """True se l'utente ha già almeno un turno OPEN nella stessa data."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT 1 FROM shifts
                   WHERE user_id=? AND date_iso=? AND status='open'
                   LIMIT 1""", (user_id, date_iso))
    row = cur.fetchone()
    conn.close()
    return row is not None

# ============== SALVATAGGIO ==============
def save_shift_raw(chat_id: int, message_id: int, user_id: Optional[int],
                   username: Optional[str], caption: str, date_iso: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO shifts(chat_id, message_id, user_id, username, date_iso, caption, photo_file_id, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'open')""",
        (chat_id, message_id, user_id, (username or ""), date_iso, caption or "", None)
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id

async def save_shift(msg: Message, date_iso: str) -> int:
    username = ""
    if msg.from_user:
        username = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.full_name
    return save_shift_raw(
        chat_id=msg.chat.id,
        message_id=msg.message_id,
        user_id=(msg.from_user.id if msg.from_user else None),
        username=username,
        caption=(msg.caption or ""),
        date_iso=date_iso
    )

# ============== HANDLERS BASE ==============
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # deep-link: /start search | /start search-YYYY-MM-DD | /start miei
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
                return
            if payload == "miei" and update.effective_user:
                await miei_list_dm(ctx, update.effective_user.id)
                return

        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")
        return

    await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await start(update, ctx)

async def version_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(VERSION)

async def welcome_new_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Saluta automaticamente chi entra nel gruppo."""
    chm = update.chat_member
    try:
        old = chm.old_chat_member.status
        new = chm.new_chat_member.status
    except Exception:
        return
    if old in ("left", "kicked") and new in ("member", "restricted"):
        await ctx.bot.send_message(chat_id=chm.chat.id, text=WELCOME_TEXT, parse_mode="Markdown")

# ============== FOTO/DOC ==============
async def photo_or_doc_image_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    caption = (msg.caption or "").strip()
    date_iso = parse_date(caption)

    if not date_iso:
        kb = build_calendar(datetime.today(), mode="SETDATE")
        cal = await msg.reply_text("📅 Seleziona la data per questo turno:", reply_markup=kb)
        PENDING[cal.message_id] = {
            "src_chat_id": msg.chat.id,
            "src_msg_id": msg.message_id,
            "owner_id": (msg.from_user.id if msg.from_user else None),
            "owner_username": (f"@{msg.from_user.username}" if msg.from_user and msg.from_user.username else (msg.from_user.full_name if msg.from_user else "")),
            "caption": caption,
        }
        return

    # ---- BLOCCO DOPPIONE: stesso utente, stessa data, open ----
    owner_id = msg.from_user.id if msg.from_user else None
    if owner_id and has_open_on_date(owner_id, date_iso):
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await dm_or_prompt_private(
            ctx, owner_id, msg,
            f"⛔ Hai già un turno *aperto* per il {human}.\n"
            f"Chiudi quello esistente con *Segna scambiato* oppure usa /miei per gestirli."
        )
        # rimuovi lo screenshot dal gruppo
        try:
            await ctx.bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
        except Exception:
            pass
        return

    # Salvataggio e conferma SOLO in privato
    await save_shift(msg, date_iso)
    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await dm_or_prompt_private(ctx, owner_id, msg, f"✅ Turno registrato per il {human}")

# ============== CERCA (DM-first) ==============
async def search_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    args = ctx.args
    bot_username = ctx.bot.username or "this_bot"

    date_iso = None
    if args:
        query_date = " ".join(args)
        date_iso = parse_date(query_date)

    if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        try:
            if date_iso:
                await ctx.bot.send_message(chat_id=user.id, text="🔍 Sto cercando i turni…")
                await show_shifts_dm(ctx, user.id, date_iso)
                await update.effective_message.reply_text("📬 Ti ho scritto in privato con i risultati.")
            else:
                kb = build_calendar(datetime.today(), mode="SEARCH")
                await ctx.bot.send_message(chat_id=user.id, text="📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
                await update.effective_message.reply_text("📬 Ti ho scritto in privato con il calendario.")
            return
        except Forbidden:
            payload = f"search-{date_iso}" if date_iso else "search"
            url = f"https://t.me/{bot_username}?start={payload}"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]])
            await update.effective_message.reply_text(
                "Per motivi di privacy, apri la chat privata con il bot e ti mostrerò il calendario/risultati lì:",
                reply_markup=kb
            )
            return

    if date_iso:
        await show_shifts(update, ctx, date_iso)
    else:
        kb = build_calendar(datetime.today(), mode="SEARCH")
        await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)

async def show_shifts_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int, date_iso: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, user_id, username, caption
                   FROM shifts
                   WHERE date_iso=? AND status='open'
                   ORDER BY created_at ASC""", (date_iso,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await ctx.bot.send_message(chat_id=user_id, text="Nessun turno salvato per quella data.")
        return

    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await ctx.bot.send_message(chat_id=user_id, text=f"📅 Turni trovati per *{human}*: {len(rows)}", parse_mode="Markdown")

    for (sid, chat_id, message_id, owner_id, username, caption) in rows:
        try:
            await ctx.bot.copy_message(chat_id=user_id, from_chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
        kb = contact_buttons(sid, username if username and username.startswith("@") else None)
        info = f"{caption}\n" if caption else ""
        await ctx.bot.send_message(chat_id=user_id, text=info + "Azioni:", reply_markup=kb)

# ============== /MIEI: DM-first ==============
async def miei_list_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, date_iso, caption
                   FROM shifts
                   WHERE user_id=? AND status='open'
                   ORDER BY created_at DESC
                   LIMIT 20""", (user_id,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await ctx.bot.send_message(chat_id=user_id, text="Non hai turni aperti al momento.")
        return

    await ctx.bot.send_message(chat_id=user_id, text="🧾 I tuoi turni aperti (max 20 più recenti):")
    for sid, chat_id, message_id, date_iso, caption in rows:
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        try:
            await ctx.bot.copy_message(chat_id=user_id, from_chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
        await ctx.bot.send_message(
            chat_id=user_id,
            text=f"📅 {human}\n{caption or ''}".strip(),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{sid}"),
                 InlineKeyboardButton("✅ Segna scambiato", callback_data=f"CLOSE|{sid}")]
            ])
        )

async def miei_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    # In gruppo → rispondi in privato
    if update.effective_chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        try:
            await ctx.bot.send_message(chat_id=user.id, text="📬 Ti invio in privato i tuoi turni aperti…")
            await miei_list_dm(ctx, user.id)
            await update.effective_message.reply_text("📬 Ti ho scritto in privato con i tuoi turni.")
        except Forbidden:
            bot_username = ctx.bot.username or "this_bot"
            url = f"https://t.me/{bot_username}?start=miei"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]])
            await update.effective_message.reply_text(
                "⚠️ Per mostrarti i tuoi turni devo scriverti in privato.\n"
                "Clicca qui sotto per aprire la chat con me:",
                reply_markup=kb
            )
        return

    # In privato → mostra direttamente
    await miei_list_dm(ctx, user.id)

# ============== SHOW & DATES ==============
async def show_shifts(update: Update, ctx: ContextTypes.DEFAULT_TYPE, date_iso: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, user_id, username, caption
                   FROM shifts
                   WHERE date_iso=? AND status='open'
                   ORDER BY created_at ASC""", (date_iso,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.effective_message.reply_text("Nessun turno salvato per quella data.")
        return

    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await update.effective_message.reply_text(f"📅 Turni trovati per *{human}*: {len(rows)}", parse_mode="Markdown")

    for (sid, chat_id, message_id, user_id, username, caption) in rows:
        try:
            await ctx.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=chat_id,
                message_id=message_id
            )
        except Exception:
            pass

        btns = [
            InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{sid}"),
            InlineKeyboardButton("✅ Segna scambiato", callback_data=f"CLOSE|{sid}")
        ]
        if username and isinstance(username, str) and username.startswith("@") and len(username) > 1:
            handle = username[1:]
            btns.insert(1, InlineKeyboardButton("👤 Profilo autore", url=f"https://t.me/{handle}"))

        txt = (caption or "").strip()
        if txt:
            await update.effective_message.reply_text(txt)
        await update.effective_message.reply_text("Azioni:", reply_markup=InlineKeyboardMarkup([btns]))

async def dates_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT date_iso, COUNT(*) FROM shifts
                   WHERE status='open'
                   GROUP BY date_iso ORDER BY date_iso ASC""")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.effective_message.reply_text("Non ci sono turni aperti al momento.")
        return

    lines = ["📆 *Date con turni aperti:*", ""]
    for date_iso, count in rows:
        d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        lines.append(f"• {d}: {count}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

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
        cb = f"{mode}|{day.strftime('%Y-%m-%d')}"  # niente extra
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

        # Fallback legacy: SETDATE|date|chat|msg|user
        if (not data) and len(parts) >= 5:
            try:
                data = {
                    "src_chat_id": int(parts[2]),
                    "src_msg_id": int(parts[3]),
                    "owner_id": int(parts[4]),
                    "owner_username": "",
                    "caption": "",
                }
            except Exception:
                data = None

        if not data:
            await query.edit_message_text("❌ Non riesco a collegare questo calendario al post originale. Rimanda la foto.")
            return

        # ---- BLOCCO DOPPIONE ----
        owner_id = data["owner_id"]
        if owner_id and has_open_on_date(owner_id, date_iso):
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            try:
                await ctx.bot.send_message(
                    chat_id=owner_id,
                    text=(f"⛔ Hai già un turno *aperto* per il {human}.\n"
                          f"Chiudi quello esistente con *Segna scambiato* oppure usa /miei per gestirli."),
                    parse_mode="Markdown"
                )
                # rimuovi lo screenshot originale dal gruppo
                try:
                    await ctx.bot.delete_message(chat_id=data["src_chat_id"], message_id=data["src_msg_id"])
                except Exception:
                    pass
                # pulizia del calendario
                try:
                    await query.message.delete()
                except Exception:
                    await query.edit_message_reply_markup(reply_markup=None)
            except Forbidden:
                bot_username = ctx.bot.username or "this_bot"
                url = f"https://t.me/{bot_username}?start=start"
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata con il bot", url=url)]])
                # prova comunque a togliere il messaggio nel gruppo
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

        # Salva
        save_shift_raw(
            chat_id=data["src_chat_id"],
            message_id=data["src_msg_id"],
            user_id=owner_id,
            username=data.get("owner_username", ""),
            caption=data.get("caption", ""),
            date_iso=date_iso
        )

        # Invio conferma SOLO in privato
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        try:
            await ctx.bot.send_message(chat_id=owner_id, text=f"✅ Turno registrato per il {human}")
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
        cur.execute("SELECT user_id, status, date_iso FROM shifts WHERE id=?", (shift_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            await query.edit_message_text("❌ Turno non trovato (forse già rimosso).")
            return
        owner_id, status, date_iso = row

        user = update.effective_user
        is_admin = await is_user_admin(update, ctx, user.id) if user else False
        if not user or (user.id != owner_id and not is_admin):
            conn.close()
            await query.answer("Non hai i permessi per chiudere questo turno.", show_alert=True)
            return

        if status == "closed":
            conn.close()
            await query.edit_message_text("ℹ️ Turno già segnato come scambiato.")
            return

        cur.execute("UPDATE shifts SET status='closed' WHERE id=?", (shift_id,))
        conn.commit()
        conn.close()

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await query.edit_message_text(f"✅ Turno segnato come *scambiato* ({human}).", parse_mode="Markdown")

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

# ============== MAIN ==============
def main():
    if not TOKEN:
        raise SystemExit("Errore: variabile d'ambiente TELEGRAM_BOT_TOKEN mancante.")

    ensure_db()
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("cerca", search_cmd))
    app.add_handler(CommandHandler("date", dates_cmd))
    app.add_handler(CommandHandler("miei", miei_cmd))

    img_doc_filter = filters.Document.IMAGE if hasattr(filters.Document, "IMAGE") else filters.Document.MimeType("image/")
    app.add_handler(MessageHandler(filters.PHOTO | img_doc_filter, photo_or_doc_image_handler))

    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(ChatMemberHandler(welcome_new_member, ChatMemberHandler.CHAT_MEMBER))

    print("ShiftBot avviato. Premi Ctrl+C per uscire.")
    app.run_polling()

if __name__ == "__main__":
    main()
