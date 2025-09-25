#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShiftBot – Gestione cambi turni su Telegram
Versione: 3.5
- NEW: /cerca in gruppo → risponde in privato (DM) con calendario/risultati.
  * Se l'utente non ha mai aperto il DM col bot, mostra un deep-link "Apri chat privata".
- Stato turni: open/closed (+ chiusura via bottone)
- /miei: elenca i tuoi turni open
- Contact fix: bottone se @handle, altrimenti menzione tg://user?id=...
"""

import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Tuple

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Message
from telegram.constants import ChatType
from telegram.error import Forbidden
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler, ChatMemberHandler
)

VERSION = "ShiftBot 3.5"
DB_PATH = os.environ.get("SHIFTBOT_DB", "shiftbot.sqlite3")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

WELCOME_TEXT = (
    "👋 Benvenuto/a nel gruppo *Cambi Servizi*!\n\n"
    "Per caricare i turni:\n"
    "• Invia l’immagine del turno con una breve descrizione (es. data, note)\n\n"
    "Per cercare i turni:\n"
    "• `/cerca` → apre il calendario (in privato)\n"
    "• `/date` → elenco date con turni aperti\n"
    "• `/miei` → i tuoi turni aperti (chiudili da lì)\n"
    "• `/version` → versione del bot\n"
)

DATE_PATTERNS = [
    r'(?P<d>\d{1,2})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<y>\d{4})',
    r'(?P<y>\d{4})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<d>\d{1,2})',
]

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
            username TEXT,                 -- @handle oppure display name
            date_iso TEXT NOT NULL,        -- YYYY-MM-DD
            caption TEXT,
            photo_file_id TEXT,
            status TEXT DEFAULT 'open',    -- 'open' | 'closed'
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    # migrazione: aggiungi colonna status se manca
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

# ============== CONTATTO: bottone o menzione ==============
def contact_payload(user_id: Optional[int], username: Optional[str]) -> Tuple[str, Optional[InlineKeyboardMarkup], str]:
    if username and isinstance(username, str) and username.startswith("@") and len(username) > 1:
        handle = username[1:]
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contatta autore", url=f"https://t.me/{handle}")]])
        return ("📩 Contatta l’autore del turno:", kb, "Markdown")
    else:
        if user_id:
            link = f'<a href="tg://user?id={user_id}">📩 Contatta autore</a>'
            return (link, None, "HTML")
        else:
            return ("📩 Contatta autore", None, "Markdown")

def close_button(shift_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Segna scambiato", callback_data=f"CLOSE|{shift_id}")]])

# ============== PERMESSI ==============
async def is_user_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    try:
        cm = await ctx.bot.get_chat_member(update.effective_chat.id, user_id)
        return cm.status in ("administrator", "creator")
    except Exception:
        return False

# ============== HANDLERS BASE ==============
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /start in privato:
    - senza parametri → benvenuto
    - con deep-link: /start search        → calendario
                      /start search-YYYY-MM-DD → risultati di quella data
    """
    # deep-link payload (args dopo /start)
    payload = None
    if update.message and update.message.text:
        parts = update.message.text.split(maxsplit=1)
        if len(parts) > 1:
            payload = parts[1].strip()

    if update.effective_chat.type in (ChatType.PRIVATE,):
        if payload:
            if payload.startswith("search"):
                # se ha la data: search-YYYY-MM-DD
                date_iso = None
                if "-" in payload:
                    try:
                        maybe = payload.split("search-", 1)[1]
                        # accettiamo solo formato ISO, es. 2025-10-12
                        datetime.strptime(maybe, "%Y-%m-%d")
                        date_iso = maybe
                    except Exception:
                        date_iso = None
                if date_iso:
                    # mostra risultati in privato
                    fake_update = update  # possiamo riusare update
                    await show_shifts(fake_update, ctx, date_iso)
                else:
                    # mostra calendario in privato
                    kb = build_calendar(datetime.today(), mode="SEARCH")
                    await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
                return

        # nessun payload specifico → benvenuto
        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")
        return

    # se qualcuno usa /start nel gruppo, mandiamo il benvenuto lì
    await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await start(update, ctx)

async def version_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(VERSION)

async def testbtn_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    username = f"@{u.username}" if (u and u.username) else None
    txt, kb, pm = contact_payload(u.id if u else None, username)
    await update.effective_message.reply_text(txt, reply_markup=kb, parse_mode=pm)

async def welcome_new_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chm = update.chat_member
    if chm.old_chat_member.status in ("left", "kicked") and chm.new_chat_member.status == "member":
        await ctx.bot.send_message(chat_id=chm.chat.id, text=WELCOME_TEXT, parse_mode="Markdown")

# ============== FOTO/DOC ==============
async def photo_or_doc_image_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    caption = (msg.caption or "").strip()
    date_iso = parse_date(caption)

    if not date_iso:
        extra = f"{msg.chat.id}|{msg.message_id}|{msg.from_user.id if msg.from_user else 0}"
        kb = build_calendar(datetime.today(), mode="SETDATE", extra=extra)
        await msg.reply_text("📅 Seleziona la data per questo turno:", reply_markup=kb)
        return

    new_id = await save_shift(msg, date_iso)

    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await update.effective_message.reply_text(f"✅ Turno registrato per il {human}")

    u = msg.from_user
    username = f"@{u.username}" if (u and u.username) else None
    txt, kb, pm = contact_payload(u.id if u else None, username)
    await ctx.bot.send_message(chat_id=msg.chat.id, text=txt, reply_markup=kb, parse_mode=pm)

    await ctx.bot.send_message(
        chat_id=msg.chat.id,
        text="Quando il cambio è concluso:",
        reply_markup=close_button(new_id)
    )

async def save_shift(msg: Message, date_iso: str) -> int:
    username = ""
    if msg.from_user:
        username = f"@{msg.from_user.username}" if msg.from_user.username else msg.from_user.full_name

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO shifts(chat_id, message_id, user_id, username, date_iso, caption, photo_file_id, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'open')""",
        (msg.chat.id, msg.message_id,
         msg.from_user.id if msg.from_user else None,
         username, date_iso, msg.caption or "", None)
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id

# ============== CERCA (DM-first) ==============
async def search_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /cerca:
    - In gruppo/supergruppo → prova a rispondere in PRIVATO (DM).
      Se l'utente non ha mai avviato il DM → mostra bottone deep-link per aprire il DM.
    - In privato → comportamento normale (calendario o ricerca diretta).
    """
    chat = update.effective_chat
    user = update.effective_user
    args = ctx.args
    bot_username = ctx.bot.username or "this_bot"

    # Parsing data opzionale
    date_iso = None
    if args:
        query_date = " ".join(args)
        date_iso = parse_date(query_date)

    if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        # tenta DM
        try:
            if date_iso:
                # invia risultati in DM
                await ctx.bot.send_message(chat_id=user.id, text="🔍 Sto cercando i turni…")
                # costruiamo un finto update per riusare show_shifts
                fake_update = Update(update.update_id, message=update.effective_message)
                # ma mostriamo in DM: quindi usiamo un messaggio DM d'appoggio
                await show_shifts_dm(ctx, user.id, date_iso)
                await update.effective_message.reply_text("📬 Ti ho scritto in privato con i risultati.")
            else:
                kb = build_calendar(datetime.today(), mode="SEARCH")
                await ctx.bot.send_message(chat_id=user.id, text="📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
                await update.effective_message.reply_text("📬 Ti ho scritto in privato con il calendario.")
            return
        except Forbidden:
            # l'utente non ha mai aperto il DM col bot → deep-link
            payload = f"search-{date_iso}" if date_iso else "search"
            url = f"https://t.me/{bot_username}?start={payload}"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata", url=url)]])
            await update.effective_message.reply_text(
                "Per motivi di privacy, apri la chat privata con il bot e ti mostrerò il calendario/risultati lì:",
                reply_markup=kb
            )
            return

    # In privato:
    if date_iso:
        await show_shifts(update, ctx, date_iso)
    else:
        kb = build_calendar(datetime.today(), mode="SEARCH")
        await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)

async def show_shifts_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int, date_iso: str):
    """Mostra i risultati di una data direttamente nel DM (senza usare update.chat)."""
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
            await ctx.bot.copy_message(
                chat_id=user_id,
                from_chat_id=chat_id,
                message_id=message_id
            )
        except Exception:
            pass

        # contatto autore
        txt, kb, pm = contact_payload(owner_id, username)
        if caption and kb is None and pm == "HTML":
            await ctx.bot.send_message(chat_id=user_id, text=f"{caption}\n{txt}", parse_mode=pm)
        else:
            await ctx.bot.send_message(chat_id=user_id, text=txt, reply_markup=kb, parse_mode=pm)

        # bottone chiusura: nel DM ha senso solo informativo (non può chiudere da qui se non c'è contesto chat originale),
        # quindi lo omettiamo nel DM per evitare confusione.

# ============== ALTRI COMANDI ==============
async def miei_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, date_iso, caption
                   FROM shifts
                   WHERE user_id=? AND status='open'
                   ORDER BY created_at DESC
                   LIMIT 20""", (user.id,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.effective_message.reply_text("Non hai turni aperti al momento.")
        return

    await update.effective_message.reply_text("🧾 I tuoi turni aperti (max 20 più recenti):")
    for sid, chat_id, message_id, date_iso, caption in rows:
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        try:
            await ctx.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=chat_id,
                message_id=message_id
            )
        except Exception:
            pass
        await update.effective_message.reply_text(f"📅 {human}\n{caption or ''}".strip(),
                                                  reply_markup=close_button(sid))

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

        txt, kb, pm = contact_payload(user_id, username)
        if caption and kb is None and pm == "HTML":
            await update.effective_message.reply_text(f"{caption}\n{txt}", parse_mode=pm)
        else:
            await update.effective_message.reply_text(txt, reply_markup=kb, parse_mode=pm)

        await update.effective_message.reply_text("Segna come scambiato quando chiuso:",
                                                  reply_markup=close_button(sid))

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
        cb = f"{mode}|{day.strftime('%Y-%m-%d')}"
        if extra:
            cb += "|" + extra
        week.append(InlineKeyboardButton(str(day.day), callback_data=cb))
        if len(week) == 7:
            keyboard.append(week); week = []
        day += timedelta(days=1)

    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="IGNORE"))
        keyboard.append(week)

    keyboard.append([
        InlineKeyboardButton("<", callback_data=f"NAV|{mode}|{prev_month.strftime('%Y-%m-%d')}|{extra}"),
        InlineKeyboardButton(">", callback_data=f"NAV|{mode}|{next_month.strftime('%Y-%m-%d')}|{extra}")
    ])
    return InlineKeyboardMarkup(keyboard)

# ============== CALLBACK INLINE ==============
async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = (query.data or "").split("|")

    if parts[0] == "SETDATE":
        # SETDATE|YYYY-MM-DD|chat_id|message_id|user_id
        date_iso = parts[1]
        chat_id = int(parts[2]) if len(parts) > 2 else None
        message_id = int(parts[3]) if len(parts) > 3 else None
        user_id = int(parts[4]) if len(parts) > 4 else None

        fake = type("obj", (), {})()
        fake.chat = type("c", (), {"id": chat_id})
        fake.message_id = message_id
        fake.caption = ""
        fake.photo = None
        fake.from_user = type("u", (), {"id": user_id, "username": None, "full_name": "Utente"})

        new_id = await save_shift(fake, date_iso)

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await query.edit_message_text(f"✅ Turno registrato per il {human}")

        txt, kb, pm = contact_payload(user_id, None)
        await ctx.bot.send_message(chat_id=update.effective_chat.id, text=txt, reply_markup=kb, parse_mode=pm)
        await ctx.bot.send_message(chat_id=update.effective_chat.id,
                                   text="Quando il cambio è concluso:",
                                   reply_markup=close_button(new_id))

    elif parts[0] == "SEARCH":
        date_iso = parts[1]
        fake_update = Update(update.update_id, message=query.message)
        await show_shifts(fake_update, ctx, date_iso)
        await query.edit_message_text(f"📅 Risultati mostrati per {datetime.strptime(date_iso, '%Y-%m-%d').strftime('%d/%m/%Y')}")

    elif parts[0] == "NAV":
        mode = parts[1]
        new_month = datetime.strptime(parts[2], "%Y-%m-%d")
        extra = parts[3] if len(parts) > 3 else ""
        kb = build_calendar(new_month, mode, extra)
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

# ============== MAIN ==============
def main():
    if not TOKEN:
        raise SystemExit("Errore: variabile d'ambiente TELEGRAM_BOT_TOKEN mancante.")

    ensure_db()
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("testbtn", testbtn_cmd))  # test rapido opzionale
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
