#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShiftBot – Gestione cambi turni su Telegram
Versione: 3.6
- /cerca risponde in privato (deep-link se necessario)
- Stato turni open/closed + chiusura
- NUOVO: "📩 Contatta autore" invia in DM all'autore lo screenshot e la frase:
         "Ciao, questo turno è ancora disponibile?" con menzione del richiedente.
         Se l'autore non ha DM aperto col bot → avviso e link profilo autore al richiedente.
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

VERSION = "ShiftBot 3.6"
DB_PATH = os.environ.get("SHIFTBOT_DB", "shiftbot.sqlite3")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

WELCOME_TEXT = (
    "👋 Benvenuto/a nel gruppo *Cambi Servizi*!\n\n"
    "Per caricare i turni:\n"
    "• Invia l’immagine del turno con una breve descrizione (es. Cambio per mattina, Cambio per interm. , Cambio per pomeriggio)\n\n"
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
            username TEXT,
            date_iso TEXT NOT NULL,
            caption TEXT,
            photo_file_id TEXT,
            status TEXT DEFAULT 'open',
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

# ============== UTILS CONTATTO ==============
def mention_html(user_id: Optional[int], username: Optional[str]) -> str:
    """Ritorna una menzione HTML sicura per il richiedente."""
    if username and isinstance(username, str) and username.startswith("@") and len(username) > 1:
        return username
    if user_id:
        return f'<a href="tg://user?id={user_id}">utente</a>'
    return "utente"

def contact_buttons(shift_id: int, owner_username: Optional[str]) -> InlineKeyboardMarkup:
    """Pulsanti: contatto assistito + (opzionale) apri profilo autore."""
    row = [InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{shift_id}")]
    if owner_username and owner_username.startswith("@") and len(owner_username) > 1:
        handle = owner_username[1:]
        row.append(InlineKeyboardButton("👤 Profilo autore", url=f"https://t.me/{handle}"))
    return InlineKeyboardMarkup([row])

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
    # deep-link per /start search o /start search-YYYY-MM-DD
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
                    fake_update = update
                    await show_shifts(fake_update, ctx, date_iso)
                else:
                    kb = build_calendar(datetime.today(), mode="SEARCH")
                    await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)
                return
        await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")
        return

    await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await start(update, ctx)

async def version_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(VERSION)

async def testbtn_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("Test OK")

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

    # pulsanti sotto al post: contatto + chiusura
    owner_username = f"@{msg.from_user.username}" if (msg.from_user and msg.from_user.username) else (msg.from_user.full_name if msg.from_user else "")
    await ctx.bot.send_message(
        chat_id=msg.chat.id,
        text="Azioni:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{new_id}"),
             InlineKeyboardButton("✅ Segna scambiato", callback_data=f"CLOSE|{new_id}")]
        ])
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

    # in privato
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
        # pulsanti: contatto assistito + profilo autore (se esiste handle)
        kb = contact_buttons(sid, username if username and username.startswith("@") else None)
        info = f"{caption}\n" if caption else ""
        await ctx.bot.send_message(chat_id=user_id, text=info + "Azioni:", reply_markup=kb)

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
        await update.effective_message.reply_text(
            f"📅 {human}\n{caption or ''}".strip(),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{sid}"),
                 InlineKeyboardButton("✅ Segna scambiato", callback_data=f"CLOSE|{sid}")]
            ])
        )

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

    for (sid, chat_id, message_id, owner_id, username, caption) in rows:
        try:
            await ctx.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=chat_id,
                message_id=message_id
            )
        except Exception:
            pass

        # pulsanti contatto assistito + profilo + chiudi
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

        await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Azioni:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{new_id}"),
                 InlineKeyboardButton("✅ Segna scambiato", callback_data=f"CLOSE|{new_id}")]
            ])
        )

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
        # chiusura turno
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
        # contatto assistito: inoltra al proprietario il post + messaggio standard
        try:
            shift_id = int(parts[1])
        except Exception:
            await query.answer("ID turno non valido.", show_alert=True)
            return

        # prendi dati turno
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

        # prova invio DM all'autore
        try:
            # 1) copia lo screenshot/originale
            await ctx.bot.copy_message(chat_id=owner_id, from_chat_id=src_chat_id, message_id=src_msg_id)
            # 2) invia il testo con menzione del richiedente + domanda
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y") if date_iso else ""
            text = f"{requester_name} ti ha scritto riguardo al tuo turno del *{human}*.\n\n" \
                   f"**Ciao, questo turno è ancora disponibile?**"
            # invia in Markdown+HTML safe: meglio HTML per la menzione tg://user
            text_html = f'{mention_html(requester.id if requester else None, f"@{requester.username}" if requester and requester.username else None)} ' \
                        f'ti ha contattato per il tuo turno del <b>{human}</b>.\n\n' \
                        f'<b>Ciao, questo turno è ancora disponibile?</b>'
            await ctx.bot.send_message(chat_id=owner_id, text=text_html, parse_mode="HTML")
            await query.answer("Richiesta inviata all'autore in privato ✅", show_alert=False)
        except Forbidden:
            # autore non ha DM aperto col bot → avvisa richiedente e offri link profilo autore
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
        else:
            # conferma al richiedente (nel gruppo o DM)
            await query.message.reply_text("📬 Ho scritto all’autore in privato. Attendi la risposta.")
            # fine CONTACT
            return

# ============== MAIN ==============
def main():
    if not TOKEN:
        raise SystemExit("Errore: variabile d'ambiente TELEGRAM_BOT_TOKEN mancante.")

    ensure_db()
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("testbtn", testbtn_cmd))
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
