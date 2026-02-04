#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShiftBot – Cambi turni Telegram (Render-ready)
Versione: 6.0  (private-first + org auth + manual approval + totals)
"""

import os
import re
import sqlite3
import shutil
import zoneinfo
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, Message,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.constants import ChatType
from telegram.error import Forbidden, BadRequest
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler,
    ApplicationHandlerStop
)

# -------------------- Config --------------------
VERSION = "ShiftBot 6.0"
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
DB_PATH = os.environ.get("SHIFTBOT_DB", "shiftbot.sqlite3")

TZ = zoneinfo.ZoneInfo("Europe/Rome")

# Reparti (codici fissi)
ORG_PDCNAFR = "PDCFRNA"
ORG_PDBNAFR = "PDBFRNA"

ORG_LABELS = {
    ORG_PDCNAFR: "PDC Napoli Frecciarossa",
    ORG_PDBNAFR: "PDB Napoli Frecciarossa",
}

# Admin per reparto: METTI QUI I NUMERIC USER_ID (non @username)
ORG_ADMINS = {
    ORG_PDCNAFR: set(),  # es: {123456789}
    ORG_PDBNAFR: set(),  # es: {987654321}
}

WELCOME_TEXT = (
    "👋 Benvenuto/a!\n\n"
    "Questo bot gestisce i *cambi turno*.\n\n"
    "✅ Per usare il bot devi essere *autenticato* nel tuo reparto.\n"
    "1) Scrivi /start\n"
    "2) Inserisci il *codice reparto*\n"
    "3) Attendi approvazione dell’admin\n\n"
    "Poi potrai:\n"
    "• Caricare un turno (invia immagine)\n"
    "• Cercare turni\n"
    "• Vedere le date\n"
)

DATE_PATTERNS = [
    r'(?P<d>\d{1,2})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<y>\d{4})',
    r'(?P<y>\d{4})[\/\-\.\s](?P<m>\d{1,2})[\/\-\.\s](?P<d>\d{1,2})',
]

# -------------------- UI Keyboard --------------------
PRIVATE_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("I miei turni")],
        [KeyboardButton("Cerca"), KeyboardButton("Date")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    is_persistent=True,
    input_field_placeholder="Usa i pulsanti 👇"
)

# -------------------- Volatile state --------------------
PENDING: Dict[int, Dict[str, Any]] = {}  # calendario -> dati post/immagine


# -------------------- Helpers: FS / DB --------------------
def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def migrate_sqlite_if_needed(persistent_path: str, legacy_path: str = "shiftbot.sqlite3"):
    """
    Se DB persistente non esiste ma c'è il vecchio DB locale, copialo una sola volta.
    Questo mantiene tutti i turni già caricati.
    """
    if os.path.abspath(persistent_path) == os.path.abspath(legacy_path):
        return
    if not os.path.exists(persistent_path) and os.path.exists(legacy_path):
        try:
            shutil.copy2(legacy_path, persistent_path)
            print(f"[ShiftBot] Migrato DB da {legacy_path} → {persistent_path}")
        except Exception as e:
            print(f"[ShiftBot] Migrazione DB fallita: {e}")

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Tabella shifts (compatibile con la tua esistente)
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

    # Soft migrations
    cur.execute("PRAGMA table_info(shifts);")
    cols = [r[1] for r in cur.fetchall()]
    if "status" not in cols:
        try: cur.execute("ALTER TABLE shifts ADD COLUMN status TEXT DEFAULT 'open';")
        except Exception: pass
    if "photo_file_id" not in cols:
        try: cur.execute("ALTER TABLE shifts ADD COLUMN photo_file_id TEXT;")
        except Exception: pass

    # Nuova tabella utenti (auth per reparto)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            org TEXT,
            status TEXT DEFAULT 'pending',   -- pending/approved/rejected
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

def is_admin_for_org(user_id: int, org: str) -> bool:
    return user_id in ORG_ADMINS.get(org, set())

def upsert_user(user_id: int, username: str, full_name: str, org: Optional[str], status: Optional[str] = None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    exists = cur.fetchone()
    if exists:
        if status is None:
            cur.execute("""
                UPDATE users SET username=?, full_name=?, org=COALESCE(?, org)
                WHERE user_id=?
            """, (username, full_name, org, user_id))
        else:
            cur.execute("""
                UPDATE users SET username=?, full_name=?, org=COALESCE(?, org), status=?
                WHERE user_id=?
            """, (username, full_name, org, status, user_id))
    else:
        cur.execute("""
            INSERT INTO users(user_id, username, full_name, org, status)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, username, full_name, org, status or "pending"))
    conn.commit()
    conn.close()

def get_user_row(user_id: int) -> Optional[Tuple[int, Optional[str], str]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id, org, status FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row  # (user_id, org, status) or None

def count_total_open_shifts() -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM shifts WHERE status='open'")
    n = int(cur.fetchone()[0])
    conn.close()
    return n

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

# -------------------- Auth gate --------------------
async def require_approved(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    """True se l’utente è approved, altrimenti spiega e blocca."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return False
    u = update.effective_user
    if not u:
        return False
    row = get_user_row(u.id)
    if not row:
        await update.effective_message.reply_text(
            "⛔ Non sei registrato.\nUsa /start e inserisci il *codice reparto*.",
            parse_mode="Markdown"
        )
        return False
    _, org, status = row
    if status != "approved":
        label = ORG_LABELS.get(org, org or "N/D")
        await update.effective_message.reply_text(
            f"⛔ Accesso non attivo.\nReparto: *{label}*\nStato: *{status}*\n\n"
            "Attendi approvazione dell’admin.",
            parse_mode="Markdown"
        )
        return False
    return True

# -------------------- Commands --------------------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    username = f"@{u.username}" if u.username else ""
    full_name = u.full_name or "utente"
    upsert_user(u.id, username, full_name, org=None, status=None)

    # Se /start <CODICE>
    payload = None
    if update.message and update.message.text:
        parts = update.message.text.split(maxsplit=1)
        if len(parts) > 1:
            payload = parts[1].strip().upper()

    if payload and payload in ORG_LABELS:
        # set org e pending
        upsert_user(u.id, username, full_name, org=payload, status="pending")
        await update.effective_message.reply_text(
            f"✅ Richiesta inviata.\nReparto: *{ORG_LABELS[payload]}*\nStato: *pending*\n\n"
            "Un admin del reparto ti approverà.\n"
            "Puoi chiedere all’admin di usare /pending.",
            parse_mode="Markdown"
        )
        # Notifica agli admin del reparto
        for admin_id in ORG_ADMINS.get(payload, set()):
            try:
                await ctx.bot.send_message(
                    chat_id=admin_id,
                    text=(f"🆕 Nuova richiesta\n"
                          f"Reparto: {ORG_LABELS[payload]}\n"
                          f"Utente: {full_name} {username}\n"
                          f"ID: {u.id}\n\n"
                          f"Usa /pending per approvare/rifiutare.")
                )
            except Exception:
                pass
        return

    # Se già registrato, mostra stato
    row = get_user_row(u.id)
    _, org, status = row if row else (u.id, None, "pending")
    if status == "approved":
        await update.effective_message.reply_text(
            "✅ Accesso attivo.\nUsa i pulsanti qui sotto 👇",
            reply_markup=PRIVATE_KB
        )
        return

    await update.effective_message.reply_text(
        WELCOME_TEXT + "\n\n"
        "📌 Inserisci il codice reparto:\n"
        f"• `{ORG_PDCNAFR}` = {ORG_LABELS[ORG_PDCNAFR]}\n"
        f"• `{ORG_PDBNAFR}` = {ORG_LABELS[ORG_PDBNAFR]}\n\n"
        "Esempio:\n`/start PDCFRNA`",
        parse_mode="Markdown"
    )

async def myid_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    uname = f"@{u.username}" if u.username else "(senza username)"
    await update.effective_message.reply_text(
        f"🆔 user_id: `{u.id}`\n👤 username: {uname}",
        parse_mode="Markdown"
    )

async def pending_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    admin = update.effective_user
    if not admin:
        return

    row = get_user_row(admin.id)
    if not row:
        await update.effective_message.reply_text("Non sei registrato. Usa /start.")
        return

    _, admin_org, status = row
    # admin deve essere approved e in ORG_ADMINS del suo org
    if status != "approved" or not admin_org or not is_admin_for_org(admin.id, admin_org):
        await update.effective_message.reply_text("⛔ Solo gli admin del reparto possono usare /pending.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, full_name, username
        FROM users
        WHERE status='pending' AND org=?
        ORDER BY created_at ASC
        LIMIT 100
    """, (admin_org,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.effective_message.reply_text("✅ Nessun utente in attesa.")
        return

    await update.effective_message.reply_text(f"⏳ Utenti in attesa ({len(rows)}):")
    for uid, full_name, username in rows:
        name_line = (full_name or "utente") + (f" ({username})" if username else "")
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Approva", callback_data=f"APPROVE|{uid}|{admin_org}"),
            InlineKeyboardButton("⛔ Rifiuta", callback_data=f"REJECT|{uid}|{admin_org}")
        ]])
        await ctx.bot.send_message(chat_id=admin.id, text=f"• {name_line}\nID: {uid}", reply_markup=kb)


# -------------------- Help & Version handlers --------------------
async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    await update.effective_message.reply_text(WELCOME_TEXT, parse_mode="Markdown")

async def version_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    await update.effective_message.reply_text(VERSION)

# -------------------- Search / Dates / My shifts --------------------
async def show_shifts(update: Update, ctx: ContextTypes.DEFAULT_TYPE, date_iso: str):
    # Solo utenti approvati (DM)
    if update.effective_chat.type == ChatType.PRIVATE:
        ok = await require_approved(update, ctx)
        if not ok:
            return

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
        f"📅 Turni trovati per *{human}*: {len(rows)}",
        parse_mode="Markdown",
        reply_markup=PRIVATE_KB
    )

    for (sid, chat_id, message_id, _user_id, _username, _caption, file_id) in rows:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contatta autore", callback_data=f"CONTACT|{sid}")]])
        sent_mid = None
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

        if file_id:
            try:
                await ctx.bot.send_photo(chat_id=update.effective_chat.id, photo=file_id, reply_markup=kb)
                continue
            except Exception:
                pass

        await ctx.bot.send_message(chat_id=update.effective_chat.id, text="(Immagine non disponibile)", reply_markup=kb)

async def search_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    ok = await require_approved(update, ctx)
    if not ok:
        return

    args = ctx.args
    date_iso = parse_date(" ".join(args)) if args else None
    if date_iso:
        await show_shifts(update, ctx, date_iso)
    else:
        kb = build_calendar(datetime.now(TZ), mode="SEARCH")
        await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)

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

    total = sum(int(c) for _, c in rows)
    lines = ["📆 *Date con turni aperti:*", ""]
    for date_iso, count in rows:
        d = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        lines.append(f"• {d}: {count}")
    lines.append("")
    lines.append(f"📌 *Totale turni aperti:* {total}")
    await ctx.bot.send_message(chat_id=user_id, text="\n".join(lines), parse_mode="Markdown", reply_markup=PRIVATE_KB)

async def dates_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    ok = await require_approved(update, ctx)
    if not ok:
        return
    await dates_list_dm(ctx, update.effective_chat.id)

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
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    ok = await require_approved(update, ctx)
    if not ok:
        return
    u = update.effective_user
    if u:
        await miei_list_dm(ctx, u.id)

# -------------------- Calendar --------------------
def build_calendar(base_date: datetime, mode="SETDATE") -> InlineKeyboardMarkup:
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
        InlineKeyboardButton("<", callback_data=f"NAV|{mode}|{prev_month.strftime('%Y-%m-%d')}"),
        InlineKeyboardButton(">", callback_data=f"NAV|{mode}|{next_month.strftime('%Y-%m-%d')}"),
    ])
    return InlineKeyboardMarkup(keyboard)

# -------------------- Upload handler (PRIVATE) --------------------
async def photo_or_doc_image_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Ora gestiamo upload SOLO in privato (approvato)
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    ok = await require_approved(update, ctx)
    if not ok:
        return

    msg = update.effective_message
    caption = (msg.caption or "").strip()
    date_iso = parse_date(caption)

    if not date_iso:
        kb = build_calendar(datetime.now(TZ), mode="SETDATE")
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
        await msg.reply_text(
            f"⛔ Hai già un turno *aperto* per il {human}.\n"
            f"Chiudi quello esistente con *Risolto* oppure usa *I miei turni*.",
            parse_mode="Markdown"
        )
        return

    await save_shift(msg, date_iso)
    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    await msg.reply_text(f"✅ Turno registrato per il {human}", reply_markup=PRIVATE_KB)

# -------------------- Callback handler --------------------
def mention_html(user_id: Optional[int], username: Optional[str]) -> str:
    if username and isinstance(username, str) and username.startswith("@") and len(username) > 1:
        return username
    if user_id:
        return f'<a href="tg://user?id={user_id}">utente</a>'
    return "utente"

async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = (query.data or "").split("|")

    # ---- NAV ----
    if parts[0] == "NAV":
        if len(parts) < 3:
            return
        date_str = parts[-1]
        mode = "|".join(parts[1:-1])
        try:
            new_month = datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            return
        kb = build_calendar(new_month, mode)
        await query.edit_message_reply_markup(reply_markup=kb)
        return

    # ---- SETDATE ----
    if parts[0] == "SETDATE":
        date_iso = parts[1]
        cal_msg_id = query.message.message_id if query.message else None
        data = PENDING.pop(cal_msg_id, None)
        if not data:
            await query.edit_message_text("❌ Non riesco a collegare il calendario al messaggio. Rimanda la foto.")
            return

        owner_id = data["owner_id"]
        if owner_id and has_open_on_date(owner_id, date_iso):
            human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
            await query.edit_message_text(
                f"⛔ Hai già un turno aperto per il {human}.\nUsa *I miei turni* per gestire.",
                parse_mode="Markdown"
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
        except Exception:
            pass
        try:
            await query.message.delete()
        except Exception:
            try: await query.edit_message_reply_markup(reply_markup=None)
            except Exception: pass
        return

    # ---- SEARCH ----
    if parts[0] == "SEARCH":
        date_iso = parts[1]
        fake_update = Update(update.update_id, message=query.message)
        await show_shifts(fake_update, ctx, date_iso)
        try:
            await query.edit_message_text(
                f"📅 Risultati mostrati per {datetime.strptime(date_iso, '%Y-%m-%d').strftime('%d/%m/%Y')}"
            )
        except Exception:
            pass
        return

    # ---- CLOSE ----
    if parts[0] == "CLOSE":
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
            await query.edit_message_text("❌ Turno non trovato.")
            return

        owner_id, status, date_iso = row
        user = update.effective_user
        if not user or user.id != owner_id:
            conn.close()
            await query.answer("Non hai i permessi.", show_alert=True)
            return

        cur.execute("DELETE FROM shifts WHERE id=?", (shift_id,))
        conn.commit()
        conn.close()

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        await query.edit_message_text(f"✅ Turno rimosso ({human}).")
        return

    # ---- CONTACT ----
    if parts[0] == "CONTACT":
        try:
            shift_id = int(parts[1])
        except Exception:
            await query.answer("ID turno non valido.", show_alert=True)
            return

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""SELECT user_id, username, date_iso FROM shifts WHERE id=?""", (shift_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            await query.answer("Turno non trovato.", show_alert=True)
            return

        owner_id, owner_username, date_iso = row
        requester = update.effective_user
        requester_name = mention_html(
            requester.id if requester else None,
            f"@{requester.username}" if requester and requester.username else None
        )
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y") if date_iso else ""

        # Telegram non consente "aprire chat tra utenti". Qui facciamo DM all’autore + bottone profilo richiedente.
        try:
            kb = None
            if requester and requester.username:
                kb = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("👤 Apri profilo richiedente", url=f"https://t.me/{requester.username}")]]
                )
            await ctx.bot.send_message(
                chat_id=owner_id,
                text=(f"📩 Richiesta cambio per il turno del *{human}*\n\n"
                      f"Richiedente: {requester_name}\n\n"
                      f"*Messaggio suggerito:* \nCiao, questo turno è ancora disponibile?"),
                parse_mode="HTML",
                reply_markup=kb
            )
            await query.message.reply_text("📬 Ho avvisato l’autore in privato.")
        except Forbidden:
            # se l’autore non ha mai aperto il bot, non possiamo scrivergli
            btns = None
            if owner_username and isinstance(owner_username, str) and owner_username.startswith("@"):
                handle = owner_username[1:]
                btns = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("👤 Apri profilo autore", url=f"https://t.me/{handle}")]]
                )
            await query.message.reply_text(
                "⚠️ Non posso scrivere all’autore perché non ha avviato il bot.\n"
                "Contattalo direttamente dal profilo:",
                reply_markup=btns
            )
        except Exception:
            await query.answer("Errore durante il contatto.", show_alert=True)
        return

    # ---- APPROVE / REJECT ----
    if parts[0] in ("APPROVE", "REJECT"):
        admin = update.effective_user
        if not admin:
            return
        try:
            target_uid = int(parts[1])
            org = parts[2]
        except Exception:
            await query.edit_message_text("❌ Parametri non validi.")
            return

        # verifica admin
        row = get_user_row(admin.id)
        if not row:
            await query.edit_message_text("⛔ Non sei registrato.")
            return
        _, admin_org, admin_status = row
        if admin_status != "approved" or admin_org != org or not is_admin_for_org(admin.id, org):
            await query.edit_message_text("⛔ Non hai permessi per approvare/rifiutare questo reparto.")
            return

        new_status = "approved" if parts[0] == "APPROVE" else "rejected"

        # aggiorna user
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE users SET status=? WHERE user_id=? AND org=?", (new_status, target_uid, org))
        conn.commit()
        conn.close()

        # notifica utente
        try:
            if new_status == "approved":
                await ctx.bot.send_message(
                    chat_id=target_uid,
                    text=(f"✅ Approvato!\nReparto: *{ORG_LABELS.get(org, org)}*\n\n"
                          "Ora puoi caricare turni (invia immagine) e usare i pulsanti 👇"),
                    parse_mode="Markdown",
                    reply_markup=PRIVATE_KB
                )
            else:
                await ctx.bot.send_message(
                    chat_id=target_uid,
                    text="⛔ Richiesta rifiutata. Se pensi sia un errore, contatta l’admin."
                )
        except Exception:
            pass

        await query.edit_message_text(f"✅ Operazione completata: {new_status} (ID {target_uid})")
        return

# -------------------- Text router (private) --------------------
async def block_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Risponde ai testi non riconosciuti in privato, senza interferire con i comandi."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    await update.effective_message.reply_text("Usa i pulsanti 👇", reply_markup=PRIVATE_KB)

async def private_text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    text = (update.effective_message.text or "").strip()

    # ✅ IMPORTANTISSIMO: non intercettare i comandi
    if text.startswith("/"):
        return

    low = text.lower()
    if low in ("cerca",):
        await search_cmd(update, ctx); return
    if low in ("date",):
        await dates_cmd(update, ctx); return
    if low in ("miei", "i miei turni"):
        await miei_cmd(update, ctx); return

    await update.effective_message.reply_text("Usa i pulsanti 👇", reply_markup=PRIVATE_KB)

# -------------------- Purge (optional) --------------------
async def purge_expired_shifts(ctx: ContextTypes.DEFAULT_TYPE):
    """Rimuove dal DB i turni con date passate."""
    try:
        today = datetime.now(TZ).date()
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""SELECT id FROM shifts
                       WHERE status='open' AND date(date_iso) < date(?)""", (today.isoformat(),))
        rows = cur.fetchall()
        ids = [r[0] for r in rows]
        if ids:
            cur.execute(
                f"DELETE FROM shifts WHERE id IN ({','.join('?'*len(ids))})",
                ids
            )
            conn.commit()
            print(f"[purge] Rimossi {len(ids)} turni scaduti (fino a {today.isoformat()}).")
        conn.close()
    except Exception as e:
        print(f"[purge] Errore durante purge: {e}")

# -------------------- MAIN --------------------
def main():
    if not TOKEN:
        raise SystemExit("Errore: variabile d'ambiente TELEGRAM_BOT_TOKEN mancante.")

    ensure_parent_dir(DB_PATH)
    migrate_sqlite_if_needed(DB_PATH)
    print(f"[ShiftBot] DB_PATH = {DB_PATH}")

    ensure_db()

    # Defaults (timezone Roma utile per jobqueue / date utils)
    try:
        from telegram.ext import Defaults
        defaults = Defaults(tzinfo=zoneinfo.ZoneInfo("Europe/Rome"))
        app = ApplicationBuilder().token(TOKEN).defaults(defaults).build()
    except Exception:
        app = ApplicationBuilder().token(TOKEN).build()

    # -------------------- Comandi (DM) --------------------
    app.add_handler(CommandHandler("start", start), group=1)
    app.add_handler(CommandHandler("help", help_cmd), group=1)          # se ce l'hai
    app.add_handler(CommandHandler("version", version_cmd), group=1)    # se ce l'hai
    app.add_handler(CommandHandler("myid", myid_cmd), group=1)
    app.add_handler(CommandHandler("pending", pending_cmd), group=1)    # se esiste davvero
    app.add_handler(CommandHandler("cerca", search_cmd), group=1)
    app.add_handler(CommandHandler("date", dates_cmd), group=1)
    app.add_handler(CommandHandler("miei", miei_cmd), group=1)

    # -------------------- Alias tastiera (DM) --------------------
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^I miei turni$"), miei_cmd),
        group=2
    )
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Cerca$"), search_cmd),
        group=2
    )
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & filters.Regex("^Date$"), dates_cmd),
        group=2
    )

    # -------------------- Upload immagini in privato --------------------
    img_doc_filter = (
        filters.Document.IMAGE
        if hasattr(filters.Document, "IMAGE")
        else filters.Document.MimeType("image/")
    )
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & (filters.PHOTO | img_doc_filter), photo_or_doc_image_handler),
        group=2
    )

    # -------------------- Callback inline --------------------
    app.add_handler(CallbackQueryHandler(button_handler), group=2)

    # -------------------- Router testo generico in privato --------------------
    # IMPORTANT: non intercettare i comandi (/myid ecc.)
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, private_text_router),
        group=3
    )

    # Blocca altro testo in DM (escludendo i 3 pulsanti e i comandi)
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND & ~filters.Regex("^(I miei turni|Cerca|Date)$"),
            block_text
        ),
        group=4
    )

    # -------------------- JobQueue purge (se disponibile) --------------------
    jq = getattr(app, "job_queue", None)
    if jq is not None:
        try:
            jq.run_once(purge_expired_shifts, when=30)
            jq.run_repeating(purge_expired_shifts, interval=3600, first=3600)
        except Exception as e:
            print(f"[ShiftBot] Errore JobQueue: {e}")
    else:
        print("[ShiftBot] JobQueue non disponibile (installa python-telegram-bot[job-queue])")

    print("ShiftBot avviato.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()