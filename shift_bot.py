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
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from glob import glob
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
LOG_PATH = os.environ.get("SHIFTBOT_LOG", "logs/shiftbot.log")

BACKUP_DIR = os.environ.get("SHIFTBOT_BACKUP_DIR", "backups")
BACKUP_KEEP = int(os.environ.get("SHIFTBOT_BACKUP_KEEP", "14"))  # numero backup da mantenere

TZ = zoneinfo.ZoneInfo("Europe/Rome")

# -------------------- Logging --------------------
logger = logging.getLogger("shiftbot")
logger.setLevel(logging.INFO)
logger.propagate = False

# evita duplicazione handler (reload / restart Render)
if not logger.handlers:
    Path(os.path.dirname(LOG_PATH) or ".").mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")

    # file log rotante
    try:
        fh = RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
    except Exception:
        pass

    # console log (Render)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

#
# -------------------- Logging helpers --------------------

def _safe_str(x: Any) -> str:
    try:
        return str(x)
    except Exception:
        return "?"

def log_event(event: str, **fields):
    """Log strutturato (key=value) per eventi chiave."""
    try:
        parts = [f"event={event}"]
        for k, v in fields.items():
            parts.append(f"{k}={_safe_str(v)}")
        logger.info(" ".join(parts))
    except Exception:
        # non rompere il bot per il logging
        pass
async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Global error handler: logs unhandled exceptions without crashing the bot."""
    try:
        logger.exception("Unhandled exception", exc_info=ctx.error)
        try:
            log_event("error", err=_safe_str(ctx.error))
        except Exception:
            pass
    except Exception:
        pass

# -------------------- Username gate (global) --------------------
USERNAME_REQUIRED_TEXT = (
    "⚠️ Per usare CambiServizi_bot devi impostare un *username* Telegram.\n\n"
    "Serve per permettere ai colleghi di contattarti direttamente (link t.me) e per usare correttamente i pulsanti e i comandi.\n\n"
    "✅ Come si imposta:\n"
    "1) Apri Telegram\n"
    "2) Vai su *Impostazioni*\n"
    "3) Tocca *Username*\n"
    "4) Scegline uno (senza spazi) e salva\n\n"
    "Poi torna qui e scrivi /start 🙂"
)

async def _reply_username_required(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Risposta standard quando manca l'username."""
    try:
        await update.effective_message.reply_text(USERNAME_REQUIRED_TEXT, parse_mode="Markdown")
    except Exception:
        try:
            await ctx.bot.send_message(chat_id=update.effective_chat.id, text=USERNAME_REQUIRED_TEXT, parse_mode="Markdown")
        except Exception:
            pass

async def _gate_username_for_commands(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Intercetta QUALSIASI comando da utenti senza username e spiega cosa fare."""
    u = update.effective_user
    if not u:
        return
    if u.username:
        return
    await _reply_username_required(update, ctx)
    raise ApplicationHandlerStop

# -------------------- Username gate for ANY text (non-command) --------------------
async def _gate_username_for_texts(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Intercetta QUALSIASI messaggio di testo (non-comando) da utenti senza username e spiega cosa fare."""
    u = update.effective_user
    if not u:
        return
    if u.username:
        return
    await _reply_username_required(update, ctx)
    raise ApplicationHandlerStop

async def _gate_username_for_callbacks(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    """True se OK; False se manca username (risponde e blocca).

    SEMPRE 'parlante': risponde sempre alla callback (query.answer) così il client non resta in loading.
    """
    query = update.callback_query
    if not query:
        return True

    u = query.from_user
    if not u:
        # chiudi comunque lo spinner
        try:
            await query.answer()
        except Exception:
            pass
        return True

    # OK: username presente -> chiudi lo spinner sempre
    if u.username:
        try:
            await query.answer()
        except Exception:
            pass
        return True

    # KO: manca username -> alert + istruzioni complete
    try:
        await query.answer(
            "Devi impostare un username Telegram (Impostazioni → Username).",
            show_alert=True
        )
    except Exception:
        pass

    # In DM o gruppo, lascia anche istruzioni complete
    try:
        await query.message.reply_text(USERNAME_REQUIRED_TEXT, parse_mode="Markdown")
    except Exception:
        pass

    return False
def _all_admin_ids() -> set[int]:
    try:
        return set().union(*ORG_ADMINS.values()) if ORG_ADMINS else set()
    except Exception:
        return set()


# -------------------- Helper: is_user_admin --------------------
async def is_user_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    """True se l'utente è admin di qualunque reparto (ed è approved)."""
    try:
        row = get_user_row(user_id)
        if not row:
            return False
        _uid, _org, status = row
        if status != "approved":
            return False
        return user_id in _all_admin_ids()
    except Exception:
        return False

# Reparti (codici fissi)
ORG_PDCNAFR = "PDCFRNA"
ORG_PDBNAFR = "PDBFRNA"

ORG_LABELS = {
    ORG_PDCNAFR: "PDC Napoli Frecciarossa",
    ORG_PDBNAFR: "PDB Napoli Frecciarossa",
}

# Admin per reparto: METTI QUI I NUMERIC USER_ID (non @username)
ORG_ADMINS = {
    ORG_PDCNAFR: {455696266},  # es: {123456789}
    ORG_PDBNAFR: {666837389},  # admin PDBFRNA
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


# -------------------- Backup helpers --------------------
def _safe_mkdir(path: str):
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

def _rotate_backups(backup_dir: str, keep: int):
    try:
        files = sorted(glob(os.path.join(backup_dir, "shiftbot_*.sqlite3")))
        if keep is None or keep <= 0:
            return
        if len(files) <= keep:
            return
        to_remove = files[: max(0, len(files) - keep)]
        for f in to_remove:
            try:
                os.remove(f)
            except Exception:
                pass
    except Exception:
        pass

def make_db_backup(reason: str = "scheduled") -> Optional[str]:
    """Crea un backup copiando il DB in BACKUP_DIR con timestamp. Ritorna il path del backup creato o None."""
    try:
        _safe_mkdir(BACKUP_DIR)
        ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        dst = os.path.join(BACKUP_DIR, f"shiftbot_{ts}.sqlite3")

        if not os.path.exists(DB_PATH):
            msg = f"[backup] DB non trovato, salto backup (DB_PATH={DB_PATH})"
            logger.warning(msg)
            print(msg, flush=True)
            return None

        shutil.copy2(DB_PATH, dst)
        _rotate_backups(BACKUP_DIR, BACKUP_KEEP)
        msg = f"[backup] OK ({reason}) -> {dst}"
        logger.info(msg)
        print(msg, flush=True)
        return dst
    except Exception as e:
        msg = f"[backup] ERRORE ({reason}): {e}"
        logger.error(msg)
        print(msg, flush=True)
        return None

async def backup_job(ctx: ContextTypes.DEFAULT_TYPE):
    # job pianificato
    make_db_backup(reason="job")


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
            org TEXT,
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

    if "org" not in cols:
        try:
            cur.execute("ALTER TABLE shifts ADD COLUMN org TEXT;")
        except Exception:
            pass

    # Backfill: i turni storici (pre-org) li consideriamo PDC di default
    try:
        cur.execute("UPDATE shifts SET org=? WHERE org IS NULL", (ORG_PDCNAFR,))
    except Exception:
        pass

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

    # Soft migrations users (tutorial evoluto)
    cur.execute("PRAGMA table_info(users);")
    ucols = [r[1] for r in cur.fetchall()]

    if "tutorial_stage" not in ucols:
        try:
            cur.execute("ALTER TABLE users ADD COLUMN tutorial_stage INTEGER DEFAULT 0;")
        except Exception:
            pass

    if "last_tutorial_at" not in ucols:
        try:
            cur.execute("ALTER TABLE users ADD COLUMN last_tutorial_at TEXT;")
        except Exception:
            pass

    if "tutorial_reminder_sent" not in ucols:
        try:
            cur.execute("ALTER TABLE users ADD COLUMN tutorial_reminder_sent INTEGER DEFAULT 0;")
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


def get_approved_org(user_id: int) -> Optional[str]:
    """Ritorna l'org se l'utente è approved, altrimenti None."""
    row = get_user_row(user_id)
    if not row:
        return None
    _, org, status = row
    if status != "approved":
        return None
    return org
# -------------------- Search / Dates / My shifts --------------------

# ----------- Tutorial quick command -----------
async def tutorial_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mostra la guida rapida.

    - In privato: risponde in chat.
    - In gruppo/supergruppo: prova a inviare in DM all'utente; se non può, mostra un bottone per aprire il bot.
    """
    u = update.effective_user
    if not u:
        return

    text = (
        "📘 Guida rapida CambiServizi_bot\n\n"
        "1️⃣ Invia screenshot turno📎\n\n"
        "Scrivi cosa vorresti in cambio ⌨️\n\n"
        "Scegli la data 🗓️\n\n"
        "2️⃣ Premi Cerca per trovare turni in una data specifica.\n\n"
        "3️⃣ Premi Date per elenco sintetico.\n\n"
        "4️⃣ Premi I miei turni per gestire i tuoi.\n"
        "Clicca su Risolto ✅ se il cambio è stato effettuato.\n\n"
        "Fine 🙂"
    )

    # log
    try:
        log_event("tutorial", user_id=u.id, org=(get_approved_org(u.id) or ""), chat_type=update.effective_chat.type)
    except Exception:
        pass

    # Private: rispondi normalmente
    if update.effective_chat.type == ChatType.PRIVATE:
        await update.effective_message.reply_text(text, reply_markup=PRIVATE_KB)
        return

    # Gruppo: invia in DM all'utente
    try:
        await ctx.bot.send_message(chat_id=u.id, text=text, reply_markup=PRIVATE_KB)
        await update.effective_message.reply_text("✉️ Ti ho inviato la guida in privato.")
        return
    except Forbidden:
        bot_username = ctx.bot.username or "this_bot"
        url = f"https://t.me/{bot_username}?start=start"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔒 Apri chat privata col bot", url=url)]])
        await update.effective_message.reply_text(
            "Per leggere la guida devi prima aprire la chat privata con me:",
            reply_markup=kb
        )
        return


# -------------------- /commands (solo admin) --------------------
async def commands_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Elenca i comandi admin disponibili (esclude /admin2507)."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    if not await is_user_admin(update, ctx, u.id):
        await update.effective_message.reply_text("⛔ Comando riservato agli admin.")
        return

    text = (
        "🛠 Comandi amministratore disponibili:\n\n"
        "• /pending — richieste in attesa (del tuo reparto)\n"
        "• /approved — lista approvati (del tuo reparto)\n"
        "• /approvedpdcfrna — lista approvati PDCFRNA\n"
        "• /approvedpdbfrna — lista approvati PDBFRNA\n"
        "• /revoke — revoca autorizzazione (del tuo reparto)\n"
        "• /backupnow — crea backup su disco\n"
        "• /backupsend — crea e invia backup in chat\n"
        "• /stats [1|7|30] — statistiche utilizzo bot\n"
        "• /logs [N] — ultime righe log (max 2000)\n"
        "• /commands — questa lista\n\n"
    )

    await update.effective_message.reply_text(text)


# -------------------- /stats (solo admin, tutte org) --------------------
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Statistiche utilizzo (solo admin). Uso: /stats [1|7|30]"""
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    row = get_user_row(u.id)
    if not row:
        await update.effective_message.reply_text("Non sei registrato. Usa /start.")
        return

    _uid, _org, status = row
    all_admins = _all_admin_ids()
    if status != "approved" or u.id not in all_admins:
        await update.effective_message.reply_text("⛔ Solo gli admin possono usare /stats.")
        return

    days = 1
    if ctx.args:
        try:
            days = int(ctx.args[0])
        except Exception:
            days = 1

    if days not in (1, 7, 30):
        await update.effective_message.reply_text("Uso: /stats 1 oppure /stats 7 oppure /stats 30")
        return

    if not os.path.exists(LOG_PATH):
        await update.effective_message.reply_text("❌ File di log non trovato.")
        return

    cutoff = datetime.now(TZ) - timedelta(days=days)

    # pattern: [2026-02-06 23:40:59,367] INFO event=tutorial user_id=...
    ts_re = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\]\s+\w+\s+(.*)$")

    event_counts: Dict[str, int] = {}
    org_counts: Dict[str, int] = {}
    user_ids: set[int] = set()
    total = 0

    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "event=" not in line:
                    continue
                m = ts_re.match(line.rstrip("\n"))
                if not m:
                    continue
                try:
                    ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                    ts = ts.replace(tzinfo=TZ)
                except Exception:
                    continue
                if ts < cutoff:
                    continue

                rest = m.group(2)
                # extract key=value pairs
                kv = {}
                for part in rest.split():
                    if "=" in part:
                        k, v = part.split("=", 1)
                        kv[k.strip()] = v.strip()

                ev = kv.get("event")
                if not ev:
                    continue

                total += 1
                event_counts[ev] = event_counts.get(ev, 0) + 1

                orgv = kv.get("org", "").strip()
                if orgv:
                    org_counts[orgv] = org_counts.get(orgv, 0) + 1

                uidv = kv.get("user_id")
                if uidv:
                    try:
                        user_ids.add(int(uidv))
                    except Exception:
                        pass

    except Exception as e:
        logger.error(f"[stats] ERROR: {e}")
        await update.effective_message.reply_text("❌ Errore durante la lettura del log.")
        return

    if total == 0:
        await update.effective_message.reply_text(f"📈 Statistiche utilizzo (ultimi {days} giorni)\n\nNessun evento registrato nel periodo.")
        return

    lines = [
        f"📈 Statistiche utilizzo (ultimi {days} giorni)",
        "",
        f"Eventi totali: {total}",
        f"Utenti unici: {len(user_ids)}",
        "",
        "Top eventi:",
    ]

    for ev, cnt in sorted(event_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        lines.append(f"• {ev}: {cnt}")

    if org_counts:
        lines.append("")
        lines.append("Top reparti:")
        for orgv, cnt in sorted(org_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            label = ORG_LABELS.get(orgv, orgv)
            lines.append(f"• {label} ({orgv}): {cnt}")

    await update.effective_message.reply_text("\n".join(lines))
    log_event("stats", admin_id=u.id, days=days, total=total)

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
                   org: Optional[str], file_id: Optional[str] = None) -> int:
    # Enforce org isolation: never save a shift without a valid org.
    if not org:
        # Try to infer from user_id (safety net)
        if user_id:
            org = get_approved_org(user_id)
        if not org:
            print(f"[ShiftBot] Refusing to save shift: missing org (chat_id={chat_id}, message_id={message_id}, user_id={user_id})")
            return -1

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO shifts(chat_id, message_id, user_id, username, date_iso, caption, photo_file_id, org, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')""",
        (chat_id, message_id, user_id, (username or ""), date_iso, caption or "", file_id, org)
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
    org = None
    if msg.from_user:
        org = get_approved_org(msg.from_user.id)
    return save_shift_raw(
        chat_id=msg.chat.id,
        message_id=msg.message_id,
        user_id=(msg.from_user.id if msg.from_user else None),
        username=username,
        caption=(msg.caption or ""),
        date_iso=date_iso,
        org=org,
        file_id=file_id
    )

# -------------------- Username requirement helper --------------------
async def require_username(update: Update) -> bool:
    """Blocca l'uso del bot se l'utente non ha un username Telegram.

    Serve per permettere il contatto diretto tra colleghi (link t.me).
    Valido in qualsiasi chat (privato o gruppo).
    """
    u = update.effective_user
    if not u:
        return False

    if not u.username:
        try:
            await update.effective_message.reply_text(
                USERNAME_REQUIRED_TEXT,
                parse_mode="Markdown"
            )
        except Exception:
            pass
        return False

    return True

# -------------------- Auth gate --------------------
async def require_approved(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    """True se l’utente è approved, altrimenti spiega e blocca."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return False
    u = update.effective_user
    if not u:
        return False
    if not await require_username(update):
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

    if not await require_username(update):
        return

    u = update.effective_user
    if not u:
        return

    username = f"@{u.username}" if u.username else ""
    full_name = u.full_name or "utente"

    # Se /start <CODICE>
    payload = None
    if update.message and update.message.text:
        parts = update.message.text.split(maxsplit=1)
        if len(parts) > 1:
            payload = parts[1].strip().upper()

    log_event("start", user_id=u.id, username=username, full_name=full_name, payload=(payload or ""))
    upsert_user(u.id, username, full_name, org=None, status=None)

    if payload and payload in ORG_LABELS:
        # Se l'utente è nella lista admin del reparto, auto-approva (bootstrap)
        desired_status = "approved" if is_admin_for_org(u.id, payload) else "pending"

        upsert_user(u.id, username, full_name, org=payload, status=desired_status)
        log_event("auth_request", user_id=u.id, org=payload, status=desired_status)

        if desired_status == "approved":
            await update.effective_message.reply_text(
                f"✅ Accesso attivo (admin).\nReparto: *{ORG_LABELS[payload]}*\n\n"
                "Ora puoi usare /pending per approvare gli altri e usare i pulsanti qui sotto 👇",
                parse_mode="Markdown",
                reply_markup=PRIVATE_KB
            )
            await update.effective_message.reply_text(
                "📘 *Mini‑tutorial rapido*\n\n"
                "• Invia una foto del turno → Scegli la data\n"
                "• Usa *Cerca* per vedere turni disponibili\n"
                "• Usa *Date* per elenco sintetico\n"
                "• Usa *I miei turni* per gestire i tuoi\n\n"
                "Se hai dubbi scrivi pure.",
                parse_mode="Markdown"
            )
            return

        await update.effective_message.reply_text(
            f"✅ Richiesta inviata.\nReparto: *{ORG_LABELS[payload]}*\nStato: *pending*\n\n"
            "Un admin del reparto ti approverà.\n"
            "Puoi chiedere all’admin di usare /pending.",
            parse_mode="Markdown"
        )

        # Notifica agli admin del reparto (se esistono)
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
        log_event("auth_ok", user_id=u.id, org=org, status=status)
        await update.effective_message.reply_text(
            "✅ Accesso attivo.\nUsa i pulsanti qui sotto 👇",
            reply_markup=PRIVATE_KB
        )
        return

    log_event("auth_needed", user_id=u.id, org=org, status=status)
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
    if not u.username:
        await update.effective_message.reply_text(
            "⚠️ Per usare CambiServizi_bot in modo completo (contatto diretto) devi impostare un username.\n"
            "Vai su Telegram: Impostazioni → Username.")

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

    log_event("pending_list", admin_id=admin.id, org=admin_org)
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



# -------------------- Approved users command --------------------
async def approved_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Lista gli utenti approvati.

    Comportamento:
    - /approved                 -> lista approvati del *tuo* reparto (solo admin del reparto)
    - /approved <ORG_CODE>      -> lista approvati di un reparto specifico (solo admin "global", cioè presente in ORG_ADMINS di qualunque reparto)

    Nota: usiamo testo semplice (no Markdown) per evitare errori di parsing con nomi/username.
    """
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

    # Caso: /approved <ORG_CODE>
    if ctx.args:
        org_code = (ctx.args[0] or "").strip().upper()

        # Consenti a qualunque admin (di qualunque reparto) di vedere liste per org specifico
        all_admins = _all_admin_ids()
        if status != "approved" or admin.id not in all_admins:
            await update.effective_message.reply_text("⛔ Solo gli admin possono usare /approved <REPARTO>.")
            return

        # Normalizza alias comuni (utente scrive spesso in minuscolo)
        if org_code in ("PDCFRNA", ORG_PDCNAFR):
            org_code = ORG_PDCNAFR
        elif org_code in ("PDBFRNA", ORG_PDBNAFR):
            org_code = ORG_PDBNAFR

        await _approved_list_for_org(update, ctx, org_code)
        return

    # Default: lista approvati del *tuo* reparto (solo admin reparto)
    if status != "approved" or not admin_org or not is_admin_for_org(admin.id, admin_org):
        await update.effective_message.reply_text("⛔ Solo gli admin del reparto possono usare /approved.")
        return

    log_event("approved_list", admin_id=admin.id, org=admin_org)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, full_name, username, created_at
        FROM users
        WHERE status='approved' AND org=?
        ORDER BY created_at ASC
        """,
        (admin_org,),
    )
    rows = cur.fetchall()
    conn.close()

    label = ORG_LABELS.get(admin_org, admin_org)

    if not rows:
        await update.effective_message.reply_text(f"✅ Nessun utente approvato per {label}.")
        return

    lines = [f"✅ Utenti approvati – {label}", ""]
    for uid, full_name, username, _created_at in rows:
        name = (full_name or "utente").strip()
        if username:
            name += f" ({username})"
        lines.append(f"• {name} — {uid}")

    lines.append("")
    lines.append(f"Totale approvati: {len(rows)}")

    # Telegram ha limite ~4096 caratteri: invia a chunk
    MAX = 3800
    chunk: list[str] = []
    size = 0

    async def _flush():
        nonlocal chunk, size
        if chunk:
            await update.effective_message.reply_text("\n".join(chunk))
            chunk = []
            size = 0

    for line in lines:
        add = len(line) + 1
        if size + add > MAX:
            await _flush()
        chunk.append(line)
        size += add

    await _flush()

# -------------------- Approved users by org (admin-only) --------------------
async def _approved_list_for_org(update: Update, ctx: ContextTypes.DEFAULT_TYPE, org_code: str):
    """Lista utenti approvati per uno specifico reparto (solo admin di qualunque reparto)."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    admin = update.effective_user
    if not admin:
        return

    row = get_user_row(admin.id)
    if not row:
        await update.effective_message.reply_text("Non sei registrato. Usa /start.")
        return

    _, _admin_org, status = row

    # Consenti a qualunque admin (di qualunque reparto)
    all_admins = _all_admin_ids()
    if status != "approved" or admin.id not in all_admins:
        await update.effective_message.reply_text("⛔ Solo gli admin possono usare questo comando.")
        return
    log_event("approved_list_org", admin_id=admin.id, org=org_code)

    if org_code not in ORG_LABELS:
        await update.effective_message.reply_text("❌ Reparto non valido.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, full_name, username, created_at
        FROM users
        WHERE status='approved' AND org=?
        ORDER BY created_at ASC
        """,
        (org_code,),
    )
    rows = cur.fetchall()
    conn.close()

    label = ORG_LABELS.get(org_code, org_code)

    if not rows:
        await update.effective_message.reply_text(f"✅ Nessun utente approvato per {label}.")
        return

    lines = [f"✅ Utenti approvati – {label}", ""]
    for uid, full_name, username, _created_at in rows:
        name = (full_name or "utente").strip()
        if username:
            name += f" ({username})"
        lines.append(f"• {name} — {uid}")

    lines.append("")
    lines.append(f"Totale approvati: {len(rows)}")

    # Chunk per limite Telegram
    MAX = 3800
    chunk: list[str] = []
    size = 0

    async def _flush():
        nonlocal chunk, size
        if chunk:
            await update.effective_message.reply_text("\n".join(chunk))
            chunk = []
            size = 0

    for line in lines:
        add = len(line) + 1
        if size + add > MAX:
            await _flush()
        chunk.append(line)
        size += add

    await _flush()


async def approvedpdcfrna_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _approved_list_for_org(update, ctx, ORG_PDCNAFR)


async def approvedpdbfrna_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await _approved_list_for_org(update, ctx, ORG_PDBNAFR)


# -------------------- Backup command --------------------

async def backupnow_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Esegue un backup immediato del DB (solo admin di qualunque reparto)."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    row = get_user_row(u.id)
    if not row:
        await update.effective_message.reply_text("Non sei registrato. Usa /start.")
        return

    _, org, status = row
    if status != "approved" or not org or not is_admin_for_org(u.id, org):
        await update.effective_message.reply_text("⛔ Solo gli admin possono eseguire /backupnow.")
        return

    log_event("backup_now", admin_id=u.id, org=org)
    path = make_db_backup(reason=f"manual by {u.id}")
    if not path:
        await update.effective_message.reply_text("❌ Backup fallito. Controlla i log su Render.")
        return

    await update.effective_message.reply_text(
        f"✅ Backup creato.\nFile: {path}\n\nMantengo gli ultimi {BACKUP_KEEP} backup in `{BACKUP_DIR}`.",
        parse_mode="Markdown"
    )

# -------------------- Backup send command --------------------
async def backupsend_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Crea un backup e lo invia come file su Telegram (solo admin del proprio reparto).

    Nota: Telegram ha limiti di dimensione; per un DB SQLite normale (pochi MB) è ok.
    """
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    row = get_user_row(u.id)
    if not row:
        await update.effective_message.reply_text("Non sei registrato. Usa /start.")
        return

    _, org, status = row
    if status != "approved" or not org or not is_admin_for_org(u.id, org):
        await update.effective_message.reply_text("⛔ Solo gli admin possono eseguire /backupsend.")
        return

    log_event("backup_send", admin_id=u.id, org=org)
    path = make_db_backup(reason=f"manual-send by {u.id}")
    if not path or not os.path.exists(path):
        await update.effective_message.reply_text("❌ Backup fallito. Controlla i log su Render.")
        return

    try:
        # Invia il file come documento
        await ctx.bot.send_document(
            chat_id=u.id,
            document=open(path, "rb"),
            filename=os.path.basename(path),
            caption=f"✅ Backup DB\n{os.path.basename(path)}"
        )
        await update.effective_message.reply_text("📦 Backup inviato in chat (documento).")
        logger.info(f"[backup] SENT (manual by {u.id}) -> {path}")
    except Exception as e:
        logger.error(f"[backup] SEND ERROR (manual by {u.id}) -> {path}: {e}")
        await update.effective_message.reply_text("❌ Non sono riuscito a inviarti il file (limite dimensione o errore Telegram).")


# -------------------- Revoke users command --------------------
async def revoke_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Revoca l'autorizzazione (approved -> pending) a un utente del *tuo* reparto (solo admin reparto).

    Uso:
      • /revoke            -> lista approvati con pulsanti Revoca
      • /revoke <user_id>  -> revoca diretta
    """
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
    if status != "approved" or not admin_org or not is_admin_for_org(admin.id, admin_org):
        await update.effective_message.reply_text("⛔ Solo gli admin del reparto possono usare /revoke.")
        return

    log_event("revoke_open", admin_id=admin.id, org=admin_org, mode=("direct" if ctx.args else "list"))
    # Revoca diretta: /revoke 123
    target_uid: Optional[int] = None
    if ctx.args:
        try:
            target_uid = int(ctx.args[0])
        except Exception:
            target_uid = None

    if target_uid is not None:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT user_id, full_name, username, org, status FROM users WHERE user_id=?", (target_uid,))
        r = cur.fetchone()
        if not r:
            conn.close()
            await update.effective_message.reply_text("❌ Utente non trovato.")
            return

        uid, full_name, username, org, ustatus = r
        if org != admin_org:
            conn.close()
            await update.effective_message.reply_text("⛔ Puoi revocare solo utenti del tuo reparto.")
            return
        if ustatus != "approved":
            conn.close()
            await update.effective_message.reply_text("ℹ️ Questo utente non è in stato approved.")
            return

        cur.execute("UPDATE users SET status='pending' WHERE user_id=? AND org=?", (uid, admin_org))
        conn.commit()
        conn.close()
        log_event("revoke_done", admin_id=admin.id, org=admin_org, target_uid=uid)

        name = (full_name or "utente") + (f" ({username})" if username else "")
        await update.effective_message.reply_text(f"✅ Autorizzazione revocata: {name} — `{uid}`", parse_mode="Markdown")

        # Notifica all'utente revocato
        try:
            await ctx.bot.send_message(
                chat_id=uid,
                text=("⛔ La tua autorizzazione è stata *revocata* dall'admin del reparto.\n\n"
                      f"Per riattivarla, invia di nuovo: `/start {admin_org}`"),
                parse_mode="Markdown"
            )
        except Exception:
            pass
        return

    # Lista approvati con pulsanti Revoca
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, full_name, username, created_at
        FROM users
        WHERE status='approved' AND org=?
        ORDER BY created_at ASC
        LIMIT 200
        """,
        (admin_org,),
    )
    rows = cur.fetchall()
    conn.close()

    label = ORG_LABELS.get(admin_org, admin_org)

    if not rows:
        await update.effective_message.reply_text(f"✅ Nessun utente approvato per {label}.")
        return

    await update.effective_message.reply_text(f"🧯 Revoca autorizzazioni – *{label}*\nSeleziona un utente:", parse_mode="Markdown")

    for uid, full_name, username, _created_at in rows:
        name = (full_name or "utente")
        if username:
            name += f" ({username})"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🧯 Revoca", callback_data=f"REVOKE|{uid}|{admin_org}")]
        ])
        await ctx.bot.send_message(chat_id=admin.id, text=f"• {name}\nID: {uid}", reply_markup=kb)


# -------------------- Admin dashboard command --------------------
async def admin_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Dashboard admin (comando: /admin2507): mostra stats per *tutti* i reparti (solo admin).

    Nota: i reparti vengono letti dinamicamente dal DB (users/shifts) così non devi aggiornare il codice
    ogni volta che aggiungi un nuovo reparto.
    """
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    row = get_user_row(u.id)
    if not row:
        await update.effective_message.reply_text("Non sei registrato. Usa /start.")
        return

    _, _org, status = row

    # Consenti /admin2507 a qualunque admin (di qualunque reparto)
    all_admins = _all_admin_ids()
    if status != "approved" or u.id not in all_admins:
        await update.effective_message.reply_text("⛔ Solo gli admin possono usare /admin2507.")
        return
    log_event("admin_dashboard", admin_id=u.id)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Reparti dinamici: union di quelli presenti in users e shifts
    cur.execute(
        """
        SELECT org FROM (
            SELECT DISTINCT org AS org FROM users  WHERE org IS NOT NULL AND org <> ''
            UNION
            SELECT DISTINCT org AS org FROM shifts WHERE org IS NOT NULL AND org <> ''
        )
        ORDER BY org ASC
        """
    )
    orgs = [r[0] for r in cur.fetchall()]

    lines = ["📊 Dashboard Admin (tutti i reparti)", ""]

    total_approved = 0
    total_pending = 0
    total_open_shifts = 0

    for org_code in orgs:
        org_label = ORG_LABELS.get(org_code, org_code)

        cur.execute("SELECT COUNT(*) FROM users WHERE status='approved' AND org=?", (org_code,))
        approved = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM users WHERE status='pending' AND org=?", (org_code,))
        pending = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM shifts WHERE status='open' AND org=?", (org_code,))
        open_shifts = int(cur.fetchone()[0])

        total_approved += approved
        total_pending += pending
        total_open_shifts += open_shifts

        lines.append(f"🏷️ {org_label} ({org_code})")
        lines.append(f"👥 Approvati: {approved}")
        lines.append(f"⏳ In attesa: {pending}")
        lines.append(f"📅 Turni aperti: {open_shifts}")
        lines.append("")

    # Info backup (ultimo file) + dimensione DB
    try:
        last_bk = sorted(glob(os.path.join(BACKUP_DIR, "shiftbot_*.sqlite3")))[-1]
    except Exception:
        last_bk = None

    try:
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    except Exception:
        db_size = 0

    conn.close()

    lines.append("—")
    lines.append(f"👥 Totale approvati: {total_approved}")
    lines.append(f"⏳ Totale pending: {total_pending}")
    lines.append(f"📅 Totale turni aperti: {total_open_shifts}")

    if last_bk:
        lines.append("")
        lines.append(f"🧾 Ultimo backup: {os.path.basename(last_bk)}")

    if db_size:
        lines.append(f"💾 Dimensione DB: {db_size/1024/1024:.2f} MB")

    await update.effective_message.reply_text("\n".join(lines))

#
# -------------------- Logs command --------------------
async def logs_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Mostra le ultime righe del log (solo admin). Uso: /logs [N]"""
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    u = update.effective_user
    if not u:
        return

    row = get_user_row(u.id)
    if not row:
        await update.effective_message.reply_text("Non sei registrato. Usa /start.")
        return

    _, _org, status = row
    all_admins = _all_admin_ids()
    if status != "approved" or u.id not in all_admins:
        await update.effective_message.reply_text("⛔ Solo gli admin possono usare /logs.")
        return

    # quante righe?
    n = 200
    if ctx.args:
        try:
            n = max(20, min(2000, int(ctx.args[0])))
        except Exception:
            n = 200

    if not os.path.exists(LOG_PATH):
        await update.effective_message.reply_text("❌ File di log non trovato.")
        return

    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        tail = lines[-n:] if len(lines) > n else lines
        header = [f"🧾 Log (ultime {len(tail)} righe)", ""]
        text_lines = header + [ln.rstrip("\n") for ln in tail]

        # invio a chunk per limite Telegram
        MAX = 3800
        chunk: list[str] = []
        size = 0

        async def _flush():
            nonlocal chunk, size
            if chunk:
                await update.effective_message.reply_text("\n".join(chunk))
                chunk = []
                size = 0

        for line in text_lines:
            add = len(line) + 1
            if size + add > MAX:
                await _flush()
            chunk.append(line)
            size += add

        await _flush()
        log_event("logs_view", admin_id=u.id, lines=len(tail))
    except Exception as e:
        logger.error(f"[logs] ERROR: {e}")
        await update.effective_message.reply_text("❌ Errore durante la lettura del log.")

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
        requester = update.effective_user
        requester_org = get_approved_org(requester.id) if requester else None
        if not requester_org:
            return
    else:
        requester_org = None

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if requester_org:
        cur.execute("""SELECT id, chat_id, message_id, user_id, username, caption, photo_file_id
                       FROM shifts
                       WHERE date_iso=? AND status='open' AND org=?
                       ORDER BY created_at ASC""", (date_iso, requester_org))
    else:
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
        # Pulsante diretto: apre subito la chat dell'autore (se ha username)
        handle = None
        if _username and isinstance(_username, str) and _username.startswith("@") and len(_username) > 1:
            handle = _username[1:]

        # Fallback: prova a leggere username aggiornato dalla tabella users (per turni legacy)
        if not handle and _user_id:
            try:
                conn2 = sqlite3.connect(DB_PATH)
                cur2 = conn2.cursor()
                cur2.execute("SELECT username FROM users WHERE user_id=?", (_user_id,))
                r2 = cur2.fetchone()
                conn2.close()
                if r2 and r2[0] and isinstance(r2[0], str) and r2[0].startswith("@") and len(r2[0]) > 1:
                    handle = r2[0][1:]
            except Exception:
                handle = None

        if handle:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contatta autore", url=f"https://t.me/{handle}")]])
        else:
            # Se non c'è username, mantieni il vecchio callback per mostrare il messaggio di avviso
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
    u = update.effective_user
    log_event("search", user_id=(u.id if u else None), org=(get_approved_org(u.id) if u else None), args=" ".join(ctx.args) if ctx.args else "")
    args = ctx.args
    date_iso = parse_date(" ".join(args)) if args else None
    if date_iso:
        await show_shifts(update, ctx, date_iso)
        # Tutorial evoluto: prima consultazione (Cerca)
        try:
            if u:
                maybe_send_tutorial_tip(ctx, u.id, 2)
        except Exception:
            pass
    else:
        kb = build_calendar(datetime.now(TZ), mode="SEARCH")
        await update.effective_message.reply_text("📅 Seleziona la data che vuoi consultare:", reply_markup=kb)

async def dates_list_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    org = get_approved_org(user_id)
    if not org:
        await ctx.bot.send_message(chat_id=user_id, text="⛔ Non sei registrato.")
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT date_iso, COUNT(*) FROM shifts
                   WHERE status='open' AND org=?
                   GROUP BY date_iso ORDER BY date_iso ASC""", (org,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await ctx.bot.send_message(chat_id=user_id, text="Non ci sono turni aperti al momento.", reply_markup=PRIVATE_KB)
        return

    total = sum(int(c) for _, c in rows)
    lines = ["🗓️ *Date con turni aperti:*", ""]
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

    u = update.effective_user
    log_event(
        "dates",
        user_id=(u.id if u else None),
        org=(get_approved_org(u.id) if u else None)
    )

    await dates_list_dm(ctx, update.effective_chat.id)

    # Tutorial evoluto: dopo una consultazione spingi verso "I miei turni"
    try:
        if u:
            maybe_send_tutorial_tip(ctx, u.id, 2)
    except Exception:
        pass

async def miei_list_dm(ctx: ContextTypes.DEFAULT_TYPE, user_id: int):
    org = get_approved_org(user_id)
    if not org:
        await ctx.bot.send_message(chat_id=user_id, text="⛔ Non sei registrato.")
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT id, chat_id, message_id, date_iso, caption, photo_file_id
                   FROM shifts
                   WHERE user_id=? AND status='open' AND org=?
                   ORDER BY created_at DESC
                   LIMIT 50""", (user_id, org))
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
    log_event("miei", user_id=(u.id if u else None), org=(get_approved_org(u.id) if u else None))
    if u:
        await miei_list_dm(ctx, u.id)
        # Tutorial evoluto: ha aperto "I miei turni"
        try:
            maybe_send_tutorial_tip(ctx, u.id, 3)
        except Exception:
            pass

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
    u = update.effective_user
    log_event("upload_received", user_id=(u.id if u else None), org=(get_approved_org(u.id) if u else None), has_date=bool(date_iso))

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

    saved_id = await save_shift(msg, date_iso)
    if saved_id == -1:
        log_event("upload_denied", user_id=(msg.from_user.id if msg.from_user else None), reason="missing_org")
        await msg.reply_text(
            "⛔ Non posso salvare il turno: non risulti *approvato* in un reparto.\n"
            "Invia di nuovo: `/start PDCFRNA` oppure `/start PDBFRNA` e attendi approvazione.",
            parse_mode="Markdown"
        )
        return

    human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    log_event("upload_saved", user_id=owner_id, org=get_approved_org(owner_id) if owner_id else None, date_iso=date_iso, shift_id=saved_id)

    # Tutorial evoluto: Step 1 (primo upload salvato)
    try:
        if owner_id:
            maybe_send_tutorial_tip(ctx, owner_id, 1)
    except Exception:
        pass

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
    # Username gate: se manca username, blocca qualsiasi pulsante/callback
    ok_user = await _gate_username_for_callbacks(update, ctx)
    if not ok_user:
        return
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

        owner_org = get_approved_org(owner_id) if owner_id else None
        if not owner_org:
            await query.edit_message_text(
                "⛔ Non posso registrare il turno perché non risulti più *approvato* in un reparto.\n"
                "Rifai /start con il tuo codice reparto e riprova.",
                parse_mode="Markdown"
            )
            return

        new_id = save_shift_raw(
            chat_id=data["src_chat_id"],
            message_id=data["src_msg_id"],
            user_id=owner_id,
            username=data.get("owner_username", ""),
            caption=data.get("caption", ""),
            date_iso=date_iso,
            org=owner_org,
            file_id=data.get("file_id"),
        )
        if new_id == -1:
            await query.edit_message_text(
                "⛔ Non posso registrare il turno: reparto non valido.\nRifai /start e riprova.",
                parse_mode="Markdown"
            )
            return

        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
        try:
            await ctx.bot.send_message(chat_id=owner_id, text=f"✅ Turno registrato per il {human}", reply_markup=PRIVATE_KB)
        except Exception:
            pass
        # Tutorial evoluto: Step 1 completato anche quando salva da calendario
        try:
            if owner_id:
                maybe_send_tutorial_tip(ctx, owner_id, 1)
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
        # IMPORTANT: non usare fake_update qui. query.message è un messaggio del bot,
        # quindi fake_update.effective_user diventerebbe il bot e l'auth fallirebbe.
        await show_shifts(update, ctx, date_iso)

        # Tutorial evoluto: prima consultazione (Cerca da calendario)
        try:
            if query.from_user:
                maybe_send_tutorial_tip(ctx, query.from_user.id, 2)
        except Exception:
            pass

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
        # Tutorial evoluto: completato quando chiude almeno un turno
        try:
            if owner_id:
                maybe_send_tutorial_tip(ctx, owner_id, 4)
        except Exception:
            pass
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
        cur.execute("""SELECT user_id, username, date_iso, org FROM shifts WHERE id=?""", (shift_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            await query.answer("Turno non trovato.", show_alert=True)
            return

        owner_id, owner_username, date_iso, shift_org = row
        requester = update.effective_user
        # blocca contatto cross-reparto
        requester_org = get_approved_org(requester.id) if requester else None
        log_event("contact_click", requester_id=(requester.id if requester else None), requester_org=(get_approved_org(requester.id) if requester else None), owner_id=owner_id, shift_org=shift_org, shift_id=shift_id)
        if requester_org and shift_org and requester_org != shift_org:
            log_event("contact_blocked_cross_org", requester_id=(requester.id if requester else None), requester_org=requester_org, shift_org=shift_org, shift_id=shift_id)
            await query.answer("Turno non visibile per il tuo reparto.", show_alert=True)
            return

        # Contatto diretto: forniamo username dell'autore + link t.me
        human = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%d/%m/%Y") if date_iso else ""

        # owner_username può essere "@handle" oppure nome completo (legacy). Accettiamo solo @handle.
        handle = None
        if owner_username and isinstance(owner_username, str) and owner_username.startswith("@") and len(owner_username) > 1:
            handle = owner_username[1:]

        if not handle:
            # fallback: prova a leggere username aggiornato dalla tabella users
            try:
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                cur.execute("SELECT username FROM users WHERE user_id=?", (owner_id,))
                r2 = cur.fetchone()
                conn.close()
                if r2 and r2[0] and isinstance(r2[0], str) and r2[0].startswith("@") and len(r2[0]) > 1:
                    handle = r2[0][1:]
            except Exception:
                handle = None

        if not handle:
            await query.message.reply_text(
                "⚠️ Non posso fornirti un contatto diretto perché l’autore non ha un username Telegram impostato.\n\n"
                "Suggerimento: chiedi all’autore di impostare uno username (Impostazioni → Username)."
            )
            log_event("contact_no_username", requester_id=(requester.id if requester else None), owner_id=owner_id, shift_id=shift_id)
            return

        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📩 Contatta autore", url=f"https://t.me/{handle}")]])
        await query.message.reply_text(
            f"👤 Autore turno ({human}): @{handle}",
            reply_markup=kb
        )
        log_event("contact_direct", requester_id=(requester.id if requester else None), owner_id=owner_id, shift_id=shift_id, owner_handle=handle)
        return

    # ---- APPROVE / REJECT / REVOKE ----
    if parts[0] in ("APPROVE", "REJECT", "REVOKE"):
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

        if parts[0] == "APPROVE":
            new_status = "approved"
        elif parts[0] == "REJECT":
            new_status = "rejected"
        else:
            # REVOKE: torna a pending
            new_status = "pending"

        log_event("user_status_change", admin_id=admin.id, org=org, target_uid=target_uid, action=parts[0], new_status=new_status)

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
                    text=(
                        f"✅ Approvato!\nReparto: *{ORG_LABELS.get(org, org)}*\n\n"
                        "Ora puoi usare il bot.\n\n"
                        "📌 Funzionamento rapido:\n"
                        "• Invia screenshot turno → scegli data\n"
                        "• Cerca turni con *Cerca* o *Date*\n"
                        "• Gestisci i tuoi con *I miei turni*\n\n"
                        "Se ti serve di nuovo la guida usa sempre:\n"
                        "👉 /tutorial"
                    ),
                    parse_mode="Markdown",
                    reply_markup=PRIVATE_KB
                )
            elif new_status == "rejected":
                await ctx.bot.send_message(
                    chat_id=target_uid,
                    text="⛔ Richiesta rifiutata. Se pensi sia un errore, contatta l’admin."
                )
            else:
                # pending (revoca)
                await ctx.bot.send_message(
                    chat_id=target_uid,
                    text=("⛔ La tua autorizzazione è stata *revocata* dall'admin del reparto.\n\n"
                          f"Per riattivarla, invia di nuovo: `/start {org}`"),
                    parse_mode="Markdown"
                )
        except Exception:
            pass

        action = parts[0]
        await query.edit_message_text(f"✅ Operazione completata: {action} → {new_status} (ID {target_uid})")
        return

# -------------------- Text router (private) --------------------
async def block_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Risponde ai testi non riconosciuti in privato, senza interferire con i comandi."""
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    # Se manca username, blocca e spiega come impostarlo
    if not await require_username(update):
        raise ApplicationHandlerStop
    await update.effective_message.reply_text("Usa i pulsanti 👇", reply_markup=PRIVATE_KB)

async def private_text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        return

    # Username obbligatorio anche per testi normali
    if not await require_username(update):
        raise ApplicationHandlerStop

    text = (update.effective_message.text or "").strip()

    # ✅ IMPORTANTISSIMO: non intercettare i comandi.
    # Fallback: se per qualche motivo /tutorial non viene trattato come comando (client/forward), gestiscilo qui.
    if text.startswith("/"):
        cmd = text.split()[0].lower()
        bot_username = (ctx.bot.username or "").lower()
        if cmd == "/tutorial" or (bot_username and cmd == f"/tutorial@{bot_username}"):
            await tutorial_cmd(update, ctx)
            raise ApplicationHandlerStop
        return

    low = text.lower()

    # Instrada SOLO i 3 pulsanti (case-insensitive)
    if low == "cerca":
        await search_cmd(update, ctx)
        raise ApplicationHandlerStop

    if low == "date":
        await dates_cmd(update, ctx)
        raise ApplicationHandlerStop

    if low in ("miei", "i miei turni"):
        await miei_cmd(update, ctx)
        raise ApplicationHandlerStop

    # Per qualsiasi altro testo: non rispondere qui (ci pensa block_text)
    return

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

    # -------------------- Global username gate (ANY /command) --------------------
    # Se l'utente non ha username, qualunque comando deve rispondere con istruzioni chiare.
    app.add_handler(
        MessageHandler(filters.COMMAND, _gate_username_for_commands),
        group=0
    )

    # -------------------- Global username gate (ANY text, non-command) --------------------
    # Se l'utente non ha username, qualunque messaggio di testo (anche non-comando) deve rispondere con istruzioni chiare.
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, _gate_username_for_texts),
        group=0
    )

    # -------------------- Comandi (DM) --------------------
    app.add_handler(CommandHandler("start", start), group=1)
    app.add_handler(CommandHandler("help", help_cmd), group=1)          # se ce l'hai
    app.add_handler(CommandHandler("version", version_cmd), group=1)    # se ce l'hai
    app.add_handler(CommandHandler("tutorial", tutorial_cmd), group=1)
    app.add_handler(CommandHandler("commands", commands_cmd), group=1)
    # Fallback robusto: intercetta anche /tutorial@BotName come testo (alcuni client/forward)
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.Regex(r"^/tutorial(?:@\\w+)?(?:\\s|$)"),
            tutorial_cmd
        ),
        group=1
    )
    app.add_handler(CommandHandler("myid", myid_cmd), group=1)
    app.add_handler(CommandHandler("pending", pending_cmd), group=1)    # se esiste davvero
    app.add_handler(CommandHandler("approved", approved_cmd), group=1)
    app.add_handler(CommandHandler("approvati", approved_cmd), group=1)
    app.add_handler(CommandHandler("approvedpdcfrna", approvedpdcfrna_cmd), group=1)
    app.add_handler(CommandHandler("approvedpdbfrna", approvedpdbfrna_cmd), group=1)
    app.add_handler(CommandHandler("admin2507", admin_cmd), group=1)
    app.add_handler(CommandHandler("logs", logs_cmd), group=1)
    app.add_handler(CommandHandler("stats", stats_cmd), group=1)
    app.add_handler(CommandHandler("revoke", revoke_cmd), group=1)
    app.add_handler(CommandHandler("cerca", search_cmd), group=1)
    app.add_handler(CommandHandler("date", dates_cmd), group=1)
    app.add_handler(CommandHandler("miei", miei_cmd), group=1)

    app.add_handler(CommandHandler("backupnow", backupnow_cmd), group=1)
    app.add_handler(CommandHandler("backupsend", backupsend_cmd), group=1)
    app.add_error_handler(on_error)

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
            filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND & ~filters.Regex("(?i)^(I miei turni|Cerca|Date)$"),
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
            # Backup DB: una volta dopo l'avvio + ogni giorno alle 03:30 (ora di Roma)
            jq.run_once(backup_job, when=60)
            jq.run_daily(backup_job, time=datetime.strptime("03:30", "%H:%M").time(), days=(0,1,2,3,4,5,6))
            # Tutorial reminder: (disabilitato)
        except Exception as e:
            print(f"[ShiftBot] Errore JobQueue: {e}")
    else:
        print("[ShiftBot] JobQueue non disponibile (installa python-telegram-bot[job-queue])")

    print("ShiftBot avviato.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()