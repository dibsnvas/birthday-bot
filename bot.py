import os, sqlite3, logging, datetime as dt, csv
from zoneinfo import ZoneInfo
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from dotenv import load_dotenv

load_dotenv()
TZ = ZoneInfo("Asia/Almaty")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB = "birthdays.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def db():
    conn = sqlite3.connect(DB)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS birthdays(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        date TEXT NOT NULL,           -- YYYY-MM-DD
        days_before INTEGER DEFAULT 0 -- напоминать за N дней (0 = только в день Х)
      );
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_birthdays_chat_name_date ON birthdays(chat_id,name,date);")
    return conn

def add_bday(chat_id: int, name: str, date_str: str, days_before: int):
    conn = db()
    conn.execute("INSERT OR IGNORE INTO birthdays(chat_id, name, date, days_before) VALUES(?,?,?,?)",
                 (chat_id, name.strip(), date_str, int(days_before)))
    conn.commit(); conn.close()

def list_bday(chat_id: int):
    conn = db()
    rows = conn.execute("SELECT id, name, date, days_before FROM birthdays WHERE chat_id=? ORDER BY date",
                        (chat_id,)).fetchall()
    conn.close()
    return rows

def remove_bday(chat_id: int, ident: str):
    conn = db()
    if ident.isdigit():
        cur = conn.execute("DELETE FROM birthdays WHERE chat_id=? AND id=?", (chat_id, int(ident)))
    else:
        cur = conn.execute("DELETE FROM birthdays WHERE chat_id=? AND name=?", (chat_id, ident.strip()))
    cnt = cur.rowcount
    conn.commit(); conn.close()
    return cnt

def is_leap(y: int) -> bool:
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)

def next_occurrence(bday: dt.date, today: dt.date) -> dt.date:
    year = today.year
    m, d = bday.month, bday.day
    if m == 2 and d == 29:
        try_date = dt.date(year, 2, 29) if is_leap(year) else dt.date(year, 2, 28)
    else:
        try_date = dt.date(year, m, d)
    if try_date < today:
        year += 1
        if m == 2 and d == 29:
            try_date = dt.date(year, 2, 29) if is_leap(year) else dt.date(year, 2, 28)
        else:
            try_date = dt.date(year, m, d)
    return try_date

async def send_due_birthdays(app: Application):
    today = dt.datetime.now(TZ).date()
    conn = db()
    rows = conn.execute("SELECT chat_id, name, date, days_before FROM birthdays").fetchall()
    conn.close()

    by_chat = {}
    for chat_id, name, date_str, days_before in rows:
        y, m, d = map(int, date_str.split("-"))
        bday = dt.date(y, m, d)
        next_date = next_occurrence(bday, today)
        delta = (next_date - today).days
        if delta == 0 or delta == int(days_before):
            by_chat.setdefault(chat_id, []).append((name, next_date, int(days_before), delta))

    for chat_id, items in by_chat.items():
        lines_now, lines_ahead = [], []
        for name, dateX, days_before, delta in items:
            if delta == 0:
                lines_now.append(f"🎉 Сегодня день рождение у {name}! Поздравляем🥳🥳🥳")
            else:
                lines_ahead.append(f"⏰ Через {days_before} дн. — ДР у {name} ({dateX.strftime('%d.%m')})")
        parts = []
        if lines_now: parts.append("\n".join(lines_now))
        if lines_ahead: parts.append("\n".join(lines_ahead))
        if parts:
            await app.bot.send_message(chat_id=chat_id, text="\n\n".join(parts))

def schedule_jobs(app: Application):
    sched = BackgroundScheduler(timezone=str(TZ))
    sched.add_job(lambda: app.create_task(send_due_birthdays(app)),
                  CronTrigger(minute="*"))
    sched.start()
    logging.info("Scheduler started for 09:00 Asia/Almaty")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hello, I am a chatbot that notifies you about the birthdays of nFactorial School employees\n"
    )

def parse_args(text: str):
    parts = text.strip().split(maxsplit=3)
    if len(parts) < 3:
        return None
    name = parts[1]
    date_str = parts[2]
    days = 0
    if len(parts) == 4:
        try:
            days = int(parts[3])
        except:
            pass
    return name, date_str, days

async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    args = parse_args(update.message.text)
    if not args:
        await update.message.reply_text("Формат: /add Имя YYYY-MM-DD [days_before]")
        return
    name, date_str, days = args
    try:
        dt.date.fromisoformat(date_str)
    except ValueError:
        await update.message.reply_text("Дата должна быть в формате YYYY-MM-DD")
        return
    add_bday(update.message.chat_id, name, date_str, days)
    await update.message.reply_text(f"Ок, добавил: {name} — {date_str} (за {days} дн.)")

async def test_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_due_birthdays(context.application)
    await update.message.reply_text("✓ Проверил и отправил все актуальные напоминания.")

async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = list_bday(update.message.chat_id)
    if not rows:
        await update.message.reply_text("Пока пусто. Добавь кого-нибудь через /add")
        return
    lines = [f"{i+1}. {r[1]} — {r[2]} (за {r[3]} дн.)" for i, r in enumerate(rows)]
    await update.message.reply_text("Список дней рождении:\n" + "\n".join(lines))

async def import_local_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    path = "birthdays.csv"
    if not os.path.exists(path):
        await update.message.reply_text("Файл birthdays.csv не найден рядом с ботом.")
        return

    added = 0
    conn = db()
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
            except csv.Error:
                dialect = csv.excel
            reader = csv.DictReader(f, dialect=dialect, skipinitialspace=True)

            if not reader.fieldnames:
                await update.message.reply_text("Не удалось прочитать заголовки CSV.")
                conn.close(); return

            keymap = { (k or "").strip().lower(): k for k in reader.fieldnames }

            name_key = keymap.get("name") or keymap.get("имя")
            date_key = keymap.get("date") or keymap.get("дата")
            days_key = keymap.get("days_before") or keymap.get("daysbefore") or keymap.get("days") or keymap.get("дни") or keymap.get("за_дней")

            if not name_key or not date_key:
                await update.message.reply_text(
                    f"Ожидались колонки name,date[,days_before]. Найдены: {reader.fieldnames}"
                )
                conn.close(); return

            for row in reader:
                if not row.get(name_key) and not row.get(date_key):
                    continue
                name = (row.get(name_key) or "").strip()
                date_str = (row.get(date_key) or "").strip()
                if not name or not date_str:
                    continue
                try:
                    dt.date.fromisoformat(date_str)
                except ValueError:
                    continue
                days = 0
                if days_key and (row.get(days_key) or "").strip():
                    try:
                        days = int((row.get(days_key) or "0").strip())
                    except ValueError:
                        days = 0

                conn.execute(
                    "INSERT OR IGNORE INTO birthdays(chat_id, name, date, days_before) VALUES(?,?,?,?)",
                    (chat_id, name, date_str, days)
                )
                added += 1

        conn.commit()
        await update.message.reply_text(f"Импортировано: {added}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка импорта: {e}")
    finally:
        conn.close()

async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    parts = update.message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Формат: /remove <id|Имя>")
        return
    ident = parts[1]
    cnt = remove_bday(update.message.chat_id, ident)
    await update.message.reply_text("Удалил." if cnt > 0 else "Ничего не нашёл.")

from datetime import time, datetime, timedelta

async def _due_job(context: ContextTypes.DEFAULT_TYPE):
    await send_due_birthdays(context.application)

def schedule_jobs(app: Application):
    app.job_queue.run_daily(
        _due_job,
        time=time(hour=9, minute=0, tzinfo=TZ),
        name="daily_bdays"
    )


    logging.info("JobQueue scheduled: daily at 09:00 Asia/Almaty")

def main():
    token = BOT_TOKEN
    if not token:
        raise RuntimeError("Нет BOT_TOKEN в .env")
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("remove", remove_cmd))
    app.add_handler(CommandHandler("import_local", import_local_cmd))
    app.add_handler(CommandHandler("test_now", test_now))
    schedule_jobs(app)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
