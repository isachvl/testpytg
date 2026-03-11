import sqlite3
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse
 
import feedparser


DB_PATH = "news.db"

# Список источников.
# Потом легко заменишь или добавишь свои.
SOURCES = [
    {
        "code": "rbc",
        "name": "РБК",
        "feed_url": "https://rssexport.rbc.ru/rbcnews/news/30/full.rss",
    },
    {
        "code": "bbc",
        "name": "BBC News",
        "feed_url": "http://feeds.bbci.co.uk/news/rss.xml",
    },
    {
        "code": "guardian",
        "name": "The Guardian World",
        "feed_url": "https://www.theguardian.com/world/rss",
    },
]


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            feed_url TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS headlines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            domain TEXT NOT NULL,
            published_at TEXT,
            fetched_at TEXT NOT NULL,
            item_hash TEXT NOT NULL UNIQUE,
            FOREIGN KEY (source_id) REFERENCES sources(id)
        )
    """)

    conn.commit()
    conn.close()


def seed_sources():
    conn = get_connection()
    cur = conn.cursor()

    for source in SOURCES:
        cur.execute("""
            INSERT INTO sources (code, name, feed_url)
            VALUES (?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name = excluded.name,
                feed_url = excluded.feed_url
        """, (source["code"], source["name"], source["feed_url"]))

    conn.commit()
    conn.close()


def get_sources():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, code, name, feed_url FROM sources ORDER BY id")
    rows = cur.fetchall()

    conn.close()
    return rows


def make_item_hash(source_code: str, title: str, url: str) -> str:
    raw = f"{source_code}|{title.strip()}|{url.strip()}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def extract_published_at(entry) -> str | None:
    """
    Пробуем достать дату публикации из RSS.
    Если даты нет — вернем None.
    """
    published_parsed = getattr(entry, "published_parsed", None)
    updated_parsed = getattr(entry, "updated_parsed", None)

    value = published_parsed or updated_parsed
    if not value:
        return None

    dt = datetime(*value[:6], tzinfo=timezone.utc)
    return dt.isoformat()


def get_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.replace("www.", "") if parsed.netloc else ""


def save_headline(source_id: int, source_code: str, title: str, url: str, published_at: str | None):
    conn = get_connection()
    cur = conn.cursor()

    item_hash = make_item_hash(source_code, title, url)
    fetched_at = datetime.now(timezone.utc).isoformat()
    domain = get_domain(url)

    cur.execute("""
        INSERT OR IGNORE INTO headlines (
            source_id, title, url, domain, published_at, fetched_at, item_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        source_id,
        title,
        url,
        domain,
        published_at,
        fetched_at,
        item_hash
    ))

    conn.commit()
    conn.close()


def parse_source(source_id: int, source_code: str, source_name: str, feed_url: str):
    print(f"\nПарсим: {source_name} ({feed_url})")

    feed = feedparser.parse(feed_url)

    if getattr(feed, "bozo", 0):
        print(f"  [!] RSS прочитан с предупреждением")

    entries = getattr(feed, "entries", [])
    print(f"  Найдено записей: {len(entries)}")

    saved_count = 0

    for entry in entries:
        title = getattr(entry, "title", "").strip()
        url = getattr(entry, "link", "").strip()

        if not title or not url:
            continue

        published_at = extract_published_at(entry)

        # До сохранения проверим, нет ли уже такой записи
        item_hash = make_item_hash(source_code, title, url)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id FROM headlines WHERE item_hash = ?", (item_hash,))
        exists = cur.fetchone()
        conn.close()

        if exists:
            continue

        save_headline(
            source_id=source_id,
            source_code=source_code,
            title=title,
            url=url,
            published_at=published_at
        )
        saved_count += 1

    print(f"  Сохранено новых: {saved_count}")


def print_last_news(limit: int = 10):
    """
    Просто для проверки покажем последние записи из БД.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT s.name, h.title, h.url, h.published_at, h.fetched_at
        FROM headlines h
        JOIN sources s ON s.id = h.source_id
        ORDER BY COALESCE(h.published_at, h.fetched_at) DESC
        LIMIT ?
    """, (limit,))

    rows = cur.fetchall()
    conn.close()

    print("\nПоследние новости в БД:")
    print("-" * 80)

    """for source_name, title, url, published_at, fetched_at in rows:
        print(f"Источник: {source_name}")
        print(f"Заголовок: {title}")
        print(f"Ссылка: {url}")
        print(f"Published: {published_at}")
        print(f"Fetched:   {fetched_at}")
        print("-" * 80)"""


def main():
    create_tables()
    seed_sources()

    sources = get_sources()

    for source_id, source_code, source_name, feed_url in sources:
        parse_source(source_id, source_code, source_name, feed_url)

    print_last_news(limit=15)


if __name__ == "__main__":
    main()