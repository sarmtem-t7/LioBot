"""
Импорт контента с lio-int.com и lio-blog.com в БД LioBot.

Использование:
    python import_content.py audiobooks   # аудиокниги
    python import_content.py articles     # статьи блога
    python import_content.py radio        # радио-станции
    python import_content.py magazines    # издания журналов
    python import_content.py all          # всё подряд

Окружение:
    GROQ_API_KEY (или GroqApiKey)  — для AI-генерации тегов
    LIOBOT_DB_PATH                 — путь к liobot.db (по умолчанию bin/Debug/net8.0/liobot.db)
"""
import os
import sys
import re
import sqlite3
import json
import html as html_lib
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from typing import Optional, List
import requests

GROQ_API_KEY = os.environ.get("GROQ_API_KEY") or os.environ.get("GroqApiKey")
DB_PATH = os.environ.get("LIOBOT_DB_PATH", "bin/Debug/net8.0/liobot.db")
UA = "Mozilla/5.0 (compatible; LioBot-Importer/1.0)"
SITEMAP_MAIN = "https://www.lio-int.com/sitemap.xml"
SITEMAP_BLOG = "https://lio-blog.com/sitemap.xml"

META_RE = {
    "og_title":       re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)', re.I),
    "og_description": re.compile(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)', re.I),
    "og_image":       re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', re.I),
    "description":    re.compile(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', re.I),
    "title":          re.compile(r'<title>([^<]+)</title>', re.I),
}


def fetch(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  [fetch] {url} → {e}")
        return None


def extract_meta(html: str) -> dict:
    out = {}
    for key, rx in META_RE.items():
        m = rx.search(html)
        if m:
            out[key] = html_lib.unescape(m.group(1)).strip()
    return out


def sitemap_urls(sitemap_url: str, prefix: Optional[str] = None) -> List[str]:
    xml = fetch(sitemap_url)
    if not xml:
        return []
    urls = re.findall(r"<loc>([^<]+)</loc>", xml)
    if prefix:
        urls = [u for u in urls if prefix in u]
    return urls


def ai_tags(title: str, description: str) -> str:
    if not GROQ_API_KEY:
        return ""
    prompt = (
        f"Название: {title}\nОписание: {description}\n\n"
        "Верни 3–6 тегов через запятую на русском. Только теги, без пояснений. "
        "Используй темы: молитва, семья, отношения, вера, Библия, воспитание, "
        "апологетика, миссия, свидетельство, проповедь, поэзия, история, дети, подростки."
    )
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3},
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"].strip()
        return re.sub(r"[^а-яА-Яa-zA-Z0-9,\s-]", "", text)
    except Exception as e:
        print(f"  [ai] {e}")
        return ""


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def url_exists(conn, url: str) -> bool:
    cur = conn.execute("SELECT 1 FROM Books WHERE Url = ? LIMIT 1", (url,))
    return cur.fetchone() is not None


def insert_content(conn, *, title, author, description, tags, url, type_, audio_url=""):
    conn.execute(
        """INSERT INTO Books (Title, Author, Description, Tags, Url, Type, AudioUrl, AiAnnotation)
           VALUES (?, ?, ?, ?, ?, ?, ?, '')""",
        (title, author, description, tags, url, type_, audio_url),
    )


# ───────────────────────────── Аудиокниги ─────────────────────────────
def import_audiobooks(conn) -> int:
    urls = sitemap_urls(SITEMAP_MAIN, "/audioknigi/")
    # Отсекаем служебные страницы-листинги
    urls = [u for u in urls if urlparse(u).path.count("/") >= 2 and "/audioknigi-" not in u]
    print(f"[audiobooks] {len(urls)} URL в sitemap")
    added = 0
    for i, url in enumerate(urls, 1):
        if url_exists(conn, url):
            continue
        html = fetch(url)
        if not html:
            continue
        meta = extract_meta(html)
        title = meta.get("og_title") or meta.get("title") or url.rsplit("/", 1)[-1]
        author = meta.get("og_description") or meta.get("description") or ""
        description = meta.get("description") or author
        tags = ai_tags(title, description)
        insert_content(conn, title=title, author=author, description=description,
                       tags=tags, url=url, type_="audio", audio_url=url)
        conn.commit()
        added += 1
        print(f"  [{i}/{len(urls)}] + {title}")
    return added


# ───────────────────────────── Статьи блога ─────────────────────────────
def import_articles(conn) -> int:
    urls = sitemap_urls(SITEMAP_BLOG)
    # Отбрасываем category/tag/author
    urls = [u for u in urls if re.search(r"/\d{4}/\d{2}/\d{2}/", u)]
    print(f"[articles] {len(urls)} URL в sitemap")
    added = 0
    for i, url in enumerate(urls, 1):
        if url_exists(conn, url):
            continue
        html = fetch(url)
        if not html:
            continue
        meta = extract_meta(html)
        title = meta.get("og_title") or meta.get("title") or ""
        # Заголовок блога обычно "Название — Вера & Жизнь", чистим
        title = re.sub(r"\s*[—–-]\s*Вера\s*&\s*Жизнь.*$", "", title)
        description = meta.get("og_description") or meta.get("description") or ""
        tags = ai_tags(title, description)
        insert_content(conn, title=title, author="", description=description,
                       tags=tags, url=url, type_="article")
        conn.commit()
        added += 1
        print(f"  [{i}/{len(urls)}] + {title}")
    return added


# ───────────────────────────── Радио-станции ─────────────────────────────
def import_radio(conn) -> int:
    urls = sitemap_urls(SITEMAP_MAIN, "/radio/")
    urls = [u for u in urls if urlparse(u).path.count("/") >= 2]
    print(f"[radio] {len(urls)} URL в sitemap")
    added = 0
    for url in urls:
        if url_exists(conn, url):
            continue
        html = fetch(url)
        if not html:
            continue
        meta = extract_meta(html)
        title = meta.get("og_title") or meta.get("title") or url.rsplit("/", 1)[-1]
        description = meta.get("og_description") or meta.get("description") or ""
        insert_content(conn, title=title, author="", description=description,
                       tags="радио", url=url, type_="radio", audio_url=url)
        conn.commit()
        added += 1
        print(f"  + {title}")
    return added


# ───────────────────────────── Журналы (издания) ─────────────────────────────
def import_magazines(conn) -> int:
    """Создаёт записи в таблице Magazines. Выпуски парсятся отдельно (TODO)."""
    urls = sitemap_urls(SITEMAP_MAIN, "/zurnaly/")
    # Оставляем только русские основные: vera, tropinka, menora (+audio)
    keep = re.compile(r"/zurnaly/(vera|tropinka|menora)(-audio)?/?$")
    urls = [u for u in urls if keep.search(u)]
    print(f"[magazines] {len(urls)} изданий")
    added = 0
    for url in urls:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        cur = conn.execute("SELECT 1 FROM Magazines WHERE Slug = ?", (slug,))
        if cur.fetchone():
            continue
        html = fetch(url)
        title = slug
        if html:
            meta = extract_meta(html)
            title = meta.get("og_title") or meta.get("title") or slug
        conn.execute(
            "INSERT INTO Magazines (Slug, Title, Url) VALUES (?, ?, ?)",
            (slug, title, url),
        )
        conn.commit()
        added += 1
        print(f"  + {title}")
    return added


# ───────────────────────────── main ─────────────────────────────
def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    target = sys.argv[1]
    if not os.path.exists(DB_PATH):
        sys.exit(f"DB не найдена: {DB_PATH} (задай LIOBOT_DB_PATH)")

    conn = connect_db()
    try:
        if target in ("audiobooks", "all"):
            print(f"→ добавлено аудиокниг: {import_audiobooks(conn)}")
        if target in ("articles", "all"):
            print(f"→ добавлено статей: {import_articles(conn)}")
        if target in ("radio", "all"):
            print(f"→ добавлено радио: {import_radio(conn)}")
        if target in ("magazines", "all"):
            print(f"→ добавлено изданий: {import_magazines(conn)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
