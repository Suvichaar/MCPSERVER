import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import psycopg2
import os
import uuid
from dotenv import load_dotenv

# Load .env credentials
load_dotenv()

def extract_slug_from_url(url):
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    return path.split("/")[0] if path else ""

def create_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/90.0.4430.93 Safari/537.36"
        )
    })
    return session

def scrape_quotes_for_slug(slug, max_pages=10):
    session = create_session()
    quotes = []
    serial_number = 1

    for page_number in range(1, max_pages + 1):
        url = f"https://quotefancy.com/{slug}/page/{page_number}"
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[ERROR] Page fetch failed: {url} -> {e}")
            break

        soup = BeautifulSoup(response.content, "html.parser")
        containers = soup.find_all("div", class_="q-wrapper")
        if not containers:
            break

        for container in containers:
            quote_div = container.find("div", class_="quote-a")
            quote_text = quote_div.get_text(strip=True) if quote_div \
                else container.find("a", class_="quote-a").get_text(strip=True)

            quote_link = ""
            if quote_div and quote_div.find("a"):
                quote_link = quote_div.find("a").get("href", "")
            elif container.find("a", class_="quote-a"):
                quote_link = container.find("a", class_="quote-a").get("href", "")
            quote_link = urljoin("https://quotefancy.com", quote_link)

            author_div = container.find("div", class_="author-p bylines")
            if author_div:
                author_text = author_div.get_text(strip=True).replace("by ", "").strip()
            else:
                author_p = container.find("p", class_="author-p")
                author_text = author_p.find("a").get_text(strip=True) if author_p and author_p.find("a") else "Anonymous"

            quotes.append({
                "serial": serial_number,
                "quote": quote_text,
                "link": quote_link,
                "author": author_text
            })
            serial_number += 1

        time.sleep(1)

    return quotes

def save_quotes_to_postgres_from_links():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quote_scraped_data (
            id SERIAL PRIMARY KEY,
            page_id INTEGER,
            quote TEXT NOT NULL,
            author_name TEXT,
            quote_link TEXT,
            page_link TEXT,
            scrape_id TEXT,
            text_structure_status TEXT DEFAULT 'Pending',
            text_structure_id TEXT,
            author_image_check TEXT DEFAULT 'Unchecked',
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (quote, author_name)
        );
    """)

    # Fetch 15 pages to scrape
    cur.execute("""
        SELECT page_id, page_link
        FROM qoutefancy_page_links 
        WHERE scraped_status = false
        LIMIT 15;
    """)
    pages = cur.fetchall()

    if not pages:
        print("No new pages to scrape.")
        return

    # Generate a single UUID for this batch
    text_structure_id = str(uuid.uuid4())
    scrape_id = text_structure_id  # ✅ Set scrape_id same as text_structure_id
    print(f"🔗 Batch ID (scrape_id = text_structure_id): {scrape_id}")

    for page_id, page_link in pages:
        slug = extract_slug_from_url(page_link)
        quotes = scrape_quotes_for_slug(slug, max_pages=10)

        for q in quotes:
            cur.execute("""
                INSERT INTO quote_scraped_data (
                    page_id, quote, author_name, quote_link, page_link,
                    scrape_id, text_structure_status, text_structure_id,
                    author_image_check, timestamp
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, 'Pending', %s, 'Unchecked', NOW()
                )
                ON CONFLICT (quote, author_name) DO NOTHING;
            """, (
                page_id,
                q["quote"],
                q["author"],
                q["link"],
                page_link,
                scrape_id,
                text_structure_id
            ))

        # ✅ Mark the page as scraped
        cur.execute("""
            UPDATE qoutefancy_page_links
            SET scraped_status = true
            WHERE page_id = %s;
        """, (page_id,))

        print(f"✅ {len(quotes)} quotes saved from: {page_link}")

    conn.commit()
    cur.close()
    conn.close()
    print("🚀 Batch scraping completed for 15 pages.")