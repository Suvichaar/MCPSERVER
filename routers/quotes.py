from fastapi import APIRouter
from services.quote_scraper import save_quotes_to_postgres_from_links
import psycopg2
import os

router = APIRouter()

@router.post("/scrape-from-db")
def scrape_from_db_pages():
    try:
        save_quotes_to_postgres_from_links()
        return {
            "status": "success",
            "message": "Quotes scraped and saved from qoutefancy_page_links."
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

@router.get("/count")
def get_quote_count():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM quote_scraped_data;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {
            "status": "success",
            "quote_count": count
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }