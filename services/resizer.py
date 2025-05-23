import os
import psycopg2
import pandas as pd
import json
import base64
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def generate_resized_urls():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # ✅ Create resized output table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS resized_url_data (
                id SERIAL PRIMARY KEY,
                author TEXT,
                filename TEXT,
                cdn_url TEXT,
                alttxt TEXT,
                potraightcoverurl TEXT,
                landscapecoverurl TEXT,
                squarecoverurl TEXT,
                socialthumbnailcoverurl TEXT,
                nextstoryimageurl TEXT,
                standardurl TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # ✅ Get unprocessed rows from alttxt_processed_data
        cur.execute("""
            SELECT id, author, filename, cdn_url, alttxt 
            FROM alttxt_processed_data 
            WHERE status_resizer = false;
        """)
        rows = cur.fetchall()

        if not rows:
            return {"status": "no_data", "message": "No unprocessed rows found in alttxt_processed_data."}

        df = pd.DataFrame(rows, columns=["id", "author", "filename", "cdn_url", "alttxt"])

        # ✅ Remove rows where filename ends with "1.jpg"
        df = df[~df["filename"].str.endswith("1.jpg")]

        if df.empty:
            return {"status": "filtered_all", "message": "All rows ended with 1.jpg and were excluded."}

        resize_presets = {
            "potraightcoverurl": (640, 853),
            "landscapecoverurl": (853, 640),
            "squarecoverurl": (800, 800),
            "socialthumbnailcoverurl": (300, 300),
            "nextstoryimageurl": (315, 315),
            "standardurl": (720, 1200)
        }

        cdn_prefix_media = "https://media.suvichaar.org/"
        cdn_prefix_cdn = "https://cdn.suvichaar.org/"

        for preset_name, (width, height) in resize_presets.items():
            urls = []
            for url in df["cdn_url"]:
                try:
                    if url.startswith(cdn_prefix_cdn):
                        url = url.replace(cdn_prefix_cdn, cdn_prefix_media)
                    key_path = url.replace(cdn_prefix_media, "")
                    template = {
                        "bucket": "suvichaarapp",
                        "key": key_path,
                        "edits": {
                            "resize": {
                                "width": width,
                                "height": height,
                                "fit": "cover"
                            }
                        }
                    }
                    encoded = base64.urlsafe_b64encode(json.dumps(template).encode()).decode()
                    urls.append(f"{cdn_prefix_media}{encoded}")
                except Exception:
                    urls.append("ERROR")
            df[preset_name] = urls

        # ✅ Insert transformed results
        cur.executemany("""
            INSERT INTO resized_url_data (
                author, filename, cdn_url, alttxt,
                potraightcoverurl, landscapecoverurl, squarecoverurl,
                socialthumbnailcoverurl, nextstoryimageurl, standardurl, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, df[[
            "author", "filename", "cdn_url", "alttxt",
            "potraightcoverurl", "landscapecoverurl", "squarecoverurl",
            "socialthumbnailcoverurl", "nextstoryimageurl", "standardurl"
        ]].assign(timestamp=datetime.utcnow()).values.tolist())

        # ✅ Mark processed rows only (based on filename NOT ending with "1.jpg")
        processed_filenames = df["filename"].tolist()
        cur.execute("""
            UPDATE alttxt_processed_data 
            SET status_resizer = TRUE 
            WHERE filename = ANY(%s);
        """, (processed_filenames,))

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "processed_count": len(df)}

    except Exception as e:
        return {"status": "error", "detail": str(e)}