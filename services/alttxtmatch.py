import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def match_alttxt_and_store():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # ✅ Create match table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alttxt_match_table (
                id SERIAL PRIMARY KEY,
                custom_id TEXT,
                alttxt TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # ✅ Get image_fetched_data
        cur.execute("SELECT id, author, filename, cdn_url, timestamp FROM image_fetched_data;")
        image_data = cur.fetchall()
        image_df = pd.DataFrame(image_data, columns=["image_id", "author", "filename", "cdn_url", "image_timestamp"])
        image_df["custom_id"] = image_df["filename"].str.replace(".jpg", "", regex=False)

        # ✅ Get custom_id and alttxt from processed image alt table
        cur.execute("SELECT custom_id, alttxt FROM image_batch_processed_data;")
        alttxt_rows = cur.fetchall()
        alttxt_df = pd.DataFrame(alttxt_rows, columns=["custom_id", "alttxt"])

        # ✅ Merge & filter valid matches
        merged_df = pd.merge(image_df, alttxt_df, on="custom_id", how="left")
        matched_df = merged_df[merged_df["alttxt"].notna()].copy()

        # ✅ Save matched entries into alttxt_match_table
        cur.executemany("""
            INSERT INTO alttxt_match_table (custom_id, alttxt)
            VALUES (%s, %s);
        """, matched_df[["custom_id", "alttxt"]].values.tolist())

        # ✅ Drop unmatched rows from alttxt_match_table
        cur.execute("DELETE FROM alttxt_match_table WHERE alttxt = 'NA';")

        # ✅ Create final processed table with status_resizer field
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alttxt_processed_data (
                id SERIAL PRIMARY KEY,
                image_id INTEGER,
                author TEXT,
                filename TEXT,
                cdn_url TEXT,
                alttxt TEXT,
                status_resizer BOOLEAN DEFAULT FALSE,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # ✅ Insert joined results into final processed table
        insert_data = matched_df[["image_id", "author", "filename", "cdn_url", "alttxt"]].copy()
        insert_data["status_resizer"] = False
        insert_data["timestamp"] = datetime.utcnow()

        cur.executemany("""
            INSERT INTO alttxt_processed_data (
                image_id, author, filename, cdn_url, alttxt, status_resizer, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, insert_data.values.tolist())

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "matched_rows": len(matched_df),
            "total_checked": len(image_df)
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}