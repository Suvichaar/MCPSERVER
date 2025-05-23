from fastapi import APIRouter, HTTPException
import pandas as pd
import psycopg2
import uuid
import os
from dotenv import load_dotenv
from datetime import datetime

router = APIRouter()
load_dotenv()

@router.post("/structure")
def structure_quotes_clean_na():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        cur.execute("""
            SELECT text_structure_id, quote, author_name
            FROM quote_scraped_data
            WHERE text_structure_status = 'Pending';
        """)
        rows = cur.fetchall()

        if not rows:
            return {"status": "success", "message": "No pending quotes found."}

        df = pd.DataFrame(rows, columns=["text_structure_id", "quote", "author_name"])
        df = df[df["quote"].apply(lambda x: isinstance(x, str) and len(x.strip()) <= 180)]

        used_quotes = set()
        grouped = []

        # Sort all unique text_structure_ids to assign t1, t2, t3...
        unique_batches = list(df["text_structure_id"].unique())
        batch_task_ids = {
            batch_id: f"{batch_id[:8]}-t{i+1}" for i, batch_id in enumerate(unique_batches)
        }

        for (batch_id, author), group in df.groupby(["text_structure_id", "author_name"]):
            quotes = group["quote"].dropna().tolist()
            task_id = batch_task_ids[batch_id]
            author_clean = author.replace(" ", "-")
            author_counter = 1

            for i in range(0, len(quotes), 8):
                chunk = quotes[i:i + 8]
                chunk += ['NA'] * (8 - len(chunk))
                if "NA" in chunk:
                    continue
                used_quotes.update(chunk)

                group_index = i // 8 + 1
                batch_custom_id = f"{batch_id[:8]}-{group_index}-{author_clean}-{author_counter}"

                grouped.append({
                    "text_structure_id": batch_id,
                    "batch_custom_id": batch_custom_id,
                    "s2paragraph1": chunk[0], "s3paragraph1": chunk[1], "s4paragraph1": chunk[2],
                    "s5paragraph1": chunk[3], "s6paragraph1": chunk[4], "s7paragraph1": chunk[5],
                    "s8paragraph1": chunk[6], "s9paragraph1": chunk[7],
                    "author_name": author,
                    "batch_type": "Auto",
                    "batch_task_id": task_id,
                    "timestamp": datetime.utcnow(),
                    "batch_created": False
                })
                author_counter += 1

        if not grouped:
            return {"status": "success", "message": "No complete 8-quote groups found."}

        final_df = pd.DataFrame(grouped)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS template1_text_structure_data (
                id SERIAL PRIMARY KEY,
                text_structure_id UUID,
                batch_custom_id TEXT,
                s2paragraph1 TEXT,
                s3paragraph1 TEXT,
                s4paragraph1 TEXT,
                s5paragraph1 TEXT,
                s6paragraph1 TEXT,
                s7paragraph1 TEXT,
                s8paragraph1 TEXT,
                s9paragraph1 TEXT,
                author_name TEXT,
                batch_type TEXT,
                batch_task_id TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                batch_created BOOLEAN DEFAULT FALSE
            );
        """)

        for _, row in final_df.iterrows():
            cur.execute("""
                INSERT INTO template1_text_structure_data (
                    text_structure_id, batch_custom_id,
                    s2paragraph1, s3paragraph1, s4paragraph1, s5paragraph1,
                    s6paragraph1, s7paragraph1, s8paragraph1, s9paragraph1,
                    author_name, batch_type, batch_task_id, timestamp, batch_created
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["text_structure_id"], row["batch_custom_id"],
                row["s2paragraph1"], row["s3paragraph1"], row["s4paragraph1"], row["s5paragraph1"],
                row["s6paragraph1"], row["s7paragraph1"], row["s8paragraph1"], row["s9paragraph1"],
                row["author_name"], row["batch_type"], row["batch_task_id"], row["timestamp"], row["batch_created"]
            ))

        if used_quotes:
            placeholders = ','.join(['%s'] * len(used_quotes))
            cur.execute(f"""
                UPDATE quote_scraped_data
                SET text_structure_status = 'Completed'
                WHERE quote IN ({placeholders});
            """, tuple(used_quotes))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "rows_structured": len(final_df),
            "batches_created": final_df["text_structure_id"].nunique(),
            "authors_structured": final_df["author_name"].nunique()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
