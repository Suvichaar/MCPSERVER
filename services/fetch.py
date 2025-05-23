import os
import httpx
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def fetch_and_store_pending_batches():
    try:
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        api_version = "2025-03-01-preview"
        headers = {"api-key": api_key}

        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT batch_id, jsonl_file FROM batch_process_tracker_data
            WHERE batch_completion_status = 'processing'
            LIMIT 15;
        """)
        pending_batches = cur.fetchall()
        if not pending_batches:
            return {"status": "no_pending_batches"}

        text_rows = []
        image_rows = []

        for batch_id, jsonl_file in pending_batches:
            if not jsonl_file:
                continue

            metadata_url = f"{endpoint}/openai/batches/{batch_id}?api-version={api_version}"
            meta_resp = httpx.get(metadata_url, headers=headers)
            if meta_resp.status_code != 200:
                continue

            output_file_id = meta_resp.json().get("output_file_id")
            if not output_file_id:
                continue

            download_url = f"{endpoint}/openai/files/{output_file_id}/content?api-version={api_version}"
            resp = httpx.get(download_url, headers=headers)
            if resp.status_code != 200:
                continue

            for line in resp.text.strip().splitlines():
                data = json.loads(line)
                custom_id = data.get("custom_id")
                message = data.get("response", {}).get("body", {}).get("choices", [{}])[0].get("message", {})
                content = message.get("content")

                if not custom_id or not content:
                    continue

                if jsonl_file.startswith("quotefancy_azure_batch"):
                    try:
                        content_json = json.loads(content)
                        text_rows.append((
                            custom_id,
                            content_json.get("storytitle", ""),
                            content_json.get("metadescription", ""),
                            content_json.get("metakeywords", ""),
                            datetime.utcnow()
                        ))
                    except json.JSONDecodeError:
                        continue

                elif jsonl_file.startswith("image_alt_batch"):
                    image_rows.append((custom_id, content, datetime.utcnow()))

            # ✅ Mark the batch as completed
            cur.execute("""
                UPDATE batch_process_tracker_data
                SET batch_completion_status = 'completed'
                WHERE batch_id = %s;
            """, (batch_id,))

        # ✅ Store textual data
        if text_rows:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS template1_text_batch_processed_data (
                    id SERIAL PRIMARY KEY,
                    batch_custom_id TEXT,
                    storytitle TEXT,
                    metadescription TEXT,
                    metakeywords TEXT,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.executemany("""
                INSERT INTO template1_text_batch_processed_data (
                    batch_custom_id, storytitle, metadescription, metakeywords, timestamp
                ) VALUES (%s, %s, %s, %s, %s);
            """, text_rows)

        # ✅ Store image ALT text data
        if image_rows:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS image_batch_processed_data (
                    id SERIAL PRIMARY KEY,
                    custom_id TEXT,
                    alttxt TEXT,
                    merged_status TEXT DEFAULT 'Pending',
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.executemany("""
                INSERT INTO image_batch_processed_data (
                    custom_id, alttxt, merged_status, timestamp
                ) VALUES (%s, %s, %s, %s);
            """, [(cid, alt, "Pending", ts) for (cid, alt, ts) in image_rows])

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "batches_processed": len(pending_batches),
            "text_entries_saved": len(text_rows),
            "image_entries_saved": len(image_rows)
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}