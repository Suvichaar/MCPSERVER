import os
import json
import uuid
import pandas as pd
import psycopg2
import httpx
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def generate_and_upload_batch():
    try:
        deployment_model = "gpt-4o-global-batch"
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        # ✅ Step 1: Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # ✅ Step 1.0: Ensure tracker table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS batch_process_tracker_data (
                id SERIAL PRIMARY KEY,
                batch_task_id TEXT,
                batch_type TEXT,
                batch_id TEXT,
                file_id TEXT,
                jsonl_file TEXT,
                csv_file TEXT,
                status TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # ✅ Step 1.1: Ensure tracker table has required columns
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'batch_process_tracker_data'
                    AND column_name = 'batch_completion_status'
                ) THEN
                    ALTER TABLE batch_process_tracker_data
                    ADD COLUMN batch_completion_status TEXT DEFAULT 'processing';
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'batch_process_tracker_data'
                    AND column_name = 'tracking_url'
                ) THEN
                    ALTER TABLE batch_process_tracker_data
                    ADD COLUMN tracking_url TEXT;
                END IF;
            END$$;
        """)

        # ✅ Step 2: Fetch unprocessed rows
        cur.execute("""
            SELECT batch_task_id, text_structure_id, batch_custom_id,
                   s2paragraph1, s3paragraph1, s4paragraph1, s5paragraph1,
                   s6paragraph1, s7paragraph1, s8paragraph1, s9paragraph1,
                   author_name, batch_type, batch_created
            FROM template1_text_structure_data
            WHERE batch_created IS NOT TRUE;
        """)
        rows = cur.fetchall()
        columns = ["batch_task_id", "text_structure_id", "batch_custom_id",
                   "s2paragraph1", "s3paragraph1", "s4paragraph1", "s5paragraph1",
                   "s6paragraph1", "s7paragraph1", "s8paragraph1", "s9paragraph1",
                   "author_name", "batch_type", "batch_created"]
        final = pd.DataFrame(rows, columns=columns)

        if final.empty:
            return {"status": "no_data", "message": "No unprocessed quotes found."}

        # ✅ Step 3: Generate JSONL payload
        payloads = []
        for _, row in final.iterrows():
            quotes = [row.get(f"s{i}paragraph1", '') for i in range(2, 10)]
            block = "\n".join(f"- {q}" for q in quotes if q and q != "NA")
            prompt = (
                f"You're given a series of quotes by {row['author_name']}\n"
                f"Use them to generate metadata for a web story.\n"
                f"Quotes:\n{block}\n\n"
                "Please respond ONLY in this exact JSON format:\n"
                "{\n  \"storytitle\": \"...\",\n  \"metadescription\": \"...\",\n  \"metakeywords\": \"...\"\n}"
            )
            payloads.append({
                "custom_id": row["batch_custom_id"],
                "method": "POST",
                "url": "/chat/completions",
                "body": {
                    "model": deployment_model,
                    "messages": [
                        {"role": "system", "content": "You are a creative and SEO-savvy content writer."},
                        {"role": "user", "content": prompt}
                    ]
                }
            })

        jsonl_filename = f"quotefancy_azure_batch_{ts}.jsonl"
        with open(jsonl_filename, "w") as f:
            for record in payloads:
                f.write(json.dumps(record) + '\n')

        # ✅ Step 4: Upload JSONL to Azure REST
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        api_version = "2025-03-01-preview"
        upload_url = f"{endpoint}/openai/files?api-version={api_version}"
        headers = {"api-key": api_key}

        with open(jsonl_filename, "rb") as file:
            files = {
                "purpose": (None, "batch"),
                "file": (jsonl_filename, file, "application/json"),
                "expires_after.seconds": (None, "1209600"),
                "expires_after.anchor": (None, "created_at")
            }
            response = httpx.post(upload_url, headers=headers, files=files)
            if response.status_code not in [200, 201]:
                raise Exception(f"File upload failed: {response.text}")
            file_id = response.json()["id"]

        # ✅ Step 5: Submit Batch Job
        batch_url = f"{endpoint}/openai/batches?api-version={api_version}"
        batch_headers = {
            "api-key": api_key,
            "Content-Type": "application/json"
        }
        batch_payload = {
            "input_file_id": file_id,
            "endpoint": "/chat/completions",
            "completion_window": "24h",
            "output_expires_after": {"seconds": 1209600},
            "anchor": "created_at"
        }

        batch_response = httpx.post(batch_url, headers=batch_headers, json=batch_payload)
        if batch_response.status_code not in [200, 201]:
            raise Exception(f"Batch creation failed: {batch_response.text}")
        batch_data = batch_response.json()
        batch_id = batch_data["id"]

        print(f"✅ Batch submitted: batch_id={batch_id}, file_id={file_id}")

        # ✅ Step 6: Track batch submission in DB
        tracking_url = f"{endpoint}/openai/batches/{batch_id}?api-version={api_version}"

        for task_id in final["batch_task_id"].unique():
            cur.execute("""
                INSERT INTO batch_process_tracker_data (
                    batch_task_id, batch_type, batch_id, file_id,
                    jsonl_file, csv_file, status, batch_completion_status, tracking_url, timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                task_id, "Auto", batch_id, file_id,
                jsonl_filename, f"structured_data_{ts}.csv", "Submitted", "processing", tracking_url, datetime.utcnow()
            ))

            cur.execute("""
                UPDATE template1_text_structure_data
                SET batch_created = TRUE
                WHERE batch_task_id = %s;
            """, (task_id,))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "batch_id": batch_id,
            "file_id": file_id,
            "jsonl_file": jsonl_filename,
            "total_prompts": len(payloads),
            "tracking_url": tracking_url
        }

    except Exception as e:
        print(f"[ERROR] Batch process failed: {e}")
        return {"status": "error", "detail": str(e)}