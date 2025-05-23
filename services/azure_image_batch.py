import os
import json
import uuid
import pandas as pd
import psycopg2
import httpx
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def generate_and_upload_image_alt_batch():
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

        # ✅ Step 2: Fetch rows from image_fetched_data where batch_created is FALSE
        cur.execute("""
            SELECT author, filename, cdn_url, batch_custom_id, batch_type
            FROM image_fetched_data
            WHERE batch_created IS NOT TRUE;
        """)
        rows = cur.fetchall()
        if not rows:
            return {"status": "no_data", "message": "No unprocessed images found."}

        df = pd.DataFrame(rows, columns=[
            "author", "filename", "cdn_url", "batch_custom_id", "batch_type"
        ])

        # ✅ Step 3: Generate prompts
        df["prompt"] = df["author"].apply(
            lambda author: f"Given the following image URL of a famous personality, generate a short ALT text (max 1–2 sentences) that introduces the {author}, including their name, legacy, or profession in a respectful tone suitable for accessibility or SEO purposes."
        )

        # ✅ Step 4: Assign same batch_task_id for all entries in this batch
        batch_uuid = str(uuid.uuid4())[:8]
        batch_task_id = f"{batch_uuid}_i1"
        df["batch_task_id"] = batch_task_id

        # ✅ Step 5: Create JSONL payload
        payloads = []
        for _, row in df.iterrows():
            payloads.append({
                "custom_id": os.path.splitext(row["filename"])[0],  # Custom ID per image
                "method": "POST",
                "url": "/chat/completions",
                "body": {
                    "model": deployment_model,
                    "messages": [
                        {"role": "system", "content": "You are a helpful and professional assistant with expertise in creating descriptive ALT texts that are accessible, informative, and optimized for SEO. Respond with clarity and respect."},
                        {"role": "user", "content": [
                            {"type": "text", "text": row["prompt"]},
                            {"type": "image_url", "image_url": {"url": row["cdn_url"], "detail": "high"}}
                        ]}
                    ],
                    "max_tokens": 1000
                }
            })

        jsonl_filename = f"image_alt_batch_{ts}.jsonl"
        with open(jsonl_filename, "w") as f:
            for record in payloads:
                f.write(json.dumps(record) + '\n')

        # ✅ Step 6: Upload JSONL to Azure
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

        # ✅ Step 7: Submit Azure Batch
        batch_url = f"{endpoint}/openai/batches?api-version={api_version}"
        batch_payload = {
            "input_file_id": file_id,
            "endpoint": "/chat/completions",
            "completion_window": "24h",
            "output_expires_after": {"seconds": 1209600},
            "anchor": "created_at"
        }
        batch_headers = {"api-key": api_key, "Content-Type": "application/json"}
        batch_response = httpx.post(batch_url, headers=batch_headers, json=batch_payload)
        if batch_response.status_code not in [200, 201]:
            raise Exception(f"Batch creation failed: {batch_response.text}")

        batch_id = batch_response.json()["id"]
        tracking_url = f"{endpoint}/openai/batches/{batch_id}?api-version={api_version}"

        # ✅ Step 8: Insert into batch_process_tracker_data only once for the whole batch
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
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                batch_completion_status TEXT DEFAULT 'processing',
                tracking_url TEXT
            );
        """)

        cur.execute("""
            INSERT INTO batch_process_tracker_data (
                batch_task_id, batch_type, batch_id, file_id,
                jsonl_file, csv_file, status, batch_completion_status,
                tracking_url, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            batch_task_id,
            df["batch_type"].iloc[0],
            batch_id,
            file_id,
            jsonl_filename,
            '',
            'Submitted',
            'processing',
            tracking_url,
            datetime.utcnow()
        ))

        # ✅ Step 9: Update processed status
        cur.execute("""
            UPDATE image_fetched_data
            SET batch_created = TRUE
            WHERE batch_created IS NOT TRUE;
        """)

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "batch_id": batch_id,
            "file_id": file_id,
            "jsonl_file": jsonl_filename,
            "total_images": len(df),
            "tracking_url": tracking_url
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}
