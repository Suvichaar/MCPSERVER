import os
import shutil
import boto3
import base64
import json
import uuid
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from simple_image_download import simple_image_download as simp

load_dotenv()

def download_and_upload_author_images():
    # AWS + S3 setup
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    region_name = "ap-south-1"
    bucket_name = "suvichaarapp"
    s3_prefix = "media/"
    cdn_base_url = "https://cdn.suvichaar.org/"

    # DB connection
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    # ✅ Fetch next scrape_id group with unchecked authors
    cur.execute("""
        SELECT scrape_id FROM quote_scraped_data
        WHERE author_image_check IS DISTINCT FROM 'checked'
        GROUP BY scrape_id
        ORDER BY MIN(timestamp)
        LIMIT 1;
    """)
    result = cur.fetchone()
    if not result:
        return {"status": "no_pending_scrape_id"}

    selected_scrape_id = result[0]

    # ✅ Fetch all distinct authors for that scrape_id
    cur.execute("""
        SELECT DISTINCT author_name FROM quote_scraped_data
        WHERE scrape_id = %s AND author_image_check IS DISTINCT FROM 'checked';
    """, (selected_scrape_id,))
    authors = [r[0].strip() for r in cur.fetchall() if r[0]]

    if not authors:
        return {"status": "no_authors"}

    # ✅ Download images
    downloader = simp.simple_image_download()
    for author in authors:
        downloader.download(author, 15)

    # ✅ Upload to S3 and record metadata
    s3 = boto3.client("s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name
    )

    results = []
    batch_uuid = str(uuid.uuid4())[:8]
    batch_task_id = f"{batch_uuid}_i1"  # Single task ID for this batch

    for folder, _, files in os.walk("simple_images"):
        for file in files:
            if not file.lower().endswith((".jpg", ".jpeg", ".png")):
                continue

            path = os.path.join(folder, file)
            author_name = os.path.basename(folder).replace(" ", "_")
            filename = file.replace(" ", "_")
            s3_key = f"{s3_prefix}{author_name}/{filename}"

            try:
                s3.upload_file(path, bucket_name, s3_key)
                cdn_url = f"{cdn_base_url}{s3_key}"
                batch_custom_id = f"{batch_task_id}_{author_name}"

                results.append((
                    author_name,
                    filename,
                    cdn_url,
                    batch_task_id,
                    batch_custom_id,
                    "Auto",
                    False,
                    datetime.utcnow()
                ))

            except Exception as e:
                continue

    # ✅ Ensure image_fetched_data table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS image_fetched_data (
            id SERIAL PRIMARY KEY,
            author TEXT,
            filename TEXT,
            cdn_url TEXT,
            batch_task_id TEXT,
            batch_custom_id TEXT,
            batch_type TEXT,
            batch_created BOOLEAN DEFAULT FALSE,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    # ✅ Insert rows
    cur.executemany("""
        INSERT INTO image_fetched_data (
            author, filename, cdn_url, batch_task_id,
            batch_custom_id, batch_type, batch_created, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, results)

    # ✅ Update quote_scraped_data to mark authors as checked
    for author in authors:
        cur.execute("""
            UPDATE quote_scraped_data
            SET author_image_check = 'checked'
            WHERE author_name = %s AND scrape_id = %s;
        """, (author, selected_scrape_id))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "success",
        "scrape_id": selected_scrape_id,
        "authors_processed": authors,
        "image_count": len(results),
        "db_table": "image_fetched_data"
    }
