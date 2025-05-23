import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def merge_textual_data():
    try:
        # ✅ Connect to DB
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # ✅ Fetch structured quote paragraphs
        cur.execute("""
            SELECT batch_custom_id, s2paragraph1, s3paragraph1, s4paragraph1, s5paragraph1,
                   s6paragraph1, s7paragraph1, s8paragraph1, s9paragraph1, author_name
            FROM template1_text_structure_data;
        """)
        structure_df = pd.DataFrame(cur.fetchall(), columns=[
            "batch_custom_id", "s2paragraph1", "s3paragraph1", "s4paragraph1", "s5paragraph1",
            "s6paragraph1", "s7paragraph1", "s8paragraph1", "s9paragraph1", "author_name"
        ])

        # ✅ Fetch metadata responses
        cur.execute("""
            SELECT batch_custom_id, storytitle, metadescription, metakeywords
            FROM template1_text_batch_processed_data;
        """)
        metadata_df = pd.DataFrame(cur.fetchall(), columns=[
            "batch_custom_id", "storytitle", "metadescription", "metakeywords"
        ])

        # ✅ Merge both DataFrames on batch_custom_id
        merged_df = pd.merge(structure_df, metadata_df, on="batch_custom_id", how="inner")

        # ✅ Create new table if it doesn’t exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS textual_structured_data (
                id SERIAL PRIMARY KEY,
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
                storytitle TEXT,
                metadescription TEXT,
                metakeywords TEXT
            );
        """)

        # ✅ Insert merged rows
        for _, row in merged_df.iterrows():
            cur.execute("""
                INSERT INTO textual_structured_data (
                    batch_custom_id, s2paragraph1, s3paragraph1, s4paragraph1, s5paragraph1,
                    s6paragraph1, s7paragraph1, s8paragraph1, s9paragraph1, author_name,
                    storytitle, metadescription, metakeywords
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.batch_custom_id, row.s2paragraph1, row.s3paragraph1, row.s4paragraph1,
                row.s5paragraph1, row.s6paragraph1, row.s7paragraph1, row.s8paragraph1,
                row.s9paragraph1, row.author_name,
                row.storytitle, row.metadescription, row.metakeywords
            ))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "rows_merged": len(merged_df)
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}