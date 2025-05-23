import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def clean_video_metadata_table():
    try:
        # 🔐 Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # 📦 Read original data
        df = pd.read_sql_query("SELECT * FROM video_meta_added_table;", conn)

        # 🧹 Columns to remove (image variants and video status)
        cols_to_remove = [
            f"{prefix}{i}"
            for i in range(2, 10)
            for prefix in [
                "potraightcoverurl", "landscapecoverurl", "squarecoverurl",
                "socialthumbnailcoverurl", "nextstoryimageurl"
            ]
        ]
        cols_to_remove.append("video_data_status")
        df.drop(columns=cols_to_remove, inplace=True, errors="ignore")

        # 🔁 Rename columns
        rename_map = {f"standardurl{i}": f"s{i}imageurl1" for i in range(2, 11)}   
        rename_map["author_name"] = "writername"
         
        df.rename(columns=rename_map, inplace=True)

        # ➕ Add default column
        df["meta_data_added"] = False

        # 🛠️ Recreate target table
        insert_cols = [col for col in df.columns if col != "id"]
        col_defs = ",\n".join([
            f"{col} TEXT" if col != "meta_data_added" else f"{col} BOOLEAN DEFAULT FALSE"
            for col in insert_cols
        ])

        cur.execute("DROP TABLE IF EXISTS cleaned_video_meta;")
        cur.execute(sql.SQL("""
            CREATE TABLE cleaned_video_meta (
                id SERIAL PRIMARY KEY,
                {}
            );
        """).format(sql.SQL(col_defs)))

        # 🚀 Insert rows
        insert_query = sql.SQL("""
            INSERT INTO cleaned_video_meta ({})
            VALUES ({});
        """).format(
            sql.SQL(', ').join(map(sql.Identifier, insert_cols)),
            sql.SQL(', ').join(sql.Placeholder() * len(insert_cols))
        )

        cur.executemany(insert_query.as_string(conn), df[insert_cols].values.tolist())

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "cleaned_records": len(df)}

    except Exception as e:
        return {"status": "error", "detail": str(e)}