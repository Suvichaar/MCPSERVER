# routers/rotate.py

from fastapi import APIRouter
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

def add_circular_navigation_fields(df):
    # Previous story: shift down by 1 row; wrap last → first
    df["prevstorytitle"] = df["storytitle"].shift(1)
    df["prevstorylink"] = df["canurl"].shift(1)
    df.loc[0, "prevstorytitle"] = df.loc[df.index[-1], "storytitle"]
    df.loc[0, "prevstorylink"] = df.loc[df.index[-1], "canurl"]

    # Next story: shift up by 1 row; wrap first → last
    df["nextstorytitle"] = df["storytitle"].shift(-1)
    df["nextstoryimage"] = df["nextstoryimageurl"].shift(-1)
    df["nextstoryimagealt"] = df["s1alt1"].shift(-1)
    df["s11paragraph1"] = df["storytitle"].shift(-1)
    df["s11btnlink"] = df["canurl"].shift(-1)
    df["nextstorylink"] = df["canurl"].shift(-1)

    last = df.index[-1]
    df.loc[last, "nextstorytitle"]     = df.loc[0, "storytitle"]
    df.loc[last, "nextstoryimage"]     = df.loc[0, "nextstoryimageurl"]
    df.loc[last, "nextstoryimagealt"]  = df.loc[0, "s1alt1"]
    df.loc[last, "s11paragraph1"]      = df.loc[0, "storytitle"]
    df.loc[last, "s11btnlink"]         = df.loc[0, "canurl"]
    df.loc[last, "nextstorylink"]      = df.loc[0, "canurl"]

    return df

@router.post("/")
def rotate_meta_data():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # 1) Read meta_data into DataFrame
        df = pd.read_sql_query("SELECT * FROM meta_data;", conn)

        # 2) Drop original ID column if present
        if "id" in df.columns:
            df.drop(columns=["id"], inplace=True)

        # 3) Clean ALT text columns by removing leading "ALT text:" prefix and any surrounding quotes
        alt_cols = ["s1alt1", "s2alt1", "s3alt1", "s4alt1", "s5alt1", "s6alt1", "s7alt1", "s8alt1", "s9alt1"]
        for col in alt_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'^ALT text:\s*', "", regex=True).str.strip('"')

        # 4) Add circular navigation fields
        df = add_circular_navigation_fields(df)

        # 5) Drop pre_existing table if exists, then create pre_final_stage_data
        cur.execute("DROP TABLE IF EXISTS pre_final_stage_data;")
        create_table_sql = (
            "CREATE TABLE pre_final_stage_data ("
            + ", ".join([f'"{col}" TEXT' for col in df.columns])
            + ", id SERIAL PRIMARY KEY"
            + ");"
        )
        cur.execute(create_table_sql)

        # 6) Bulk‐insert rotated (and cleaned) data into the newly‐created table
        insert_cols = list(df.columns)
        insert_sql = f"""
            INSERT INTO pre_final_stage_data (
                {', '.join([f'"{col}"' for col in insert_cols])}
            ) VALUES (
                {', '.join(['%s'] * len(insert_cols))}
            );
        """
        cur.executemany(insert_sql, df[insert_cols].values.tolist())
        conn.commit()

        cur.close()
        conn.close()

        return {"status": "success", "records_rotated": len(df)}

    except Exception as e:
        return {"status": "error", "message": str(e)}
