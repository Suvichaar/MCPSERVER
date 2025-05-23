# routers/rotate.py

from fastapi import APIRouter
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

def add_circular_navigation_fields(df):
    df["{{prevstorytitle}}"] = df["storytitle"].shift(1)
    df["{{prevstorylink}}"] = df["canurl"].shift(1)
    df.loc[0, "{{prevstorytitle}}"] = df.loc[df.index[-1], "storytitle"]
    df.loc[0, "{{prevstorylink}}"] = df.loc[df.index[-1], "canurl"]

    df["{{nextstorytitle}}"] = df["storytitle"].shift(-1)
    df["{{nextstoryimage}}"] = df["squarecoverurl"].shift(-1)
    df["{{nextstoryimagealt}}"] = df["s1alt1"].shift(-1)
    df["{{s11paragraph1}}"] = df["storytitle"].shift(-1)
    df["{{s11btnlink}}"] = df["canurl"].shift(-1)

    last = df.index[-1]
    df.loc[last, "{{nextstorytitle}}"] = df.loc[0, "storytitle"]
    df.loc[last, "{{nextstoryimage}}"] = df.loc[0, "squarecoverurl"]
    df.loc[last, "{{nextstoryimagealt}}"] = df.loc[0, "s1alt1"]
    df.loc[last, "{{s11paragraph1}}"] = df.loc[0, "storytitle"]
    df.loc[last, "{{s11btnlink}}"] = df.loc[0, "canurl"]

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

        df = pd.read_sql_query("SELECT * FROM meta_data;", conn)

        # Drop original ID column if present to avoid conflict with SERIAL PRIMARY KEY
        if "id" in df.columns:
            df.drop(columns=["id"], inplace=True)

        df = add_circular_navigation_fields(df)

        # Create new table
        create_table_sql = (
            "CREATE TABLE IF NOT EXISTS pre_final_stage_data ("
            + ", ".join([f'"{col}" TEXT' for col in df.columns])
            + ", id SERIAL PRIMARY KEY);"
        )
        cur.execute(create_table_sql)

        # Insert rotated data
        insert_cols = list(df.columns)
        insert_sql = f"""
            INSERT INTO pre_final_stage_data ({', '.join([f'"{col}"' for col in insert_cols])})
            VALUES ({', '.join(['%s'] * len(insert_cols))})
        """
        cur.executemany(insert_sql, df[insert_cols].values.tolist())
        conn.commit()

        cur.close()
        conn.close()

        return {"status": "success", "records_rotated": len(df)}

    except Exception as e:
        return {"status": "error", "message": str(e)}