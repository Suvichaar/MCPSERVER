import os
import psycopg2
import pandas as pd
import random
from dotenv import load_dotenv

load_dotenv()

def assign_video_metadata():
    try:
        # ✅ Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # ✅ Load data from distribution_data
        dist_df = pd.read_sql_query("SELECT * FROM distribution_data;", conn)

        # ✅ Load video metadata (excluding id/inserted_at)
        video_df = pd.read_sql_query("""
            SELECT s10video1, hookline, s10alt1, videoscreenshot, s10caption1
            FROM video_metadata;
        """, conn)

        # ✅ Randomly assign one full row of video metadata to each distribution row
        enriched_rows = []
        for _, row in dist_df.iterrows():
            video_row = video_df.sample(1).iloc[0]
            full_row = row.to_dict()
            for col in video_df.columns:
                full_row[col] = video_row[col]
            enriched_rows.append(full_row)

        final_df = pd.DataFrame(enriched_rows)

        # ✅ Create the final_distribution_video table if not exists
        col_defs = ",\n".join([f"{col} TEXT" for col in final_df.columns if col != "id"])
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS video_meta_added_table (
                id SERIAL PRIMARY KEY,
                {col_defs}
            );
        """)

        # ✅ Insert data into final table
        insert_cols = [col for col in final_df.columns if col != "id"]
        insert_query = f"""
            INSERT INTO video_meta_added_table ({', '.join(insert_cols)})
            VALUES ({', '.join(['%s'] * len(insert_cols))});
        """
        cur.executemany(insert_query, final_df[insert_cols].values.tolist())

        # ✅ Finalize
        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "rows_inserted": len(final_df)
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}