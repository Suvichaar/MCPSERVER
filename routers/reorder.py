from fastapi import APIRouter
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

# Final required order with curly braces
final_column_order = [
    "{{storytitle}}", "{{pagetitle}}", "{{uuid}}", "{{urlslug}}", "{{canurl}}", "{{canurl 1}}", "{{publishedtime}}", "{{modifiedtime}}",
    "{{metakeywords}}", "{{metadescription}}", "{{s2paragraph1}}", "{{s3paragraph1}}", "{{s4paragraph1}}", "{{s5paragraph1}}",
    "{{s6paragraph1}}", "{{s7paragraph1}}", "{{s8paragraph1}}", "{{s9paragraph1}}", "{{s1alt1}}", "{{s2alt1}}", "{{s3alt1}}",
    "{{s4alt1}}", "{{s5alt1}}", "{{s6alt1}}", "{{s7alt1}}", "{{s8alt1}}", "{{s9alt1}}", "{{hookline}}", "{{potraightcoverurl}}",
    "{{landscapecoverurl}}", "{{squarecoverurl}}", "{{socialthumbnailcoverurl}}", "{{s1image1}}", "{{s2image1}}", "{{s3image1}}",
    "{{s4image1}}", "{{s5image1}}", "{{s6image1}}", "{{s7image1}}", "{{s8image1}}", "{{s9image1}}", "{{s11btntext}}",
    "{{s11btnlink}}", "{{lang}}", "{{prevstorytitle}}", "{{prevstorylink}}", "{{nextstorytitle}}", "{{nextstorylink}}",
    "{{user}}", "{{userprofileurl}}", "{{storygeneratorname}}", "{{contenttype}}", "{{storygeneratorversion}}", "{{sitename}}",
    "{{generatorplatform}}", "{{sitelogo96x96}}", "{{person}}", "{{sitelogo32x32}}", "{{sitelogo192x192}}", "{{sitelogo144x144}}",
    "{{sitelogo92x92}}", "{{sitelogo180x180}}", "{{publisher}}", "{{publisherlogosrc}}", "{{gtagid}}", "{{organization}}",
    "{{publisherlogoalt}}", "{{s10video1}}", "{{s10alt1}}", "{{videoscreenshot}}", "{{writername}}", "{{s10caption1}}",
    "{{s11paragraph1}}", "{{nextstoryimage}}", "{{nextstoryimagealt}}"
]

@router.post("/reorder")
def reorder_and_clean_data():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # Read original data
        df = pd.read_sql_query("SELECT * FROM pre_final_stage_data;", conn)

        # Drop the `id` column if it exists
        if "id" in df.columns:
            df.drop(columns=["id"], inplace=True)

        # Rename columns to curly-brace format if they match stripped versions in final_column_order
        brace_map = {
            col: f"{{{{{col}}}}}" for col in df.columns if f"{{{{{col}}}}}" in final_column_order
        }
        df.rename(columns=brace_map, inplace=True)

        # Reorder columns according to final_column_order
        existing_columns = [col for col in final_column_order if col in df.columns]
        df = df[existing_columns]

        # Recreate final table
        cur.execute("DROP TABLE IF EXISTS final_quote_fancy_data;")
        create_sql = (
            "CREATE TABLE final_quote_fancy_data (\n" +
            ",\n".join([f'"{col}" TEXT' for col in df.columns]) +
            "\n);"
        )
        cur.execute(create_sql)

        # Insert reordered data
        insert_sql = f"""
            INSERT INTO final_quote_fancy_data ({', '.join([f'"{col}"' for col in df.columns])})
            VALUES ({', '.join(['%s'] * len(df.columns))})
        """
        cur.executemany(insert_sql, df.values.tolist())
        conn.commit()

        cur.close()
        conn.close()

        return {
            "status": "success",
            "message": "Data saved to final_quote_fancy_data",
            "columns_saved": df.columns.tolist(),
            "records": len(df)
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}