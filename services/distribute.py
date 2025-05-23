import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def distribute_urls():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    # Load paragraph data
    paragraph_query = """
        SELECT batch_custom_id, s2paragraph1, s3paragraph1, s4paragraph1, s5paragraph1, s6paragraph1,
               s7paragraph1, s8paragraph1, s9paragraph1, author_name, storytitle, metadescription, metakeywords
        FROM textual_structured_data;
    """
    paragraph_df = pd.read_sql_query(paragraph_query, conn)

    # Load image resize URLs including alttxt
    resize_query = """
        SELECT author, alttxt, potraightcoverurl, landscapecoverurl, squarecoverurl,
               socialthumbnailcoverurl, nextstoryimageurl, standardurl
        FROM resized_url_data;
    """
    resize_df = pd.read_sql_query(resize_query, conn)

    # Normalize author names
    paragraph_df["author_key"] = paragraph_df["author_name"].str.replace(" ", "_").str.strip()
    resize_df["author"] = resize_df["author"].str.strip()

    output_rows = []

    for _, row in paragraph_df.iterrows():
        author_key = row["author_key"]
        author_images = resize_df[resize_df["author"] == author_key].reset_index(drop=True)
        total_imgs = len(author_images)

        if total_imgs == 0:
            continue

        combined = row.drop("author_key").to_dict()

        for i in range(2, 10):  # s2 to s9
            img_idx = (i - 2) % total_imgs
            suffix = str(i)

            combined[f"potraightcoverurl{suffix}"] = author_images.at[img_idx, 'potraightcoverurl']
            combined[f"landscapecoverurl{suffix}"] = author_images.at[img_idx, 'landscapecoverurl']
            combined[f"squarecoverurl{suffix}"] = author_images.at[img_idx, 'squarecoverurl']
            combined[f"socialthumbnailcoverurl{suffix}"] = author_images.at[img_idx, 'socialthumbnailcoverurl']
            combined[f"nextstoryimageurl{suffix}"] = author_images.at[img_idx, 'nextstoryimageurl']
            combined[f"standardurl{suffix}"] = author_images.at[img_idx, 'standardurl']
            combined[f"s{suffix}alt1"] = author_images.at[img_idx, 'alttxt']  # NEW LINE

        # Add representative image + alt text (first image)
        combined["potraightcoverurl"] = author_images.at[0, 'potraightcoverurl']
        combined["landscapecoverurl"] = author_images.at[0, 'landscapecoverurl']
        combined["squarecoverurl"] = author_images.at[0, 'squarecoverurl']
        combined["socialthumbnailcoverurl"] = author_images.at[0, 'socialthumbnailcoverurl']
        combined["nextstoryimageurl"] = author_images.at[0, 'nextstoryimageurl']
        combined["s1image1"] = author_images.at[0, 'standardurl']
        combined["s1alt1"] = author_images.at[0, 'alttxt']

        combined["video_data_status"] = False
        output_rows.append(combined)

    final_df = pd.DataFrame(output_rows)

    # Create schema
    columns_to_create = """
        batch_custom_id TEXT,
        s2paragraph1 TEXT, s3paragraph1 TEXT, s4paragraph1 TEXT, s5paragraph1 TEXT,
        s6paragraph1 TEXT, s7paragraph1 TEXT, s8paragraph1 TEXT, s9paragraph1 TEXT,
        author_name TEXT, storytitle TEXT, metadescription TEXT, metakeywords TEXT
    """
    for i in range(2, 10):
        columns_to_create += f", s{i}alt1 TEXT"
        for col in ["potraightcoverurl", "landscapecoverurl", "squarecoverurl",
                    "socialthumbnailcoverurl", "nextstoryimageurl", "standardurl"]:
            columns_to_create += f", {col}{i} TEXT"

    columns_to_create += """
        , potraightcoverurl TEXT
        , landscapecoverurl TEXT
        , squarecoverurl TEXT
        , socialthumbnailcoverurl TEXT
        , nextstoryimageurl TEXT
        , s1image1 TEXT
        , s1alt1 TEXT
        , video_data_status BOOLEAN DEFAULT FALSE
    """

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS distribution_data (
            id SERIAL PRIMARY KEY,
            {columns_to_create}
        );
    """)

    if not final_df.empty:
        insert_cols = list(final_df.columns)
        insert_sql = f"""
            INSERT INTO distribution_data ({', '.join(insert_cols)})
            VALUES ({', '.join(['%s'] * len(insert_cols))});
        """
        cur.executemany(insert_sql, final_df[insert_cols].values.tolist())
        conn.commit()

    cur.close()
    conn.close()

    print(f"✅ Distribution completed for {len(final_df)} records")

    return {
        "status": "success",
        "records_distributed": len(final_df)
    }