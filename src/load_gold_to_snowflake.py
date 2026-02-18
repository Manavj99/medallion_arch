import io
import os
from datetime import datetime
from pathlib import Path
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BUCKET = "medallion12"
ENTITY = "posts"
today = datetime.now().strftime("%Y-%m-%d")

s3 = boto3.client(
    "s3",
    aws_access_key_id=(os.getenv("ACCESS_KEY_ID") or "").strip(),
    aws_secret_access_key=(os.getenv("SECRET_ACCESS_KEY") or "").strip(),
    region_name=(os.getenv("AWS_REGION") or "us-east-1").strip(),
)

gold_key = f"gold/fact_posts/load_date={today}/data.parquet"
obj = s3.get_object(Bucket=BUCKET, Key=gold_key)
df = pd.read_parquet(io.BytesIO(obj["Body"].read()))


conn = snowflake.connector.connect(
    account=(os.getenv("SNOWFLAKE_ACCOUNT") or "").strip(),
    user=(os.getenv("SNOWFLAKE_USER") or "").strip(),
    password=(os.getenv("SNOWFLAKE_PASSWORD") or "").strip(),
    warehouse=(os.getenv("SNOWFLAKE_WAREHOUSE") or "").strip(),
    database=(os.getenv("SNOWFLAKE_DATABASE") or "").strip(),
    schema=(os.getenv("SNOWFLAKE_SCHEMA") or "").strip(),
)

db = (os.getenv("SNOWFLAKE_DATABASE") or "").strip()
schema = (os.getenv("SNOWFLAKE_SCHEMA") or "").strip()

conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
conn.cursor().execute(f"CREATE OR REPLACE TABLE {db}.{schema}.fact_posts (post_id INTEGER, user_id INTEGER, title VARCHAR, body VARCHAR, load_date VARCHAR)")
df.columns = [c.upper() for c in df.columns]
write_pandas(conn, df, "FACT_POSTS", schema=schema, database=db, overwrite=True)
conn.close()
