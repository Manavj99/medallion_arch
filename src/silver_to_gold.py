import io
import os
from datetime import datetime
from pathlib import Path
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BUCKET = "medallion12"
ENTITY = "posts"

s3 = boto3.client(
    "s3",
    aws_access_key_id=(os.getenv("ACCESS_KEY_ID") or "").strip(),
    aws_secret_access_key=(os.getenv("SECRET_ACCESS_KEY") or "").strip(),
    region_name=(os.getenv("AWS_REGION") or "us-east-1").strip(),
)

today = datetime.now().strftime("%Y-%m-%d")
silver_key = f"silver/{ENTITY}/ingest_date={today}/data.parquet"
obj = s3.get_object(Bucket=BUCKET, Key=silver_key)
df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

df = df.rename(columns={"id": "post_id"})
df["load_date"] = today

buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
buffer.seek(0)

gold_key = f"gold/fact_posts/load_date={today}/data.parquet"
s3.put_object(Bucket=BUCKET, Key=gold_key, Body=buffer.getvalue())
