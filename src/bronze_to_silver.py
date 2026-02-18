import io
import json
import os
from datetime import datetime
from pathlib import Path
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BUCKET = "medallion12"
SOURCE = "jsonplaceholder"
ENTITY = "posts"

s3 = boto3.client(
    "s3",
    aws_access_key_id=(os.getenv("ACCESS_KEY_ID") or "").strip(),
    aws_secret_access_key=(os.getenv("SECRET_ACCESS_KEY") or "").strip(),
    region_name=(os.getenv("AWS_REGION") or "us-east-1").strip(),
)

today = datetime.now().strftime("%Y-%m-%d")
prefix = f"bronze/{SOURCE}/{ENTITY}/ingest_date={today}/"
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]

rows = []
for key in keys:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    data = json.load(obj["Body"])
    rows.extend(data if isinstance(data, list) else [data])

df = pd.DataFrame(rows)
df = df.rename(columns={"userId": "user_id"})
df = df.drop_duplicates(subset=["id"])
df = df.astype({"id": "int64", "user_id": "int64", "title": "string", "body": "string"})

buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
buffer.seek(0)

silver_key = f"silver/{ENTITY}/ingest_date={today}/data.parquet"
s3.put_object(Bucket=BUCKET, Key=silver_key, Body=buffer.getvalue())
