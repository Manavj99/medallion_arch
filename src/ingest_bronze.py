import json
import os
from datetime import datetime
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

API_URL = "https://jsonplaceholder.typicode.com/posts"
BUCKET = "medallion12"
SOURCE = "jsonplaceholder"
ENTITY = "posts"

response = requests.get(API_URL)
data = response.json()

today = datetime.now().strftime("%Y-%m-%d")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
s3_key = f"bronze/{SOURCE}/{ENTITY}/ingest_date={today}/{timestamp}.json"

s3 = boto3.client(
    "s3",
    aws_access_key_id=(os.getenv("ACCESS_KEY_ID") or "").strip(),
    aws_secret_access_key=(os.getenv("SECRET_ACCESS_KEY") or "").strip(),
    region_name=(os.getenv("AWS_REGION") or "us-east-1").strip(),
)
s3.put_object(
    Bucket=BUCKET,
    Key=s3_key,
    Body=json.dumps(data),
    ContentType="application/json",
)
