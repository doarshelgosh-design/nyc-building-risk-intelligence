import json
import os
from datetime import datetime
from io import BytesIO

import requests
from dotenv import load_dotenv
from minio import Minio


# --------------------------------------------------
# 1. Load environment variables
# --------------------------------------------------

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


# --------------------------------------------------
# 2. NYC 311 API configuration
# --------------------------------------------------

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

params = {
    "$limit": 100,
    "$where": "agency='HPD'",
    "$order": "created_date DESC"
}


# --------------------------------------------------
# 3. Request data from NYC 311
# --------------------------------------------------

response = requests.get(
    API_URL,
    params=params,
    timeout=60
)

response.raise_for_status()

data = response.json()

print(f"Received {len(data)} records from NYC 311.")

if data:
    print(f"Latest record date: {data[0].get('created_date')}")
    print(f"Agency: {data[0].get('agency')}")
    print(f"Complaint type: {data[0].get('complaint_type')}")

# --------------------------------------------------
# 4. Convert raw response to JSON bytes
# --------------------------------------------------

json_bytes = json.dumps(
    data,
    ensure_ascii=False
).encode("utf-8")

json_stream = BytesIO(json_bytes)


# --------------------------------------------------
# 5. Connect to MinIO
# --------------------------------------------------

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


# --------------------------------------------------
# 6. Build Bronze object path
# --------------------------------------------------

now = datetime.now()

object_name = (
    f"bronze/311/"
    f"year={now.year}/"
    f"month={now.month:02d}/"
    f"day={now.day:02d}/"
    f"311_{now.strftime('%Y%m%d_%H%M%S')}.json"
)


# --------------------------------------------------
# 7. Upload raw JSON to Bronze
# --------------------------------------------------

client.put_object(
    bucket_name=MINIO_BUCKET,
    object_name=object_name,
    data=json_stream,
    length=len(json_bytes),
    content_type="application/json"
)


print("SUCCESS: NYC 311 data saved to Bronze.")
print(f"Object: {object_name}")