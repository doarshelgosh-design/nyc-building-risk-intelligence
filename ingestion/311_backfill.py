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

START_DATE = "2026-08-18T00:00:00.000"
END_DATE = "2026-08-25T00:00:00.000"

PAGE_SIZE = 1000


# --------------------------------------------------
# 3. Connect to MinIO
# --------------------------------------------------

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


# --------------------------------------------------
# 4. Download NYC 311 data page by page
# --------------------------------------------------

offset = 0
page_number = 1
total_records = 0

while True:

    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$where": (
            "agency='HPD' "
            f"AND created_date >= '{START_DATE}' "
            f"AND created_date < '{END_DATE}'"
        ),
        "$order": "created_date ASC, unique_key ASC"
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=60
    )

    response.raise_for_status()

    data = response.json()

    if not data:
        break

    print(
        f"Page {page_number}: "
        f"received {len(data)} records."
    )


    # --------------------------------------------------
    # 5. Keep the page as raw JSON
    # --------------------------------------------------

    json_bytes = json.dumps(
        data,
        ensure_ascii=False
    ).encode("utf-8")

    json_stream = BytesIO(json_bytes)


    # --------------------------------------------------
    # 6. Build Bronze object path
    # --------------------------------------------------

    ingestion_time = datetime.now()

    object_name = (
        "bronze/311/backfill/"
        "start=2026-08-18/"
        "end=2026-08-25/"
        f"page_{page_number:05d}_"
        f"{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json"
    )


    # --------------------------------------------------
    # 7. Upload page to MinIO
    # --------------------------------------------------

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=json_stream,
        length=len(json_bytes),
        content_type="application/json"
    )

    print(
        f"Saved page {page_number} to Bronze."
    )

    total_records += len(data)


    # --------------------------------------------------
    # 8. Stop when the last page is smaller
    # --------------------------------------------------

    if len(data) < PAGE_SIZE:
        break

    offset += PAGE_SIZE
    page_number += 1


# --------------------------------------------------
# 9. Final summary
# --------------------------------------------------

print("----------------------------------")
print("BACKFILL COMPLETED")
print(f"Total records: {total_records}")
print(f"Total pages: {page_number}")
print("----------------------------------")