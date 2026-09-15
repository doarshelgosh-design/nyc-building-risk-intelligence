import json
import os
from datetime import datetime, timedelta
from io import BytesIO
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from minio import Minio


# ============================================================
# 1. ENVIRONMENT
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

LOOKBACK_DAYS = int(
    os.getenv(
        "NYC_311_LOOKBACK_DAYS",
        "7"
    )
)

PAGE_SIZE = int(
    os.getenv(
        "NYC_311_PAGE_SIZE",
        "1000"
    )
)


required_env = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_BUCKET": MINIO_BUCKET,
}


missing_env = [
    name
    for name, value in required_env.items()
    if not value
]


if missing_env:
    raise RuntimeError(
        "Missing environment variables: "
        + ", ".join(missing_env)
    )


# ============================================================
# 2. NYC 311 API
# ============================================================

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/erm2-nwe9.json"
)

NYC_TZ = ZoneInfo(
    "America/New_York"
)

now_nyc = datetime.now(
    NYC_TZ
)

start_date = (
    now_nyc
    - timedelta(
        days=LOOKBACK_DAYS
    )
).replace(
    hour=0,
    minute=0,
    second=0,
    microsecond=0,
)

start_date_string = (
    start_date.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
)


print()
print(
    "========================================"
)

print(
    "NYC 311 DAILY INGESTION"
)

print(
    "========================================"
)

print(
    f"Lookback days: {LOOKBACK_DAYS}"
)

print(
    f"From created_date: {start_date_string}"
)

print(
    f"Page size: {PAGE_SIZE}"
)


# ============================================================
# 3. MINIO CONNECTION
# ============================================================

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


if not client.bucket_exists(
    MINIO_BUCKET
):
    raise RuntimeError(
        f"MinIO bucket does not exist: "
        f"{MINIO_BUCKET}"
    )


# ============================================================
# 4. RUN INFORMATION
# ============================================================

run_timestamp = (
    now_nyc.strftime(
        "%Y%m%d_%H%M%S"
    )
)

base_object_path = (
    "bronze/311/"
    f"year={now_nyc.year}/"
    f"month={now_nyc.month:02d}/"
    f"day={now_nyc.day:02d}/"
    "daily/"
    f"run={run_timestamp}"
)


# ============================================================
# 5. PAGINATED API INGESTION
# ============================================================

session = requests.Session()

offset = 0
page_number = 1
total_records = 0
objects_written = []


while True:

    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,

        "$where": (
            "agency='HPD' "
            f"AND created_date >= "
            f"'{start_date_string}'"
        ),

        "$order": (
            "created_date ASC, "
            "unique_key ASC"
        ),
    }


    print()
    print(
        f"Requesting page {page_number} "
        f"(offset={offset})..."
    )


    response = session.get(
        API_URL,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()


    record_count = len(
        data
    )


    print(
        f"Received {record_count:,} records."
    )


    # --------------------------------------------------------
    # No more records
    # --------------------------------------------------------

    if record_count == 0:
        break


    # --------------------------------------------------------
    # Convert raw page to JSON
    # --------------------------------------------------------

    json_bytes = json.dumps(
        data,
        ensure_ascii=False,
    ).encode(
        "utf-8"
    )

    json_stream = BytesIO(
        json_bytes
    )


    # --------------------------------------------------------
    # Bronze object name
    # --------------------------------------------------------

    object_name = (
        f"{base_object_path}/"
        f"page_{page_number:05d}.json"
    )


    # --------------------------------------------------------
    # Upload raw JSON page
    # --------------------------------------------------------

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=json_stream,
        length=len(json_bytes),
        content_type="application/json",
    )


    print(
        f"Saved: {object_name}"
    )


    objects_written.append(
        object_name
    )

    total_records += (
        record_count
    )


    # --------------------------------------------------------
    # Last page
    # --------------------------------------------------------

    if record_count < PAGE_SIZE:
        break


    offset += PAGE_SIZE
    page_number += 1


# ============================================================
# 6. FINAL RESULT
# ============================================================

print()
print(
    "========================================"
)

print(
    "NYC 311 DAILY INGESTION COMPLETED"
)

print(
    "========================================"
)

print(
    f"Total records received: "
    f"{total_records:,}"
)

print(
    f"Objects written: "
    f"{len(objects_written):,}"
)

print(
    f"Bronze path: "
    f"{base_object_path}/"
)

print(
    "========================================"
)