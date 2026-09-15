import os
import time
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

MINIO_SECURE = (
    os.getenv(
        "MINIO_SECURE",
        "false"
    ).lower()
    == "true"
)

LOOKBACK_DAYS = int(
    os.getenv(
        "HPD_LOOKBACK_DAYS",
        "7"
    )
)

PAGE_SIZE = int(
    os.getenv(
        "HPD_PAGE_SIZE",
        "1000"
    )
)

MAX_RETRIES = 3


required_variables = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_BUCKET": MINIO_BUCKET,
}

missing_variables = [
    name
    for name, value in required_variables.items()
    if not value
]

if missing_variables:
    raise RuntimeError(
        "Missing environment variables: "
        + ", ".join(missing_variables)
    )


# ============================================================
# 2. HPD API
# ============================================================

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/wvxf-dwi5.json"
)

NYC_TZ = ZoneInfo(
    "America/New_York"
)

now_nyc = datetime.now(
    NYC_TZ
)

run_timestamp = now_nyc.strftime(
    "%Y%m%d_%H%M%S"
)


# Process 7 calendar days including today
end_date = now_nyc.replace(
    hour=0,
    minute=0,
    second=0,
    microsecond=0,
)

start_date = (
    end_date
    - timedelta(
        days=LOOKBACK_DAYS - 1
    )
)


# ============================================================
# 3. MINIO
# ============================================================

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

if not client.bucket_exists(
    MINIO_BUCKET
):
    raise RuntimeError(
        f"MinIO bucket does not exist: "
        f"{MINIO_BUCKET}"
    )


# ============================================================
# 4. API REQUEST WITH RETRY
# ============================================================

session = requests.Session()


def fetch_page(params):

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            response = session.get(
                API_URL,
                params=params,
                timeout=60,
            )

            response.raise_for_status()

            return response

        except requests.RequestException as error:

            print(
                f"API request failed. "
                f"Attempt {attempt}/{MAX_RETRIES}"
            )

            print(
                f"Error: {error}"
            )

            if attempt == MAX_RETRIES:
                raise

            wait_seconds = (
                attempt * 5
            )

            print(
                f"Waiting {wait_seconds} seconds..."
            )

            time.sleep(
                wait_seconds
            )


# ============================================================
# 5. HEADER
# ============================================================

print()
print(
    "========================================"
)

print(
    "HPD DAILY INGESTION"
)

print(
    "========================================"
)

print(
    f"Lookback days: {LOOKBACK_DAYS}"
)

print(
    f"From date: {start_date.date()}"
)

print(
    f"Through date: {end_date.date()}"
)

print(
    f"Page size: {PAGE_SIZE}"
)

print(
    f"Run: {run_timestamp}"
)


# ============================================================
# 6. PROCESS EACH DAY
# ============================================================

grand_total_records = 0
grand_total_pages = 0
days_processed = 0

current_date = start_date


while current_date <= end_date:

    next_date = (
        current_date
        + timedelta(days=1)
    )

    day_start = current_date.strftime(
        "%Y-%m-%dT00:00:00.000"
    )

    day_end = next_date.strftime(
        "%Y-%m-%dT00:00:00.000"
    )


    base_path = (
        "bronze/hpd_violations/"
        f"year={current_date.year}/"
        f"month={current_date.month:02d}/"
        f"day={current_date.day:02d}/"
        "daily/"
        f"run={run_timestamp}/"
    )


    print()
    print(
        "----------------------------------------"
    )

    print(
        f"Processing date: "
        f"{current_date.date()}"
    )

    print(
        "----------------------------------------"
    )


    offset = 0
    page_number = 1
    daily_total = 0


    # ========================================================
    # 7. PAGINATION
    # ========================================================

    while True:

        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,

            "$where": (
                f"inspectiondate >= '{day_start}' "
                f"AND inspectiondate < '{day_end}'"
            ),

            "$order": (
                "inspectiondate ASC, "
                "violationid ASC"
            ),
        }


        print(
            f"Requesting page {page_number} "
            f"(offset={offset})..."
        )


        response = fetch_page(
            params
        )

        data = response.json()

        records_in_page = len(
            data
        )


        print(
            f"Received "
            f"{records_in_page:,} records."
        )


        if records_in_page == 0:
            break


        raw_json = (
            response.content
        )

        json_stream = BytesIO(
            raw_json
        )


        object_name = (
            f"{base_path}"
            f"page_{page_number:05d}.json"
        )


        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=json_stream,
            length=len(raw_json),
            content_type="application/json",
        )


        print(
            f"Saved: {object_name}"
        )


        daily_total += (
            records_in_page
        )

        grand_total_records += (
            records_in_page
        )

        grand_total_pages += 1


        if records_in_page < PAGE_SIZE:
            break


        offset += PAGE_SIZE
        page_number += 1


    # ========================================================
    # 8. SUCCESS MARKER FOR THIS DAY/RUN
    # ========================================================

    success_marker = (
        f"{base_path}_SUCCESS"
    )


    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=success_marker,
        data=BytesIO(b""),
        length=0,
        content_type="application/octet-stream",
    )


    print(
        f"Date completed: "
        f"{current_date.date()} "
        f"| Records: {daily_total:,}"
    )


    days_processed += 1

    current_date = next_date


# ============================================================
# 9. FINAL RESULT
# ============================================================

print()
print(
    "========================================"
)

print(
    "HPD DAILY INGESTION COMPLETED"
)

print(
    "========================================"
)

print(
    f"Days processed: "
    f"{days_processed}"
)

print(
    f"Total records received: "
    f"{grand_total_records:,}"
)

print(
    f"Objects written: "
    f"{grand_total_pages:,}"
)

print(
    f"Run: "
    f"{run_timestamp}"
)

print(
    "========================================"
)