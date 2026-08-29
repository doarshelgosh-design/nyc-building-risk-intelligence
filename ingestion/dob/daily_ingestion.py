import os
import time
from datetime import datetime, timedelta
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

MINIO_SECURE = (
    os.getenv("MINIO_SECURE", "false").lower() == "true"
)


# --------------------------------------------------
# 2. Validate environment variables
# --------------------------------------------------

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
    raise ValueError(
        "Missing environment variables: "
        + ", ".join(missing_variables)
    )


# --------------------------------------------------
# 3. DOB Safety Violations API
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/855j-jady.json"
)


# --------------------------------------------------
# 4. Daily ingestion configuration
# --------------------------------------------------

# We intentionally reload a rolling window.
# This helps capture records that arrive late.
LOOKBACK_DAYS = 14

PAGE_SIZE = 1000
MAX_RETRIES = 3


# --------------------------------------------------
# 5. Define ingestion time
# --------------------------------------------------

ingestion_time = datetime.now()

run_id = ingestion_time.strftime(
    "%Y%m%d_%H%M%S"
)


# --------------------------------------------------
# 6. Define source data window
# --------------------------------------------------

window_end = (
    ingestion_time.date()
    + timedelta(days=1)
)

window_start = (
    ingestion_time.date()
    - timedelta(days=LOOKBACK_DAYS)
)


START_DATE = (
    window_start.strftime("%Y-%m-%d")
    + "T00:00:00.000"
)

END_DATE = (
    window_end.strftime("%Y-%m-%d")
    + "T00:00:00.000"
)


# --------------------------------------------------
# 7. Connect to MinIO
# --------------------------------------------------

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)


# --------------------------------------------------
# 8. Function: request DOB API with retry
# --------------------------------------------------

def fetch_page(params):

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            response = requests.get(
                API_URL,
                params=params,
                timeout=60
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
                f"Waiting {wait_seconds} seconds "
                "before retry..."
            )

            time.sleep(
                wait_seconds
            )


# --------------------------------------------------
# 9. Build Bronze path for this ingestion run
# --------------------------------------------------

base_path = (
    "bronze/dob_violations/daily/"
    f"ingestion_year={ingestion_time.year}/"
    f"ingestion_month={ingestion_time.month:02d}/"
    f"ingestion_day={ingestion_time.day:02d}/"
    f"run={run_id}/"
)


# --------------------------------------------------
# 10. Start ingestion
# --------------------------------------------------

print()
print("==================================")
print("DOB DAILY INGESTION")
print("==================================")

print(
    f"Source window: "
    f"{START_DATE} -> {END_DATE}"
)

print(
    f"Lookback days: "
    f"{LOOKBACK_DAYS}"
)

print(
    f"Run ID: "
    f"{run_id}"
)

print("----------------------------------")


# --------------------------------------------------
# 11. Pagination
# --------------------------------------------------

offset = 0
page_number = 1
total_records = 0


while True:

    params = {

        "$limit": PAGE_SIZE,

        "$offset": offset,

        "$where": (
            f"violation_issue_date >= '{START_DATE}' "
            f"AND violation_issue_date < '{END_DATE}'"
        ),

        "$order": (
            "violation_issue_date ASC, "
            "violation_number ASC"
        )
    }


    # --------------------------------------------------
    # 12. Request one page
    # --------------------------------------------------

    response = fetch_page(
        params
    )

    data = response.json()


    # --------------------------------------------------
    # 13. Stop when no more records exist
    # --------------------------------------------------

    if not data:
        break


    records_in_page = len(
        data
    )

    print(
        f"Page {page_number}: "
        f"{records_in_page} records"
    )


    # --------------------------------------------------
    # 14. Preserve raw JSON response
    # --------------------------------------------------

    raw_json = response.content

    json_stream = BytesIO(
        raw_json
    )


    # --------------------------------------------------
    # 15. Build MinIO object name
    # --------------------------------------------------

    object_name = (
        base_path
        + f"page_{page_number:05d}.json"
    )


    # --------------------------------------------------
    # 16. Save raw JSON to Bronze
    # --------------------------------------------------

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=json_stream,
        length=len(raw_json),
        content_type="application/json"
    )


    print(
        f"Saved: {object_name}"
    )


    # --------------------------------------------------
    # 17. Update counters
    # --------------------------------------------------

    total_records += (
        records_in_page
    )


    # --------------------------------------------------
    # 18. Check whether this is the last page
    # --------------------------------------------------

    if records_in_page < PAGE_SIZE:
        break


    # --------------------------------------------------
    # 19. Move to next page
    # --------------------------------------------------

    offset += PAGE_SIZE
    page_number += 1


# --------------------------------------------------
# 20. Create run-level _SUCCESS marker
# --------------------------------------------------

success_marker = (
    base_path
    + "_SUCCESS"
)

success_stream = BytesIO(
    b""
)

client.put_object(
    bucket_name=MINIO_BUCKET,
    object_name=success_marker,
    data=success_stream,
    length=0,
    content_type="application/octet-stream"
)


# --------------------------------------------------
# 21. Final summary
# --------------------------------------------------

print()
print("==================================")
print("DOB DAILY INGESTION COMPLETED")
print("----------------------------------")

print(
    f"Records loaded: "
    f"{total_records}"
)

print(
    f"Pages saved: "
    f"{page_number if total_records > 0 else 0}"
)

print(
    f"Bronze path: "
    f"{base_path}"
)

print(
    "SUCCESS marker created."
)

print("==================================")