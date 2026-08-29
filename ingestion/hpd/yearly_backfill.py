import os
import time
from datetime import datetime, timedelta
from io import BytesIO

import requests
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


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
# 3. HPD Violations API configuration
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/wvxf-dwi5.json"
)

# TEST RANGE
#
# Includes:
# 2026-08-18
# 2026-08-19
#
# END_DATE is NOT included.
START_DATE = "2025-08-25"
END_DATE = "2026-08-25"

PAGE_SIZE = 1000

MAX_RETRIES = 3


# --------------------------------------------------
# 4. Connect to MinIO
# --------------------------------------------------

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)


# --------------------------------------------------
# 5. Function: check if object exists
# --------------------------------------------------

def object_exists(
    minio_client,
    bucket_name,
    object_name
):
    try:

        minio_client.stat_object(
            bucket_name,
            object_name
        )

        return True

    except S3Error as error:

        if error.code in (
            "NoSuchKey",
            "NoSuchObject"
        ):
            return False

        raise


# --------------------------------------------------
# 6. Function: request one page from HPD API
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

            wait_seconds = attempt * 5

            print(
                f"Waiting {wait_seconds} seconds "
                "before retry..."
            )

            time.sleep(
                wait_seconds
            )


# --------------------------------------------------
# 7. Convert dates to Python datetime
# --------------------------------------------------

current_date = datetime.strptime(
    START_DATE,
    "%Y-%m-%d"
)

end_date = datetime.strptime(
    END_DATE,
    "%Y-%m-%d"
)


# --------------------------------------------------
# 8. Global counters
# --------------------------------------------------

grand_total_records = 0
grand_total_pages = 0

completed_days = 0
skipped_days = 0


# --------------------------------------------------
# 9. Process one day at a time
# --------------------------------------------------

while current_date < end_date:

    next_date = (
        current_date
        + timedelta(days=1)
    )


    # --------------------------------------------------
    # 10. Build Bronze path
    # --------------------------------------------------

    base_path = (
        "bronze/hpd_violations/"
        f"year={current_date.year}/"
        f"month={current_date.month:02d}/"
        f"day={current_date.day:02d}/"
        "backfill/"
    )

    success_marker = (
        base_path
        + "_SUCCESS"
    )


    # --------------------------------------------------
    # 11. Skip already completed day
    # --------------------------------------------------

    if object_exists(
        client,
        MINIO_BUCKET,
        success_marker
    ):

        print()

        print(
            f"Skipping {current_date.date()} "
            "- already completed."
        )

        skipped_days += 1

        current_date = next_date

        continue


    # --------------------------------------------------
    # 12. Define current day's time range
    # --------------------------------------------------

    day_start = current_date.strftime(
        "%Y-%m-%dT00:00:00.000"
    )

    day_end = next_date.strftime(
        "%Y-%m-%dT00:00:00.000"
    )


    print()
    print("==================================")

    print(
        f"Processing date: "
        f"{current_date.date()}"
    )

    print("==================================")


    # --------------------------------------------------
    # 13. Reset daily pagination
    # --------------------------------------------------

    offset = 0
    page_number = 1
    daily_total = 0


    # --------------------------------------------------
    # 14. Pagination inside current day
    # --------------------------------------------------

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
            )
        }


        # --------------------------------------------------
        # 15. Request page from HPD API
        # --------------------------------------------------

        response = fetch_page(
            params
        )

        data = response.json()


        # --------------------------------------------------
        # 16. No more data
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
        # 17. Keep raw API response
        # --------------------------------------------------

        raw_json = (
            response.content
        )

        json_stream = BytesIO(
            raw_json
        )


        # --------------------------------------------------
        # 18. Build MinIO object name
        # --------------------------------------------------

        object_name = (
            base_path
            + f"page_{page_number:05d}.json"
        )


        # --------------------------------------------------
        # 19. Save raw JSON to Bronze
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
        # 20. Update counters
        # --------------------------------------------------

        daily_total += (
            records_in_page
        )

        grand_total_records += (
            records_in_page
        )

        grand_total_pages += 1


        # --------------------------------------------------
        # 21. Last page?
        # --------------------------------------------------

        if records_in_page < PAGE_SIZE:
            break


        # --------------------------------------------------
        # 22. Move to next page
        # --------------------------------------------------

        offset += PAGE_SIZE

        page_number += 1


    # --------------------------------------------------
    # 23. Create _SUCCESS marker
    # --------------------------------------------------

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


    completed_days += 1


    print()

    print(
        f"SUCCESS marker created for "
        f"{current_date.date()}"
    )

    print(
        f"Date completed: "
        f"{current_date.date()} "
        f"| Records: {daily_total}"
    )


    # --------------------------------------------------
    # 24. Move to next day
    # --------------------------------------------------

    current_date = next_date


# --------------------------------------------------
# 25. Final summary
# --------------------------------------------------

print()
print("==================================")

print(
    "HPD DAILY BACKFILL COMPLETED"
)

print("----------------------------------")

print(
    f"New records loaded: "
    f"{grand_total_records}"
)

print(
    f"New pages saved: "
    f"{grand_total_pages}"
)

print(
    f"Days completed: "
    f"{completed_days}"
)

print(
    f"Days skipped: "
    f"{skipped_days}"
)

print("==================================")