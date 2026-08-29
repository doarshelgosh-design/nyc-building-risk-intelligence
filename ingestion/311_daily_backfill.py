import os
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
# 3. NYC 311 API configuration
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/erm2-nwe9.json"
)

# Test range:
# Includes 2026-08-18 and 2026-08-19
# END_DATE is NOT included
START_DATE = "2025-08-25"
END_DATE = "2026-08-25"

PAGE_SIZE = 1000


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
# 5. Function: check if object exists in MinIO
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
# 6. Convert dates to Python datetime objects
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
# 7. Global counters
# --------------------------------------------------

grand_total_records = 0
grand_total_pages = 0
completed_days = 0
skipped_days = 0


# --------------------------------------------------
# 8. Process one day at a time
# --------------------------------------------------

while current_date < end_date:

    next_date = current_date + timedelta(days=1)


    # --------------------------------------------------
    # 9. Build Bronze path for current day
    # --------------------------------------------------

    base_path = (
        "bronze/311/"
        f"year={current_date.year}/"
        f"month={current_date.month:02d}/"
        f"day={current_date.day:02d}/"
        "backfill/"
    )

    success_marker = (
        base_path + "_SUCCESS"
    )


    # --------------------------------------------------
    # 10. Skip day if it was already completed
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
    # 11. Define current day's time window
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
    # 12. Reset daily pagination counters
    # --------------------------------------------------

    offset = 0
    page_number = 1
    daily_total = 0


    # --------------------------------------------------
    # 13. Pagination inside current day
    # --------------------------------------------------

    while True:

        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,

            "$where": (
                "agency='HPD' "
                f"AND created_date >= '{day_start}' "
                f"AND created_date < '{day_end}'"
            ),

            "$order": (
                "created_date ASC, "
                "unique_key ASC"
            )
        }


        # --------------------------------------------------
        # 14. Request data from NYC 311
        # --------------------------------------------------

        response = requests.get(
            API_URL,
            params=params,
            timeout=60
        )

        response.raise_for_status()

        data = response.json()


        # --------------------------------------------------
        # 15. No more records for current day
        # --------------------------------------------------

        if not data:
            break


        print(
            f"Page {page_number}: "
            f"{len(data)} records"
        )


        # --------------------------------------------------
        # 16. Preserve raw API JSON response
        # --------------------------------------------------

        raw_json = response.content

        json_stream = BytesIO(
            raw_json
        )


        # --------------------------------------------------
        # 17. Build object name
        # --------------------------------------------------

        object_name = (
            base_path
            + f"page_{page_number:05d}.json"
        )


        # --------------------------------------------------
        # 18. Save raw page to MinIO Bronze
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
        # 19. Update counters
        # --------------------------------------------------

        records_in_page = len(data)

        daily_total += records_in_page
        grand_total_records += records_in_page
        grand_total_pages += 1


        # --------------------------------------------------
        # 20. Stop if this is the last page
        # --------------------------------------------------

        if records_in_page < PAGE_SIZE:
            break


        # --------------------------------------------------
        # 21. Move to next API page
        # --------------------------------------------------

        offset += PAGE_SIZE
        page_number += 1


    # --------------------------------------------------
    # 22. Create _SUCCESS marker
    # --------------------------------------------------

    success_stream = BytesIO(b"")

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
    # 23. Move to next day
    # --------------------------------------------------

    current_date = next_date


# --------------------------------------------------
# 24. Final summary
# --------------------------------------------------

print()
print("==================================")
print("DAILY BACKFILL COMPLETED")
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