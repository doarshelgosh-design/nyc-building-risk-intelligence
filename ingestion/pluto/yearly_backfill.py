import os
import time
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
# 3. PLUTO API configuration
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/64uk-42ks.json"
)

PAGE_SIZE = 10000
MAX_RETRIES = 3


# --------------------------------------------------
# 4. Project snapshot configuration
# --------------------------------------------------

PROJECT_YEAR = 2026

# The PLUTO version selected as the historical
# baseline for the project.
TARGET_VERSION = "26v2"


# --------------------------------------------------
# 5. Connect to MinIO
# --------------------------------------------------

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)


# --------------------------------------------------
# 6. Check if object exists in MinIO
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
# 7. Request API with retry
# --------------------------------------------------

def fetch_data(params):

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            response = requests.get(
                API_URL,
                params=params,
                timeout=120
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
# 8. Get version currently exposed by PLUTO API
# --------------------------------------------------

version_response = fetch_data(
    {
        "$select": "version",
        "$limit": 1
    }
)

version_data = version_response.json()

if not version_data:
    raise RuntimeError(
        "PLUTO API returned no records."
    )

current_version = version_data[0].get(
    "version"
)

if not current_version:
    raise RuntimeError(
        "PLUTO version field was not found."
    )


print()
print("==================================")
print("PLUTO YEARLY BACKFILL")
print("==================================")

print(
    f"Project year: "
    f"{PROJECT_YEAR}"
)

print(
    f"Target PLUTO version: "
    f"{TARGET_VERSION}"
)

print(
    f"Current API version: "
    f"{current_version}"
)


# --------------------------------------------------
# 9. Validate requested version
# --------------------------------------------------

if current_version != TARGET_VERSION:

    raise RuntimeError(
        "The PLUTO API currently exposes version "
        f"'{current_version}', but this backfill "
        f"is configured for '{TARGET_VERSION}'. "
        "Do not silently load a different version."
    )


# --------------------------------------------------
# 10. Build shared Bronze snapshot path
# --------------------------------------------------

safe_version = (
    TARGET_VERSION
    .replace("/", "_")
    .replace(" ", "_")
)

base_path = (
    "bronze/pluto/"
    "snapshots/"
    f"version={safe_version}/"
)

success_marker = (
    base_path
    + "_SUCCESS"
)


# --------------------------------------------------
# 11. Check whether snapshot already exists
# --------------------------------------------------

if object_exists(
    client,
    MINIO_BUCKET,
    success_marker
):

    print()
    print("----------------------------------")

    print(
        f"PLUTO version {TARGET_VERSION} "
        "is already fully loaded."
    )

    print(
        f"Bronze path: "
        f"{base_path}"
    )

    print(
        "No duplicate backfill is required."
    )

    print("----------------------------------")
    print("PLUTO YEARLY BACKFILL COMPLETED")
    print("==================================")

    raise SystemExit(0)


# --------------------------------------------------
# 12. Start full snapshot backfill
# --------------------------------------------------

print()
print(
    "Snapshot was not found in Bronze."
)

print(
    "Starting full PLUTO backfill..."
)

print("----------------------------------")


offset = 0
page_number = 1
total_records = 0


# --------------------------------------------------
# 13. Download PLUTO page by page
# --------------------------------------------------

while True:

    params = {

        "$limit": PAGE_SIZE,

        "$offset": offset,

        "$order": "bbl ASC"
    }


    response = fetch_data(
        params
    )

    data = response.json()


    # --------------------------------------------------
    # 14. Stop when there are no more records
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
    # 15. Preserve raw API JSON
    # --------------------------------------------------

    raw_json = response.content

    json_stream = BytesIO(
        raw_json
    )


    # --------------------------------------------------
    # 16. Build MinIO object name
    # --------------------------------------------------

    object_name = (
        base_path
        + f"page_{page_number:05d}.json"
    )


    # --------------------------------------------------
    # 17. Save page to Bronze
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
    # 18. Update counters
    # --------------------------------------------------

    total_records += (
        records_in_page
    )


    # --------------------------------------------------
    # 19. Last page?
    # --------------------------------------------------

    if records_in_page < PAGE_SIZE:
        break


    offset += PAGE_SIZE
    page_number += 1


# --------------------------------------------------
# 20. Create _SUCCESS marker
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


# --------------------------------------------------
# 21. Final summary
# --------------------------------------------------

print()
print("==================================")
print("PLUTO YEARLY BACKFILL COMPLETED")
print("----------------------------------")

print(
    f"Project year: "
    f"{PROJECT_YEAR}"
)

print(
    f"PLUTO version: "
    f"{TARGET_VERSION}"
)

print(
    f"Records loaded: "
    f"{total_records}"
)

print(
    f"Pages saved: "
    f"{page_number if total_records else 0}"
)

print(
    f"Bronze path: "
    f"{base_path}"
)

print(
    "SUCCESS marker created."
)

print("==================================")