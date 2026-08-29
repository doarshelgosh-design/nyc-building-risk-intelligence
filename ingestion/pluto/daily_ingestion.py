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
# 4. Connect to MinIO
# --------------------------------------------------

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)


# --------------------------------------------------
# 5. Check whether object exists in MinIO
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
# 6. Request API with retry
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
# 7. Get current PLUTO version
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


# Make version safe for MinIO path
safe_version = (
    current_version
    .replace("/", "_")
    .replace(" ", "_")
)


print()
print("==================================")
print("PLUTO DAILY CHECK")
print("==================================")

print(
    f"Current PLUTO version: "
    f"{current_version}"
)


# --------------------------------------------------
# 8. Define snapshot path
# --------------------------------------------------

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
# 9. Skip if this version is already loaded
# --------------------------------------------------

if object_exists(
    client,
    MINIO_BUCKET,
    success_marker
):

    print()
    print(
        "This PLUTO version is already "
        "available in Bronze."
    )

    print(
        "No new snapshot is required."
    )

    print("==================================")

    raise SystemExit(0)


# --------------------------------------------------
# 10. Download full PLUTO snapshot
# --------------------------------------------------

print()
print(
    "New PLUTO version detected."
)

print(
    "Starting full snapshot ingestion..."
)

print("----------------------------------")


offset = 0
page_number = 1
total_records = 0


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
    # 11. Preserve raw JSON
    # --------------------------------------------------

    raw_json = response.content

    json_stream = BytesIO(
        raw_json
    )


    # --------------------------------------------------
    # 12. Save page to Bronze
    # --------------------------------------------------

    object_name = (
        base_path
        + f"page_{page_number:05d}.json"
    )


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


    total_records += (
        records_in_page
    )


    # --------------------------------------------------
    # 13. Last page?
    # --------------------------------------------------

    if records_in_page < PAGE_SIZE:
        break


    offset += PAGE_SIZE
    page_number += 1


# --------------------------------------------------
# 14. Create _SUCCESS marker
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
# 15. Final summary
# --------------------------------------------------

print()
print("==================================")
print("PLUTO SNAPSHOT COMPLETED")
print("----------------------------------")

print(
    f"Version: "
    f"{current_version}"
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