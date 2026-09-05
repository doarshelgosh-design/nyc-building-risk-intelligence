import json
import sys

import requests
from pyspark.sql import functions as F


# ==================================================
# PROJECT CONFIG
# ==================================================

PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from spark_session import create_spark_session
from minio_config import minio_path


# ==================================================
# ELASTICSEARCH CONFIG
# ==================================================

ELASTIC_URL = "http://host.docker.internal:9200"
INDEX_NAME = "pluto_address_index"

# Smaller batches reduce memory/load pressure
BULK_SIZE = 500

# Helps us verify that all documents were updated
LOAD_VERSION = "street_normalization_v2"


# ==================================================
# CREATE SPARK SESSION
# ==================================================

spark = create_spark_session(
    "NYC Building Risk - Load PLUTO to Elasticsearch"
)


# ==================================================
# READ PLUTO SILVER
# ==================================================

PLUTO_PATH = minio_path(
    "silver/pluto/version=26v2"
)

pluto_df = (
    spark.read
    .parquet(PLUTO_PATH)
)


# ==================================================
# ADDRESS NORMALIZATION FUNCTIONS
# ==================================================

def normalize_address(column):
    """
    Basic normalization.

    Example:
        800 Concourse Village W.
    ->
        800 CONCOURSE VILLAGE W
    """

    return F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.upper(column),
                r"[^A-Z0-9 ]",
                " "
            ),
            r"\s+",
            " "
        )
    )


def extract_house_number(column):
    """
    Extract house number from beginning of address.

    Examples:
        800 CONCOURSE VILLAGE W -> 800
        123-45 QUEENS BLVD      -> 123-45
        25A BROADWAY            -> 25A
    """

    value = F.regexp_extract(
        F.upper(F.trim(column)),
        r"^([0-9]+(?:-[0-9]+)?[A-Z]?)\s+",
        1
    )

    return F.when(
        F.length(value) > 0,
        value
    )


def extract_street_name(column):
    """
    Removes house number.

    Example:
        800 CONCOURSE VILLAGE WEST
    ->
        CONCOURSE VILLAGE WEST
    """

    street = F.regexp_replace(
        F.upper(F.trim(column)),
        r"^[0-9]+(?:-[0-9]+)?[A-Z]?\s+",
        ""
    )

    return normalize_address(street)


def normalize_street_name(column):
    """
    Standardizes common NYC street-name variations.

    Examples:
        CONCOURSE VILLAGE WEST
        -> CONCOURSE VILLAGE W

        EAST 159 STREET
        -> E 159 ST

        QUEENS BOULEVARD
        -> QUEENS BLVD
    """

    street = normalize_address(column)

    # Directions
    street = F.regexp_replace(
        street,
        r"\bWEST\b",
        "W"
    )

    street = F.regexp_replace(
        street,
        r"\bEAST\b",
        "E"
    )

    street = F.regexp_replace(
        street,
        r"\bNORTH\b",
        "N"
    )

    street = F.regexp_replace(
        street,
        r"\bSOUTH\b",
        "S"
    )

    # Street types
    street = F.regexp_replace(
        street,
        r"\bSTREET\b",
        "ST"
    )

    street = F.regexp_replace(
        street,
        r"\bAVENUE\b",
        "AVE"
    )

    street = F.regexp_replace(
        street,
        r"\bBOULEVARD\b",
        "BLVD"
    )

    street = F.regexp_replace(
        street,
        r"\bROAD\b",
        "RD"
    )

    street = F.regexp_replace(
        street,
        r"\bDRIVE\b",
        "DR"
    )

    street = F.regexp_replace(
        street,
        r"\bPLACE\b",
        "PL"
    )

    street = F.regexp_replace(
        street,
        r"\bCOURT\b",
        "CT"
    )

    street = F.regexp_replace(
        street,
        r"\bLANE\b",
        "LN"
    )

    street = F.regexp_replace(
        street,
        r"\bPARKWAY\b",
        "PKWY"
    )

    street = F.regexp_replace(
        street,
        r"\bTERRACE\b",
        "TER"
    )

    # Clean spaces again after replacements
    street = F.trim(
        F.regexp_replace(
            street,
            r"\s+",
            " "
        )
    )

    return street


# ==================================================
# PREPARE ELASTICSEARCH DOCUMENTS
# ==================================================

elastic_df = (
    pluto_df

    .select(
        "bbl",
        "address",
        "borough",
        "latitude",
        "longitude",
        "yearbuilt",
        "landuse",
        "bldgclass"
    )

    .filter(
        F.col("bbl").isNotNull()
    )

    # Full normalized address
    .withColumn(
        "normalized_address",
        normalize_address(
            F.col("address")
        )
    )

    # Example: 800
    .withColumn(
        "house_number",
        extract_house_number(
            F.col("address")
        )
    )

    # Example: CONCOURSE VILLAGE WEST
    .withColumn(
        "street_name",
        extract_street_name(
            F.col("address")
        )
    )

    # Example: CONCOURSE VILLAGE W
    .withColumn(
        "normalized_street_name",
        normalize_street_name(
            extract_street_name(
                F.col("address")
            )
        )
    )
)


# ==================================================
# COUNT
# ==================================================

total_rows = elastic_df.count()

print()
print("========================================")
print("PLUTO -> ELASTICSEARCH")
print("========================================")
print(f"Rows to index: {total_rows:,}")
print(f"Index: {INDEX_NAME}")
print(f"Load version: {LOAD_VERSION}")
print("========================================")


# ==================================================
# BULK INDEXING
# ==================================================

def send_bulk(batch):

    if not batch:
        return

    lines = []

    for row in batch:

        document = {
            "bbl": row["bbl"],
            "address": row["address"],
            "normalized_address": row["normalized_address"],
            "house_number": row["house_number"],
            "street_name": row["street_name"],
            "normalized_street_name": row["normalized_street_name"],
            "borough": row["borough"],
            "yearbuilt": row["yearbuilt"],
            "landuse": row["landuse"],
            "bldgclass": row["bldgclass"],
            "load_version": LOAD_VERSION
        }

        # Add geo_point only when coordinates exist
        if (
            row["latitude"] is not None
            and row["longitude"] is not None
        ):
            document["location"] = {
                "lat": row["latitude"],
                "lon": row["longitude"]
            }

        # BBL is Elasticsearch document ID
        action = {
            "index": {
                "_index": INDEX_NAME,
                "_id": row["bbl"]
            }
        }

        lines.append(
            json.dumps(action)
        )

        lines.append(
            json.dumps(document)
        )

    payload = "\n".join(lines) + "\n"

    response = requests.post(
        f"{ELASTIC_URL}/_bulk",
        headers={
            "Content-Type": "application/x-ndjson"
        },
        data=payload,
        timeout=120
    )

    response.raise_for_status()

    result = response.json()

    if result.get("errors"):

        failed_items = [
            item
            for item in result["items"]
            if list(item.values())[0].get("error")
        ]

        print(
            f"ERROR: {len(failed_items)} documents failed"
        )

        for item in failed_items[:5]:
            print(item)

        raise RuntimeError(
            f"Bulk indexing errors detected: "
            f"{len(failed_items)} failed documents"
        )


# ==================================================
# PROCESS SPARK PARTITION
# ==================================================

def process_partition(rows):

    batch = []

    for row in rows:

        batch.append(
            row.asDict()
        )

        if len(batch) >= BULK_SIZE:

            send_bulk(batch)

            batch = []

    if batch:
        send_bulk(batch)


# ==================================================
# LOAD INTO ELASTICSEARCH
# ==================================================

# Only 2 partitions send data to Elasticsearch
# at the same time.
# This reduces memory pressure.
elastic_df.coalesce(2).foreachPartition(
    process_partition
)


# ==================================================
# REFRESH INDEX
# ==================================================

refresh_response = requests.post(
    f"{ELASTIC_URL}/{INDEX_NAME}/_refresh",
    timeout=30
)

refresh_response.raise_for_status()


# ==================================================
# VALIDATE TOTAL DOCUMENT COUNT
# ==================================================

count_response = requests.get(
    f"{ELASTIC_URL}/{INDEX_NAME}/_count",
    timeout=30
)

count_response.raise_for_status()

indexed_count = (
    count_response
    .json()
    .get("count", 0)
)


# ==================================================
# FINAL RESULT
# ==================================================

print()
print("========================================")
print("ELASTICSEARCH LOAD COMPLETED")
print("========================================")
print(f"PLUTO rows: {total_rows:,}")
print(f"Indexed documents: {indexed_count:,}")
print(f"Index: {INDEX_NAME}")
print(f"Load version: {LOAD_VERSION}")
print("========================================")


spark.stop()