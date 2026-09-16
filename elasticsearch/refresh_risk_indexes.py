import json
import math
import sys
import time
from datetime import date, datetime
from decimal import Decimal

import requests
from pyspark.sql import functions as F


PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from spark_session import create_spark_session
from minio_config import minio_path


# ============================================================
# CONFIGURATION
# ============================================================

ES_URL = "http://host.docker.internal:9200"

BUILDING_INDEX = "building_risk_index"
PROPERTY_INDEX = "property_risk_index"

BUILDING_BATCH_SIZE = 250
PROPERTY_BATCH_SIZE = 250

MAX_RETRIES = 5
RETRY_BASE_SECONDS = 2

MODEL_VERSION = "elasticsearch_refresh_v1"


# ============================================================
# JSON / ELASTICSEARCH HELPERS
# ============================================================

def clean_for_json(value):
    if value is None:
        return None

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, dict):
        return {
            key: clean_for_json(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [
            clean_for_json(item)
            for item in value
        ]

    return value


def request_json(session, method, url, **kwargs):
    response = session.request(
        method,
        url,
        **kwargs,
    )

    response.raise_for_status()

    if response.content:
        return response.json()

    return {}


def check_elasticsearch(session):
    result = request_json(
        session,
        "GET",
        ES_URL,
        timeout=10,
    )

    version = result.get("version", {}).get("number")

    print("\n" + "-" * 60)
    print("ELASTICSEARCH CONNECTION")
    print("-" * 60)
    print("URL:", ES_URL)
    print("Version:", version)

    if not version:
        raise RuntimeError(
            "Elasticsearch connection succeeded but version was not returned."
        )

    return version


def delete_index_if_exists(session, index_name):
    response = session.delete(
        f"{ES_URL}/{index_name}",
        timeout=30,
    )

    if response.status_code not in (200, 404):
        response.raise_for_status()

    if response.status_code == 200:
        print(f"Deleted existing index: {index_name}")
    else:
        print(f"Index did not exist: {index_name}")


def create_index(session, index_name, mappings):
    body = {
        "settings": {
            "index": {
                "number_of_replicas": 0,
                "refresh_interval": "-1",
            }
        },
        "mappings": {
            "dynamic": True,
            "properties": mappings,
        },
    }

    result = request_json(
        session,
        "PUT",
        f"{ES_URL}/{index_name}",
        json=body,
        timeout=30,
    )

    if not result.get("acknowledged"):
        raise RuntimeError(
            f"Elasticsearch did not acknowledge index creation: {index_name}"
        )

    print(f"Created index: {index_name}")


def restore_index_settings(session, index_name):
    request_json(
        session,
        "PUT",
        f"{ES_URL}/{index_name}/_settings",
        json={
            "index": {
                "refresh_interval": "1s",
            }
        },
        timeout=30,
    )

    request_json(
        session,
        "POST",
        f"{ES_URL}/{index_name}/_refresh",
        timeout=30,
    )

    print(f"Restored refresh_interval and refreshed: {index_name}")


def get_index_count(session, index_name):
    result = request_json(
        session,
        "GET",
        f"{ES_URL}/{index_name}/_count",
        timeout=30,
    )

    return int(result["count"])


def send_bulk_batch(
    session,
    index_name,
    id_field,
    documents,
):
    bulk_lines = []

    for document in documents:
        document_id = document.get(id_field)

        if not document_id:
            raise RuntimeError(
                f"Cannot index document without {id_field}."
            )

        bulk_lines.append(
            json.dumps(
                {
                    "index": {
                        "_index": index_name,
                        "_id": document_id,
                    }
                },
                separators=(",", ":"),
            )
        )

        bulk_lines.append(
            json.dumps(
                document,
                separators=(",", ":"),
            )
        )

    payload = "\n".join(bulk_lines) + "\n"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(
                (
                    f"{ES_URL}/_bulk"
                    "?filter_path="
                    "errors,"
                    "items.*.index.status,"
                    "items.*.index.error"
                ),
                data=payload,
                headers={
                    "Content-Type": "application/x-ndjson"
                },
                timeout=60,
            )

            response.raise_for_status()

            result = response.json()

            if result.get("errors"):
                failures = []

                for item in result.get("items", []):
                    action = item.get("index", {})

                    if action.get("error"):
                        failures.append(action)

                print(
                    "Example bulk failures:",
                    failures[:3],
                )

                raise RuntimeError(
                    f"Bulk document errors: {len(failures)}"
                )

            return len(documents)

        except Exception as exc:
            print(
                f"Bulk attempt {attempt}/{MAX_RETRIES} "
                f"failed for {index_name}: {exc}"
            )

            if attempt == MAX_RETRIES:
                raise

            wait_seconds = attempt * RETRY_BASE_SECONDS
            print(
                f"Waiting {wait_seconds} seconds before retry..."
            )
            time.sleep(wait_seconds)

    raise RuntimeError(
        f"Unexpected bulk retry exit for {index_name}."
    )


# ============================================================
# SOURCE PREPARATION
# ============================================================

def prepare_building_for_elasticsearch(serving_building):
    return (
        serving_building
        .withColumn(
            "search_address",
            F.when(
                F.length(
                    F.trim(F.col("search_address"))
                ) > 0,
                F.col("search_address"),
            ).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "location",
            F.when(
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull(),
                F.struct(
                    F.col("latitude").alias("lat"),
                    F.col("longitude").alias("lon"),
                ),
            ),
        )
        .withColumn(
            "elasticsearch_model_version",
            F.lit(MODEL_VERSION),
        )
    )


def prepare_property_for_elasticsearch(serving_property):
    return (
        serving_property
        .withColumn(
            "search_address",
            F.when(
                F.length(
                    F.trim(F.col("search_address"))
                ) > 0,
                F.col("search_address"),
            ).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "location",
            F.when(
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull(),
                F.struct(
                    F.col("latitude").alias("lat"),
                    F.col("longitude").alias("lon"),
                ),
            ),
        )
        .withColumn(
            "elasticsearch_model_version",
            F.lit(MODEL_VERSION),
        )
    )


# ============================================================
# SOURCE DATA QUALITY
# ============================================================

def validate_source(
    df,
    id_field,
    expected_entity_type,
    label,
):
    stats = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct(id_field).alias("distinct_ids"),
        F.sum(
            F.when(
                F.col(id_field).isNull(),
                1,
            ).otherwise(0)
        ).alias("null_ids"),
        F.sum(
            F.when(
                F.col("entity_type") != expected_entity_type,
                1,
            ).otherwise(0)
        ).alias("invalid_entity_type"),
        F.sum(
            F.when(
                F.col("search_address").isNull()
                | (
                    F.length(
                        F.trim(F.col("search_address"))
                    ) == 0
                ),
                1,
            ).otherwise(0)
        ).alias("missing_address"),
        F.sum(
            F.when(
                F.col("location").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_location"),
    ).first()

    print("\n" + "-" * 60)
    print(f"{label} DATA QUALITY")
    print("-" * 60)
    print("Rows:", f"{stats['rows']:,}")
    print(
        f"Distinct {id_field}:",
        f"{stats['distinct_ids']:,}",
    )
    print(
        f"Null {id_field}:",
        f"{stats['null_ids'] or 0:,}",
    )
    print(
        "Invalid entity_type:",
        f"{stats['invalid_entity_type'] or 0:,}",
    )
    print(
        "Missing / empty search_address:",
        f"{stats['missing_address'] or 0:,}",
    )
    print(
        "Missing location:",
        f"{stats['missing_location'] or 0:,}",
    )

    if stats["rows"] == 0:
        raise RuntimeError(
            f"DQ FAILED: {label} is empty."
        )

    if stats["rows"] != stats["distinct_ids"]:
        raise RuntimeError(
            f"DQ FAILED: {label} contains duplicate {id_field}."
        )

    if (stats["null_ids"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains null {id_field}."
        )

    if (stats["invalid_entity_type"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains invalid entity_type."
        )

    return int(stats["rows"])


# ============================================================
# BULK STREAMING
# ============================================================

def stream_to_elasticsearch(
    df,
    session,
    index_name,
    id_field,
    batch_size,
    progress_every_batches,
):
    total_processed = 0
    batch_number = 0
    batch = []

    print("\n" + "-" * 60)
    print(f"BULK LOAD: {index_name}")
    print("-" * 60)
    print("Batch size:", batch_size)

    for row in df.toLocalIterator():
        document = clean_for_json(
            row.asDict(recursive=True)
        )

        batch.append(document)

        if len(batch) >= batch_size:
            sent = send_bulk_batch(
                session=session,
                index_name=index_name,
                id_field=id_field,
                documents=batch,
            )

            total_processed += sent
            batch_number += 1

            if (
                batch_number == 1
                or batch_number % progress_every_batches == 0
            ):
                print(
                    f"Batch {batch_number} | "
                    f"Processed: {total_processed:,}"
                )

            batch = []

            # Small pause retained from the validated notebook
            # to avoid overloading the local Elasticsearch node.
            time.sleep(0.05)

    if batch:
        sent = send_bulk_batch(
            session=session,
            index_name=index_name,
            id_field=id_field,
            documents=batch,
        )

        total_processed += sent
        batch_number += 1

    print(
        f"Finished {index_name}. "
        f"Documents processed: {total_processed:,}"
    )

    return total_processed


# ============================================================
# INDEX MAPPINGS
# ============================================================

BUILDING_MAPPINGS = {
    "building_id": {
        "type": "keyword",
    },
    "bin": {
        "type": "keyword",
    },
    "property_id": {
        "type": "keyword",
    },
    "bbl": {
        "type": "keyword",
    },
    "resolved_bbl": {
        "type": "keyword",
    },
    "current_bbl": {
        "type": "keyword",
    },
    "borough": {
        "type": "keyword",
    },
    "zipcode": {
        "type": "keyword",
    },
    "entity_type": {
        "type": "keyword",
    },
    "building_risk_level": {
        "type": "keyword",
    },
    "property_risk_level": {
        "type": "keyword",
    },
    "property_risk_source": {
        "type": "keyword",
    },
    "current_address": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256,
            }
        },
    },
    "property_address": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256,
            }
        },
    },
    "search_address": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256,
            }
        },
    },
    "search_aliases": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256,
            }
        },
    },
    "location": {
        "type": "geo_point",
    },
    "risk_as_of_date": {
        "type": "date",
    },
    "serving_calculated_at": {
        "type": "date",
    },
    "serving_model_version": {
        "type": "keyword",
    },
    "elasticsearch_model_version": {
        "type": "keyword",
    },
}


PROPERTY_MAPPINGS = {
    "property_id": {
        "type": "keyword",
    },
    "bbl": {
        "type": "keyword",
    },
    "borough": {
        "type": "keyword",
    },
    "zipcode": {
        "type": "keyword",
    },
    "entity_type": {
        "type": "keyword",
    },
    "property_risk_level": {
        "type": "keyword",
    },
    "property_risk_source": {
        "type": "keyword",
    },
    "property_address": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256,
            }
        },
    },
    "search_address": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256,
            }
        },
    },
    "location": {
        "type": "geo_point",
    },
    "risk_as_of_date": {
        "type": "date",
    },
    "serving_calculated_at": {
        "type": "date",
    },
    "serving_model_version": {
        "type": "keyword",
    },
    "elasticsearch_model_version": {
        "type": "keyword",
    },
}


# ============================================================
# MAIN
# ============================================================

def main():
    spark = create_spark_session(
        "NYC Building Risk - Elasticsearch Refresh Production"
    )
    spark.sparkContext.setLogLevel("WARN")

    session = requests.Session()

    try:
        print("\n" + "=" * 60)
        print("ELASTICSEARCH REFRESH PRODUCTION JOB")
        print("=" * 60)
        print("Model version:", MODEL_VERSION)

        check_elasticsearch(session)

        serving_building_path = minio_path(
            "serving/building"
        )
        serving_property_path = minio_path(
            "serving/property"
        )

        print("\nINPUTS")
        print("Serving Building:", serving_building_path)
        print("Serving Property:", serving_property_path)

        serving_building = spark.read.parquet(
            serving_building_path
        )
        serving_property = spark.read.parquet(
            serving_property_path
        )

        serving_building_es = (
            prepare_building_for_elasticsearch(
                serving_building
            )
        )
        serving_property_es = (
            prepare_property_for_elasticsearch(
                serving_property
            )
        )

        expected_building_count = validate_source(
            serving_building_es,
            id_field="building_id",
            expected_entity_type="BUILDING",
            label="SERVING BUILDING",
        )

        expected_property_count = validate_source(
            serving_property_es,
            id_field="property_id",
            expected_entity_type="PROPERTY",
            label="SERVING PROPERTY",
        )

        print("\nEXPECTED DOCUMENT COUNTS")
        print(
            "Building documents:",
            f"{expected_building_count:,}",
        )
        print(
            "Property documents:",
            f"{expected_property_count:,}",
        )

        # Full rebuild guarantees that documents which disappeared
        # from the Serving Layer cannot remain stale in Elasticsearch.
        delete_index_if_exists(
            session,
            BUILDING_INDEX,
        )
        create_index(
            session,
            BUILDING_INDEX,
            BUILDING_MAPPINGS,
        )

        processed_building = stream_to_elasticsearch(
            df=serving_building_es,
            session=session,
            index_name=BUILDING_INDEX,
            id_field="building_id",
            batch_size=BUILDING_BATCH_SIZE,
            progress_every_batches=10,
        )

        restore_index_settings(
            session,
            BUILDING_INDEX,
        )

        building_es_count = get_index_count(
            session,
            BUILDING_INDEX,
        )

        print(
            "Building Elasticsearch documents:",
            f"{building_es_count:,}",
        )

        if processed_building != expected_building_count:
            raise RuntimeError(
                "DQ FAILED: Processed Building count does not "
                "match Serving Building count."
            )

        if building_es_count != expected_building_count:
            raise RuntimeError(
                "DQ FAILED: building_risk_index document count "
                "does not match Serving Building count."
            )

        delete_index_if_exists(
            session,
            PROPERTY_INDEX,
        )
        create_index(
            session,
            PROPERTY_INDEX,
            PROPERTY_MAPPINGS,
        )

        processed_property = stream_to_elasticsearch(
            df=serving_property_es,
            session=session,
            index_name=PROPERTY_INDEX,
            id_field="property_id",
            batch_size=PROPERTY_BATCH_SIZE,
            progress_every_batches=20,
        )

        restore_index_settings(
            session,
            PROPERTY_INDEX,
        )

        property_es_count = get_index_count(
            session,
            PROPERTY_INDEX,
        )

        print(
            "Property Elasticsearch documents:",
            f"{property_es_count:,}",
        )

        if processed_property != expected_property_count:
            raise RuntimeError(
                "DQ FAILED: Processed Property count does not "
                "match Serving Property count."
            )

        if property_es_count != expected_property_count:
            raise RuntimeError(
                "DQ FAILED: property_risk_index document count "
                "does not match Serving Property count."
            )

        print("\n" + "=" * 60)
        print("ELASTICSEARCH REFRESH COMPLETED")
        print("=" * 60)
        print(
            f"{BUILDING_INDEX}: {building_es_count:,}"
        )
        print(
            f"{PROPERTY_INDEX}: {property_es_count:,}"
        )

    finally:
        session.close()
        spark.stop()


if __name__ == "__main__":
    main()
