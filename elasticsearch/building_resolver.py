import math
import re
import sys
from difflib import SequenceMatcher

import requests
from pyspark.sql import SparkSession
from pyspark.sql import types as T


# ==================================================
# PROJECT CONFIG
# ==================================================

PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from minio_config import configure_minio, minio_path


# ==================================================
# ELASTICSEARCH CONFIG
# ==================================================

ELASTIC_URL = "http://host.docker.internal:9200"
INDEX_NAME = "pluto_address_index"

SEARCH_RADIUS = "200m"
MAX_CANDIDATES = 25


# ==================================================
# CREATE SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .appName("NYC Building Risk - Elasticsearch Resolver")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)

configure_minio(spark)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# PATHS
# ==================================================

UNRESOLVED_PATH = minio_path(
    "gold/building_identity/unresolved"
)

RESULT_PATH = minio_path(
    "gold/building_identity/elasticsearch_resolver"
)


# ==================================================
# NORMALIZATION
# ==================================================

def clean_text(value):

    if value is None:
        return None

    value = str(value).upper().strip()

    value = re.sub(
        r"[^A-Z0-9 ]",
        " ",
        value
    )

    value = re.sub(
        r"\s+",
        " ",
        value
    ).strip()

    return value or None


def extract_house_number(address):

    address = clean_text(address)

    if not address:
        return None

    match = re.match(
        r"^([0-9]+(?:-[0-9]+)?[A-Z]?)\s+",
        address
    )

    if not match:
        return None

    return match.group(1)


def extract_street_name(address):

    address = clean_text(address)

    if not address:
        return None

    street = re.sub(
        r"^[0-9]+(?:-[0-9]+)?[A-Z]?\s+",
        "",
        address
    )

    return street.strip() or None


def normalize_street_name(street):

    street = clean_text(street)

    if not street:
        return None

    replacements = {
        "WEST": "W",
        "EAST": "E",
        "NORTH": "N",
        "SOUTH": "S",
        "STREET": "ST",
        "AVENUE": "AVE",
        "BOULEVARD": "BLVD",
        "ROAD": "RD",
        "DRIVE": "DR",
        "PLACE": "PL",
        "COURT": "CT",
        "LANE": "LN",
        "PARKWAY": "PKWY",
        "TERRACE": "TER"
    }

    for old, new in replacements.items():

        street = re.sub(
            rf"\b{old}\b",
            new,
            street
        )

    street = re.sub(
        r"\s+",
        " ",
        street
    ).strip()

    return street or None


def normalize_borough(value):

    value = clean_text(value)

    if not value:
        return None

    borough_map = {
        "1": "MANHATTAN",
        "MN": "MANHATTAN",
        "MANHATTAN": "MANHATTAN",

        "2": "BRONX",
        "BX": "BRONX",
        "BRONX": "BRONX",

        "3": "BROOKLYN",
        "BK": "BROOKLYN",
        "BROOKLYN": "BROOKLYN",

        "4": "QUEENS",
        "QN": "QUEENS",
        "QUEENS": "QUEENS",

        "5": "STATEN ISLAND",
        "SI": "STATEN ISLAND",
        "STATEN ISLAND": "STATEN ISLAND"
    }

    return borough_map.get(
        value,
        value
    )


# ==================================================
# STREET SIMILARITY
# ==================================================

def street_similarity(source, candidate):

    if not source or not candidate:
        return 0.0

    return SequenceMatcher(
        None,
        source,
        candidate
    ).ratio()


# ==================================================
# ELASTICSEARCH SESSION
# ==================================================

session = requests.Session()


# ==================================================
# ELASTICSEARCH CANDIDATE SEARCH
# ==================================================

def search_candidates(
    house_number,
    normalized_street,
    borough,
    latitude,
    longitude
):

    source_fields = [
        "bbl",
        "address",
        "house_number",
        "street_name",
        "normalized_street_name",
        "borough",
        "location",
        "yearbuilt",
        "landuse",
        "bldgclass"
    ]

    # ----------------------------------------------
    # CASE 1: coordinates exist
    # ----------------------------------------------

    if (
        latitude is not None
        and longitude is not None
    ):

        filters = [
            {
                "geo_distance": {
                    "distance": SEARCH_RADIUS,
                    "location": {
                        "lat": latitude,
                        "lon": longitude
                    }
                }
            }
        ]

        if borough:
            filters.append(
                {
                    "term": {
                        "borough": borough
                    }
                }
            )

        query = {
            "size": MAX_CANDIDATES,

            "_source": source_fields,

            "query": {
                "bool": {
                    "filter": filters
                }
            },

            "sort": [
                {
                    "_geo_distance": {
                        "location": {
                            "lat": latitude,
                            "lon": longitude
                        },
                        "order": "asc",
                        "unit": "m",
                        "distance_type": "arc"
                    }
                }
            ]
        }

    # ----------------------------------------------
    # CASE 2: no coordinates, but address exists
    # ----------------------------------------------

    elif normalized_street:

        must = [
            {
                "match": {
                    "normalized_street_name": {
                        "query": normalized_street,
                        "fuzziness": "AUTO",
                        "operator": "and"
                    }
                }
            }
        ]

        filters = []

        if borough:
            filters.append(
                {
                    "term": {
                        "borough": borough
                    }
                }
            )

        if house_number:
            filters.append(
                {
                    "term": {
                        "house_number": house_number
                    }
                }
            )

        query = {
            "size": MAX_CANDIDATES,

            "_source": source_fields,

            "query": {
                "bool": {
                    "must": must,
                    "filter": filters
                }
            }
        }

    else:
        return []

    response = session.post(
        f"{ELASTIC_URL}/{INDEX_NAME}/_search",
        json=query,
        timeout=30
    )

    response.raise_for_status()

    return (
        response
        .json()
        .get("hits", {})
        .get("hits", [])
    )


# ==================================================
# SCORE ONE CANDIDATE
# ==================================================

def evaluate_candidate(
    source_house_number,
    source_street,
    source_borough,
    candidate_hit
):

    candidate = candidate_hit.get(
        "_source",
        {}
    )

    candidate_house = candidate.get(
        "house_number"
    )

    candidate_street = candidate.get(
        "normalized_street_name"
    )

    candidate_borough = candidate.get(
        "borough"
    )

    distance_m = None

    sort_values = candidate_hit.get(
        "sort"
    )

    if sort_values:
        distance_m = float(
            sort_values[0]
        )

    house_match = (
        source_house_number is not None
        and candidate_house is not None
        and source_house_number == candidate_house
    )

    street_exact = (
        source_street is not None
        and candidate_street is not None
        and source_street == candidate_street
    )

    similarity = street_similarity(
        source_street,
        candidate_street
    )

    borough_match = (
        source_borough is not None
        and candidate_borough is not None
        and source_borough == candidate_borough
    )

    # ----------------------------------------------
    # Deterministic score
    # ----------------------------------------------

    score = 0.0

    if house_match:
        score += 40

    if street_exact:
        score += 40
    else:
        score += similarity * 30

    if borough_match:
        score += 10

    if distance_m is not None:

        if distance_m <= 25:
            score += 10

        elif distance_m <= 50:
            score += 8

        elif distance_m <= 100:
            score += 5

        elif distance_m <= 200:
            score += 2

    location = candidate.get(
        "location"
    ) or {}

    return {
        "candidate_bbl": candidate.get("bbl"),

        "candidate_address": candidate.get(
            "address"
        ),

        "candidate_house_number": candidate_house,

        "candidate_street": candidate_street,

        "candidate_borough": candidate_borough,

        "candidate_latitude": location.get(
            "lat"
        ),

        "candidate_longitude": location.get(
            "lon"
        ),

        "distance_m": distance_m,

        "house_number_match": house_match,

        "street_exact_match": street_exact,

        "street_similarity": similarity,

        "borough_match": borough_match,

        "resolver_score": score
    }


# ==================================================
# CLASSIFY CANDIDATES
# ==================================================

def classify_candidates(candidates):

    if not candidates:

        return {
            "match_confidence": "NONE",
            "resolution_status": "NO_CANDIDATE"
        }

    candidates.sort(
        key=lambda x: (
            -x["resolver_score"],
            x["distance_m"]
            if x["distance_m"] is not None
            else math.inf
        )
    )

    best = candidates[0]

    # ----------------------------------------------
    # HIGH
    # Exact house + exact street + borough
    # ----------------------------------------------

    high_candidates = [
        c
        for c in candidates
        if (
            c["house_number_match"]
            and c["street_exact_match"]
            and c["borough_match"]
            and (
                c["distance_m"] is None
                or c["distance_m"] <= 100
            )
        )
    ]

    if len(high_candidates) == 1:

        best = high_candidates[0]

        return {
            **best,
            "match_confidence": "HIGH",
            "resolution_status": "AUTO_RESOLVED"
        }

    if len(high_candidates) > 1:

        return {
            **best,
            "match_confidence": "REVIEW",
            "resolution_status": "AMBIGUOUS"
        }

    # ----------------------------------------------
    # MEDIUM
    # ----------------------------------------------

    if (
        best["house_number_match"]
        and best["street_similarity"] >= 0.90
        and best["borough_match"]
        and best["distance_m"] is not None
        and best["distance_m"] <= 50
    ):

        return {
            **best,
            "match_confidence": "MEDIUM",
            "resolution_status": "REVIEW_REQUIRED"
        }

    # ----------------------------------------------
    # LOW
    # ----------------------------------------------

    return {
        **best,
        "match_confidence": "LOW",
        "resolution_status": "REVIEW_REQUIRED"
    }


# ==================================================
# READ UNRESOLVED BUILDINGS
# ==================================================

unresolved_df = (
    spark.read
    .parquet(UNRESOLVED_PATH)
)


# ==================================================
# PROCESS BUILDINGS
# ==================================================

results = []

processed = 0


for row in unresolved_df.toLocalIterator():

    processed += 1

    source_address = row["current_address"]

    source_house_number = extract_house_number(
        source_address
    )

    source_street = normalize_street_name(
        extract_street_name(
            source_address
        )
    )

    source_borough = normalize_borough(
        row["borough"]
    )

    latitude = row["latitude"]
    longitude = row["longitude"]

    # ----------------------------------------------
    # NO INPUT
    # ----------------------------------------------

    if (
        not source_address
        and latitude is None
        and longitude is None
    ):

        results.append(
            {
                "bin": row["bin"],
                "source_bbl": row["current_bbl"],
                "source_address": source_address,
                "source_house_number": source_house_number,
                "source_street": source_street,
                "source_borough": source_borough,
                "source_latitude": latitude,
                "source_longitude": longitude,

                "candidate_bbl": None,
                "candidate_address": None,
                "candidate_house_number": None,
                "candidate_street": None,
                "candidate_borough": None,
                "candidate_latitude": None,
                "candidate_longitude": None,

                "distance_m": None,

                "house_number_match": False,
                "street_exact_match": False,
                "street_similarity": 0.0,
                "borough_match": False,

                "resolver_score": 0.0,

                "match_method": "ELASTICSEARCH",
                "match_confidence": "NONE",
                "resolution_status": "NO_INPUT"
            }
        )

        continue

    hits = search_candidates(
        source_house_number,
        source_street,
        source_borough,
        latitude,
        longitude
    )

    evaluated = [
        evaluate_candidate(
            source_house_number,
            source_street,
            source_borough,
            hit
        )
        for hit in hits
    ]

    classified = classify_candidates(
        evaluated
    )

    results.append(
        {
            "bin": row["bin"],
            "source_bbl": row["current_bbl"],
            "source_address": source_address,
            "source_house_number": source_house_number,
            "source_street": source_street,
            "source_borough": source_borough,
            "source_latitude": latitude,
            "source_longitude": longitude,

            "candidate_bbl": classified.get(
                "candidate_bbl"
            ),

            "candidate_address": classified.get(
                "candidate_address"
            ),

            "candidate_house_number": classified.get(
                "candidate_house_number"
            ),

            "candidate_street": classified.get(
                "candidate_street"
            ),

            "candidate_borough": classified.get(
                "candidate_borough"
            ),

            "candidate_latitude": classified.get(
                "candidate_latitude"
            ),

            "candidate_longitude": classified.get(
                "candidate_longitude"
            ),

            "distance_m": classified.get(
                "distance_m"
            ),

            "house_number_match": classified.get(
                "house_number_match",
                False
            ),

            "street_exact_match": classified.get(
                "street_exact_match",
                False
            ),

            "street_similarity": classified.get(
                "street_similarity",
                0.0
            ),

            "borough_match": classified.get(
                "borough_match",
                False
            ),

            "resolver_score": classified.get(
                "resolver_score",
                0.0
            ),

            "match_method": "ELASTICSEARCH",

            "match_confidence": classified[
                "match_confidence"
            ],

            "resolution_status": classified[
                "resolution_status"
            ]
        }
    )

    if processed % 50 == 0:

        print(
            f"Processed: {processed}"
        )


# ==================================================
# RESULT SCHEMA
# ==================================================

schema = T.StructType([

    T.StructField("bin", T.StringType(), True),
    T.StructField("source_bbl", T.StringType(), True),
    T.StructField("source_address", T.StringType(), True),
    T.StructField("source_house_number", T.StringType(), True),
    T.StructField("source_street", T.StringType(), True),
    T.StructField("source_borough", T.StringType(), True),

    T.StructField("source_latitude", T.DoubleType(), True),
    T.StructField("source_longitude", T.DoubleType(), True),

    T.StructField("candidate_bbl", T.StringType(), True),
    T.StructField("candidate_address", T.StringType(), True),
    T.StructField("candidate_house_number", T.StringType(), True),
    T.StructField("candidate_street", T.StringType(), True),
    T.StructField("candidate_borough", T.StringType(), True),

    T.StructField("candidate_latitude", T.DoubleType(), True),
    T.StructField("candidate_longitude", T.DoubleType(), True),

    T.StructField("distance_m", T.DoubleType(), True),

    T.StructField("house_number_match", T.BooleanType(), True),
    T.StructField("street_exact_match", T.BooleanType(), True),
    T.StructField("street_similarity", T.DoubleType(), True),
    T.StructField("borough_match", T.BooleanType(), True),

    T.StructField("resolver_score", T.DoubleType(), True),

    T.StructField("match_method", T.StringType(), True),
    T.StructField("match_confidence", T.StringType(), True),
    T.StructField("resolution_status", T.StringType(), True)
])


# ==================================================
# CREATE RESULT DATAFRAME
# ==================================================

result_df = spark.createDataFrame(
    results,
    schema=schema
)


# ==================================================
# WRITE RESULTS TO MINIO
# ==================================================

(
    result_df
    .write
    .mode("overwrite")
    .parquet(RESULT_PATH)
)


print()
print("========================================")
print("ELASTICSEARCH RESOLVER COMPLETED")
print("========================================")
print(f"Processed buildings: {processed}")
print(f"Output: {RESULT_PATH}")
print("========================================")


# ==================================================
# SUMMARY
# ==================================================

(
    result_df
    .groupBy(
        "match_confidence",
        "resolution_status"
    )
    .count()
    .orderBy(
        "match_confidence",
        "resolution_status"
    )
    .show(
        100,
        truncate=False
    )
)


spark.stop()