import sys

from pyspark import StorageLevel
from pyspark.sql import functions as F


# --------------------------------------------------
# 1. Project paths
# --------------------------------------------------

PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)


# --------------------------------------------------
# 2. Project imports
# --------------------------------------------------

from spark_session import create_spark_session
from minio_config import minio_path


# --------------------------------------------------
# 3. Create SparkSession
# --------------------------------------------------

spark = create_spark_session(
    "NYC Building Risk - Silver NYC 311"
)


# --------------------------------------------------
# 4. Bronze input
#
# Read only the production daily backfill structure.
# This intentionally excludes old experimental files.
# --------------------------------------------------

BRONZE_PATH = minio_path(
    "bronze/311/"
    "year=*/"
    "month=*/"
    "day=*/"
    "backfill/"
    "page_*.json"
)


# --------------------------------------------------
# 5. Silver output
# --------------------------------------------------

SILVER_PATH = minio_path(
    "silver/nyc_311"
)


print()
print("========================================")
print("NYC 311 BRONZE -> SILVER")
print("========================================")

print(
    f"Bronze path: {BRONZE_PATH}"
)

print(
    f"Silver path: {SILVER_PATH}"
)


# --------------------------------------------------
# 6. Read Bronze JSON
# --------------------------------------------------

bronze_df = (
    spark.read
    .option(
        "multiline",
        "true"
    )
    .json(
        BRONZE_PATH
    )
)


# --------------------------------------------------
# 7. Count Bronze records
# --------------------------------------------------

bronze_count = (
    bronze_df.count()
)

print()
print(
    f"Bronze records: "
    f"{bronze_count:,}"
)


# --------------------------------------------------
# 8. Select fields required for Silver
# --------------------------------------------------

silver_df = (
    bronze_df

    .select(
        "unique_key",

        "created_date",
        "closed_date",
        "resolution_action_updated_date",

        "agency",
        "agency_name",

        "complaint_type",
        "descriptor",
        "descriptor_2",

        "status",

        "incident_address",
        "street_name",
        "incident_zip",
        "borough",
        "city",

        "bbl",

        "latitude",
        "longitude",

        "community_board",
        "council_district",

        "location_type",
        "open_data_channel_type"
    )


    # --------------------------------------------------
    # 9. Convert date fields
    # --------------------------------------------------

    .withColumn(
        "created_date",
        F.to_timestamp(
            "created_date",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "closed_date",
        F.to_timestamp(
            "closed_date",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "resolution_action_updated_date",
        F.to_timestamp(
            "resolution_action_updated_date",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )


    # --------------------------------------------------
    # 10. Convert coordinates
    # --------------------------------------------------

    .withColumn(
        "latitude",
        F.col(
            "latitude"
        ).cast(
            "double"
        )
    )

    .withColumn(
        "longitude",
        F.col(
            "longitude"
        ).cast(
            "double"
        )
    )


    # --------------------------------------------------
    # 11. Normalize text
    # --------------------------------------------------

    .withColumn(
        "agency",
        F.upper(
            F.trim(
                F.col("agency")
            )
        )
    )

    .withColumn(
        "complaint_type",
        F.regexp_replace(
            F.upper(
                F.trim(
                    F.col("complaint_type")
                )
            ),
            r"\s+",
            " "
        )
    )

    .withColumn(
        "status",
        F.upper(
            F.trim(
                F.col("status")
            )
        )
    )

    .withColumn(
        "borough",
        F.upper(
            F.trim(
                F.col("borough")
            )
        )
    )

    .withColumn(
        "incident_address",
        F.upper(
            F.trim(
                F.col("incident_address")
            )
        )
    )


    # --------------------------------------------------
    # 12. Validate / normalize BBL
    #
    # Valid NYC BBL should contain 10 digits.
    # Invalid values become NULL rather than
    # silently becoming a wrong building key.
    # --------------------------------------------------

    .withColumn(
        "bbl",
        F.trim(
            F.col("bbl")
        )
    )

    .withColumn(
        "bbl",
        F.when(
            F.col(
                "bbl"
            ).rlike(
                "^[0-9]{10}$"
            ),
            F.col(
                "bbl"
            )
        ).otherwise(
            F.lit(None)
        )
    )


    # --------------------------------------------------
    # 13. Create partition / analysis fields
    # --------------------------------------------------

    .withColumn(
        "created_year",
        F.year(
            "created_date"
        )
    )

    .withColumn(
        "created_month",
        F.month(
            "created_date"
        )
    )

    .withColumn(
        "created_day",
        F.dayofmonth(
            "created_date"
        )
    )


    # --------------------------------------------------
    # 14. Remove duplicate service requests
    # --------------------------------------------------

    .dropDuplicates(
        [
            "unique_key"
        ]
    )
)


# --------------------------------------------------
# 15. Persist Silver during validation + write
# --------------------------------------------------

silver_df = (
    silver_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )
)


# --------------------------------------------------
# 16. Data Quality checks
# --------------------------------------------------

dq = (
    silver_df
    .agg(

        F.count(
            "*"
        ).alias(
            "silver_rows"
        ),

        F.sum(
            F.when(
                F.col(
                    "unique_key"
                ).isNull(),
                1
            ).otherwise(
                0
            )
        ).alias(
            "missing_unique_key"
        ),

        F.sum(
            F.when(
                F.col(
                    "created_date"
                ).isNull(),
                1
            ).otherwise(
                0
            )
        ).alias(
            "missing_created_date"
        ),

        F.sum(
            F.when(
                F.col(
                    "bbl"
                ).isNull(),
                1
            ).otherwise(
                0
            )
        ).alias(
            "missing_bbl"
        ),

        F.sum(
            F.when(
                F.col(
                    "latitude"
                ).isNull()
                |
                F.col(
                    "longitude"
                ).isNull(),
                1
            ).otherwise(
                0
            )
        ).alias(
            "missing_coordinates"
        )
    )
    .first()
)


silver_count = (
    dq["silver_rows"]
)

duplicate_count = (
    bronze_count
    - silver_count
)


print()
print("----------------------------------------")
print("DATA QUALITY")
print("----------------------------------------")

print(
    f"Silver records: "
    f"{silver_count:,}"
)

print(
    f"Duplicates removed: "
    f"{duplicate_count:,}"
)

print(
    f"Missing unique_key: "
    f"{dq['missing_unique_key']:,}"
)

print(
    f"Missing created_date: "
    f"{dq['missing_created_date']:,}"
)

print(
    f"Missing BBL: "
    f"{dq['missing_bbl']:,}"
)

print(
    f"Missing coordinates: "
    f"{dq['missing_coordinates']:,}"
)


# --------------------------------------------------
# 17. Safety checks
# --------------------------------------------------

if silver_count == 0:

    raise RuntimeError(
        "Silver DataFrame is empty. "
        "Stopping before write."
    )


if dq["missing_unique_key"] > 0:

    raise RuntimeError(
        "Silver contains records without unique_key. "
        "Stopping before write."
    )


if dq["missing_created_date"] > 0:

    raise RuntimeError(
        "Silver contains records without created_date. "
        "Stopping before write."
    )


# --------------------------------------------------
# 18. Write Silver as Parquet
#
# Partition by year/month.
# We intentionally do NOT partition by day
# to avoid creating too many small files.
# --------------------------------------------------

print()
print("----------------------------------------")
print("WRITING SILVER PARQUET")
print("----------------------------------------")


(
    silver_df
    .write
    .mode(
        "overwrite"
    )
    .partitionBy(
        "created_year",
        "created_month"
    )
    .parquet(
        SILVER_PATH
    )
)


# --------------------------------------------------
# 19. Cleanup
# --------------------------------------------------

silver_df.unpersist()


# --------------------------------------------------
# 20. Final result
# --------------------------------------------------

print()
print("========================================")
print("NYC 311 SILVER COMPLETED")
print("========================================")

print(
    f"Bronze records: "
    f"{bronze_count:,}"
)

print(
    f"Silver records: "
    f"{silver_count:,}"
)

print(
    f"Output: "
    f"{SILVER_PATH}"
)

print("========================================")


spark.stop()