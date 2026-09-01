import sys

from pyspark.sql import functions as F


# ==================================================
# 1. PROJECT PATHS
# ==================================================

PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)


# ==================================================
# 2. PROJECT IMPORTS
# ==================================================

from spark_session import create_spark_session
from minio_config import minio_path


# ==================================================
# 3. CREATE SPARK SESSION
# ==================================================

spark = create_spark_session(
    "NYC Building Risk - Silver DOB"
)


# ==================================================
# 4. BRONZE INPUT
# ==================================================

BRONZE_PATH = minio_path(
    "bronze/dob_violations/"
    "year=*/"
    "month=*/"
    "day=*/"
    "backfill/"
    "page_*.json"
)


# ==================================================
# 5. SILVER OUTPUT
# ==================================================

SILVER_PATH = minio_path(
    "silver/dob"
)


print()
print("========================================")
print("DOB BRONZE -> SILVER")
print("========================================")

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")


# ==================================================
# 6. READ BRONZE
# ==================================================

bronze_df = (
    spark.read
    .option("multiline", "true")
    .json(BRONZE_PATH)
)

bronze_count = bronze_df.count()

print()
print(f"Bronze records: {bronze_count:,}")


# ==================================================
# 7. SELECT SILVER FIELDS
# ==================================================

silver_df = (
    bronze_df

    .select(
        "violation_number",
        "violation_issue_date",
        "violation_status",
        "violation_type",
        "device_type",

        "bbl",
        "bin",
        "borough",
        "block",
        "lot",

        "house_number",
        "street",
        "city",
        "state",
        "zip",

        "latitude",
        "longitude",

        "community_board",
        "council_district",
        "census_tract_2020_",
        "neighborhood_tabulation_area_nta_2020_"
    )
)


# ==================================================
# 8. CLEAN IDENTIFIERS
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "violation_number",
        F.trim(F.col("violation_number"))
    )

    .withColumn(
        "bin",
        F.trim(F.col("bin"))
    )

    .withColumn(
        "block",
        F.trim(F.col("block"))
    )

    .withColumn(
        "lot",
        F.trim(F.col("lot"))
    )
)


# ==================================================
# 9. PRESERVE ORIGINAL BBL
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "source_bbl",
        F.trim(F.col("bbl"))
    )
)


# ==================================================
# 10. CONVERT DATE
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "violation_issue_date",

        F.to_timestamp(
            "violation_issue_date",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )
)


# ==================================================
# 11. CONVERT COORDINATES
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "latitude",
        F.col("latitude").cast("double")
    )

    .withColumn(
        "longitude",
        F.col("longitude").cast("double")
    )
)


# ==================================================
# 12. NORMALIZE TEXT
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "borough",
        F.upper(F.trim(F.col("borough")))
    )

    .withColumn(
        "violation_status",
        F.upper(F.trim(F.col("violation_status")))
    )

    .withColumn(
        "violation_type",
        F.upper(F.trim(F.col("violation_type")))
    )

    .withColumn(
        "device_type",
        F.upper(F.trim(F.col("device_type")))
    )

    .withColumn(
        "street",
        F.upper(F.trim(F.col("street")))
    )

    .withColumn(
        "city",
        F.upper(F.trim(F.col("city")))
    )

    .withColumn(
        "state",
        F.upper(F.trim(F.col("state")))
    )
)


# ==================================================
# 13. BOROUGH CODE
#
# Manhattan      = 1
# Bronx          = 2
# Brooklyn       = 3
# Queens         = 4
# Staten Island  = 5
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "borough_code",

        F.when(
            F.col("borough") == "MANHATTAN",
            F.lit("1")
        )

        .when(
            F.col("borough") == "BRONX",
            F.lit("2")
        )

        .when(
            F.col("borough") == "BROOKLYN",
            F.lit("3")
        )

        .when(
            F.col("borough") == "QUEENS",
            F.lit("4")
        )

        .when(
            F.col("borough") == "STATEN ISLAND",
            F.lit("5")
        )

        .otherwise(
            F.lit(None)
        )
    )
)


# ==================================================
# 14. CONSTRUCT FALLBACK BBL
#
# Borough = 1 digit
# Block   = 5 digits
# Lot     = 4 digits
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "constructed_bbl",

        F.when(

            F.col("borough_code").isNotNull()

            &

            F.col("block").rlike(
                "^[0-9]{1,5}$"
            )

            &

            F.col("lot").rlike(
                "^[0-9]{1,4}$"
            ),

            F.concat(
                F.col("borough_code"),
                F.lpad(
                    F.col("block"),
                    5,
                    "0"
                ),
                F.lpad(
                    F.col("lot"),
                    4,
                    "0"
                )
            )
        )
    )
)


# ==================================================
# 15. FINAL BBL
#
# Priority:
# 1. Source BBL
# 2. Constructed BBL
# 3. NULL
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "bbl",

        F.when(
            F.col("source_bbl").rlike(
                "^[0-9]{10}$"
            ),
            F.col("source_bbl")
        )

        .when(
            F.col("constructed_bbl").rlike(
                "^[0-9]{10}$"
            ),
            F.col("constructed_bbl")
        )

        .otherwise(
            F.lit(None)
        )
    )
)


# ==================================================
# 16. BBL SOURCE
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "bbl_source",

        F.when(
            F.col("source_bbl").rlike(
                "^[0-9]{10}$"
            ),
            F.lit("SOURCE")
        )

        .when(
            F.col("constructed_bbl").rlike(
                "^[0-9]{10}$"
            ),
            F.lit("CONSTRUCTED")
        )

        .otherwise(
            F.lit("MISSING")
        )
    )
)


# ==================================================
# 17. PARTITION FIELDS
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "violation_year",
        F.year("violation_issue_date")
    )

    .withColumn(
        "violation_month",
        F.month("violation_issue_date")
    )

    .withColumn(
        "violation_day",
        F.dayofmonth("violation_issue_date")
    )
)


# ==================================================
# 18. REMOVE DUPLICATES
# ==================================================

silver_df = (
    silver_df

    .dropDuplicates(
        ["violation_number"]
    )
)


# ==================================================
# 19. REMOVE TEMPORARY FIELDS
# ==================================================

silver_df = (
    silver_df

    .drop(
        "constructed_bbl",
        "borough_code"
    )
)


# ==================================================
# 20. DATA QUALITY
#
# No cache / persist is used.
# ==================================================

dq = (
    silver_df

    .agg(

        F.count("*").alias(
            "silver_rows"
        ),

        F.sum(
            F.when(
                F.col("violation_number").isNull()
                |
                (
                    F.trim(
                        F.col("violation_number")
                    )
                    == ""
                ),
                1
            ).otherwise(0)
        ).alias(
            "missing_violation_number"
        ),

        F.sum(
            F.when(
                F.col("violation_issue_date").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "missing_issue_date"
        ),

        F.sum(
            F.when(
                F.col("bbl").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "missing_bbl"
        ),

        F.sum(
            F.when(
                F.col("bin").isNull()
                |
                (
                    F.trim(
                        F.col("bin")
                    )
                    == ""
                ),
                1
            ).otherwise(0)
        ).alias(
            "missing_bin"
        ),

        F.sum(
            F.when(
                F.col("latitude").isNull()
                |
                F.col("longitude").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "missing_coordinates"
        ),

        F.sum(
            F.when(
                F.col("bbl_source") == "SOURCE",
                1
            ).otherwise(0)
        ).alias(
            "source_bbl"
        ),

        F.sum(
            F.when(
                F.col("bbl_source") == "CONSTRUCTED",
                1
            ).otherwise(0)
        ).alias(
            "constructed_bbl"
        ),

        F.sum(
            F.when(
                F.col("bbl_source") == "MISSING",
                1
            ).otherwise(0)
        ).alias(
            "unresolved_bbl"
        )
    )

    .first()
)


# ==================================================
# 21. COUNTS
# ==================================================

silver_count = dq["silver_rows"]

duplicate_count = (
    bronze_count
    - silver_count
)


# ==================================================
# 22. PRINT DATA QUALITY
# ==================================================

print()
print("----------------------------------------")
print("DATA QUALITY")
print("----------------------------------------")

print(
    f"Bronze records: "
    f"{bronze_count:,}"
)

print(
    f"Silver records: "
    f"{silver_count:,}"
)

print(
    f"Duplicates removed: "
    f"{duplicate_count:,}"
)

print(
    f"Missing violation_number: "
    f"{dq['missing_violation_number']:,}"
)

print(
    f"Missing violation_issue_date: "
    f"{dq['missing_issue_date']:,}"
)

print(
    f"Missing BBL: "
    f"{dq['missing_bbl']:,}"
)

print(
    f"Missing BIN: "
    f"{dq['missing_bin']:,}"
)

print(
    f"Missing coordinates: "
    f"{dq['missing_coordinates']:,}"
)

print(
    f"BBL from source: "
    f"{dq['source_bbl']:,}"
)

print(
    f"BBL constructed: "
    f"{dq['constructed_bbl']:,}"
)

print(
    f"BBL unresolved: "
    f"{dq['unresolved_bbl']:,}"
)


# ==================================================
# 23. SAFETY CHECKS
# ==================================================

if silver_count == 0:
    raise RuntimeError(
        "DOB Silver DataFrame is empty. "
        "Stopping before write."
    )


if dq["missing_violation_number"] > 0:
    raise RuntimeError(
        "DOB Silver contains records without "
        "violation_number. Stopping before write."
    )


if dq["missing_issue_date"] > 0:
    raise RuntimeError(
        "DOB Silver contains records without "
        "violation_issue_date. Stopping before write."
    )


# ==================================================
# 24. WRITE SILVER PARQUET
# ==================================================

print()
print("----------------------------------------")
print("WRITING DOB SILVER PARQUET")
print("----------------------------------------")


(
    silver_df

    .write

    .mode("overwrite")

    .partitionBy(
        "violation_year",
        "violation_month"
    )

    .parquet(
        SILVER_PATH
    )
)


# ==================================================
# 25. FINAL RESULT
# ==================================================

print()
print("========================================")
print("DOB SILVER COMPLETED")
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


# ==================================================
# 26. STOP SPARK
# ==================================================

spark.stop()