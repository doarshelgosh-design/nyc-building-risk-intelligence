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
    "NYC Building Risk - Silver HPD"
)


# ==================================================
# 4. BRONZE INPUT PATH
# ==================================================

BRONZE_PATH = minio_path(
    "bronze/hpd_violations/"
    "year=*/"
    "month=*/"
    "day=*/"
    "backfill/"
    "page_*.json"
)


# ==================================================
# 5. SILVER OUTPUT PATH
# ==================================================

SILVER_PATH = minio_path(
    "silver/hpd"
)


print()
print("========================================")
print("HPD BRONZE -> SILVER")
print("========================================")

print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")


# ==================================================
# 6. READ BRONZE JSON
# ==================================================

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


# ==================================================
# 7. COUNT BRONZE RECORDS
# ==================================================

bronze_count = bronze_df.count()

print()
print(
    f"Bronze records: {bronze_count:,}"
)


# ==================================================
# 8. SELECT FIELDS REQUIRED FOR SILVER
# ==================================================

silver_df = (
    bronze_df

    .select(

        # ------------------------------------------
        # Violation identifiers
        # ------------------------------------------

        "violationid",
        "novid",
        "buildingid",
        "registrationid",

        # ------------------------------------------
        # Building identifiers
        # ------------------------------------------

        "bbl",
        "bin",
        "boroid",
        "boro",
        "block",
        "lot",

        # ------------------------------------------
        # Address
        # ------------------------------------------

        "housenumber",
        "lowhousenumber",
        "highhousenumber",
        "streetname",
        "apartment",
        "story",
        "zip",

        # ------------------------------------------
        # Violation information
        # ------------------------------------------

        "class",
        "violationstatus",
        "currentstatus",
        "currentstatusid",

        # ------------------------------------------
        # Dates
        # ------------------------------------------

        "inspectiondate",
        "novissueddate",
        "currentstatusdate",
        "approveddate",
        "certifieddate",
        "originalcorrectbydate",
        "originalcertifybydate",

        # ------------------------------------------
        # NOV information
        # ------------------------------------------

        "novtype",
        "novdescription",
        "rentimpairing",

        # ------------------------------------------
        # Geographic information
        # ------------------------------------------

        "latitude",
        "longitude",

        "communityboard",
        "councildistrict",
        "censustract",
        "nta"
    )
)


# ==================================================
# 9. CLEAN BASIC IDENTIFIERS
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "violationid",
        F.trim(
            F.col("violationid")
        )
    )

    .withColumn(
        "buildingid",
        F.trim(
            F.col("buildingid")
        )
    )

    .withColumn(
        "bin",
        F.trim(
            F.col("bin")
        )
    )

    .withColumn(
        "boroid",
        F.trim(
            F.col("boroid")
        )
    )

    .withColumn(
        "block",
        F.trim(
            F.col("block")
        )
    )

    .withColumn(
        "lot",
        F.trim(
            F.col("lot")
        )
    )
)


# ==================================================
# 10. PRESERVE ORIGINAL HPD BBL
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "source_bbl",
        F.trim(
            F.col("bbl")
        )
    )
)


# ==================================================
# 11. CONVERT DATE FIELDS
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "inspectiondate",
        F.to_timestamp(
            "inspectiondate",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "novissueddate",
        F.to_timestamp(
            "novissueddate",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "currentstatusdate",
        F.to_timestamp(
            "currentstatusdate",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "approveddate",
        F.to_timestamp(
            "approveddate",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "certifieddate",
        F.to_timestamp(
            "certifieddate",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "originalcorrectbydate",
        F.to_timestamp(
            "originalcorrectbydate",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )

    .withColumn(
        "originalcertifybydate",
        F.to_timestamp(
            "originalcertifybydate",
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )
)


# ==================================================
# 12. CONVERT COORDINATES
# ==================================================

silver_df = (
    silver_df

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
)


# ==================================================
# 13. NORMALIZE TEXT
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "boro",
        F.upper(
            F.trim(
                F.col("boro")
            )
        )
    )

    .withColumn(
        "class",
        F.upper(
            F.trim(
                F.col("class")
            )
        )
    )

    .withColumn(
        "violationstatus",
        F.upper(
            F.trim(
                F.col("violationstatus")
            )
        )
    )

    .withColumn(
        "currentstatus",
        F.upper(
            F.trim(
                F.col("currentstatus")
            )
        )
    )

    .withColumn(
        "streetname",
        F.upper(
            F.trim(
                F.col("streetname")
            )
        )
    )

    .withColumn(
        "novtype",
        F.upper(
            F.trim(
                F.col("novtype")
            )
        )
    )

    .withColumn(
        "rentimpairing",
        F.upper(
            F.trim(
                F.col("rentimpairing")
            )
        )
    )
)


# ==================================================
# 14. BUILD FALLBACK BBL
#
# NYC BBL structure:
#
# Borough = 1 digit
# Block   = 5 digits
# Lot     = 4 digits
#
# Example:
#
# Borough = 2
# Block   = 3309
# Lot     = 23
#
# 2 + 03309 + 0023
#
# Result:
# 2033090023
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "constructed_bbl",

        F.when(

            F.col(
                "boroid"
            ).rlike(
                "^[1-5]$"
            )

            &

            F.col(
                "block"
            ).rlike(
                "^[0-9]{1,5}$"
            )

            &

            F.col(
                "lot"
            ).rlike(
                "^[0-9]{1,4}$"
            ),

            F.concat(

                F.col(
                    "boroid"
                ),

                F.lpad(
                    F.col(
                        "block"
                    ),
                    5,
                    "0"
                ),

                F.lpad(
                    F.col(
                        "lot"
                    ),
                    4,
                    "0"
                )
            )
        )
    )
)


# ==================================================
# 15. CREATE FINAL NORMALIZED BBL
#
# Priority:
#
# 1. Original valid BBL from HPD
# 2. Constructed BBL
# 3. NULL
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "bbl",

        F.when(

            F.col(
                "source_bbl"
            ).rlike(
                "^[0-9]{10}$"
            ),

            F.col(
                "source_bbl"
            )
        )

        .when(

            F.col(
                "constructed_bbl"
            ).rlike(
                "^[0-9]{10}$"
            ),

            F.col(
                "constructed_bbl"
            )
        )

        .otherwise(
            F.lit(None)
        )
    )
)


# ==================================================
# 16. BBL SOURCE
#
# SOURCE
#     BBL came directly from HPD
#
# CONSTRUCTED
#     BBL was reconstructed using
#     borough + block + lot
#
# MISSING
#     No valid BBL could be created
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "bbl_source",

        F.when(

            F.col(
                "source_bbl"
            ).rlike(
                "^[0-9]{10}$"
            ),

            F.lit(
                "SOURCE"
            )
        )

        .when(

            F.col(
                "constructed_bbl"
            ).rlike(
                "^[0-9]{10}$"
            ),

            F.lit(
                "CONSTRUCTED"
            )
        )

        .otherwise(
            F.lit(
                "MISSING"
            )
        )
    )
)


# ==================================================
# 17. CREATE DATE PARTITION FIELDS
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "inspection_year",
        F.year(
            "inspectiondate"
        )
    )

    .withColumn(
        "inspection_month",
        F.month(
            "inspectiondate"
        )
    )

    .withColumn(
        "inspection_day",
        F.dayofmonth(
            "inspectiondate"
        )
    )
)


# ==================================================
# 18. REMOVE DUPLICATE VIOLATIONS
# ==================================================

silver_df = (
    silver_df

    .dropDuplicates(
        [
            "violationid"
        ]
    )
)


# ==================================================
# 19. REMOVE TEMPORARY FIELD
# ==================================================

silver_df = (
    silver_df

    .drop(
        "constructed_bbl"
    )
)


# ==================================================
# IMPORTANT
#
# We intentionally DO NOT use:
#
# persist()
# cache()
#
# HPD contains wide text columns such as
# novdescription.
#
# Caching the complete DataFrame caused:
#
# java.lang.OutOfMemoryError: Java heap space
#
# Spark will recompute the transformation when
# necessary instead of keeping the complete
# DataFrame in JVM memory.
# ==================================================


# ==================================================
# 20. DATA QUALITY
# ==================================================

dq = (
    silver_df

    .agg(

        F.count(
            "*"
        ).alias(
            "silver_rows"
        ),


        # ------------------------------------------
        # Missing violation ID
        # ------------------------------------------

        F.sum(

            F.when(

                F.col(
                    "violationid"
                ).isNull()

                |

                (
                    F.trim(
                        F.col(
                            "violationid"
                        )
                    )
                    == ""
                ),

                1
            )

            .otherwise(
                0
            )

        ).alias(
            "missing_violationid"
        ),


        # ------------------------------------------
        # Missing inspection date
        # ------------------------------------------

        F.sum(

            F.when(

                F.col(
                    "inspectiondate"
                ).isNull(),

                1
            )

            .otherwise(
                0
            )

        ).alias(
            "missing_inspectiondate"
        ),


        # ------------------------------------------
        # Missing BBL
        # ------------------------------------------

        F.sum(

            F.when(

                F.col(
                    "bbl"
                ).isNull(),

                1
            )

            .otherwise(
                0
            )

        ).alias(
            "missing_bbl"
        ),


        # ------------------------------------------
        # Missing BIN
        # ------------------------------------------

        F.sum(

            F.when(

                F.col(
                    "bin"
                ).isNull()

                |

                (
                    F.trim(
                        F.col(
                            "bin"
                        )
                    )
                    == ""
                ),

                1
            )

            .otherwise(
                0
            )

        ).alias(
            "missing_bin"
        ),


        # ------------------------------------------
        # Missing coordinates
        # ------------------------------------------

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
            )

            .otherwise(
                0
            )

        ).alias(
            "missing_coordinates"
        ),


        # ------------------------------------------
        # Original BBL from HPD
        # ------------------------------------------

        F.sum(

            F.when(

                F.col(
                    "bbl_source"
                )
                ==
                "SOURCE",

                1
            )

            .otherwise(
                0
            )

        ).alias(
            "source_bbl"
        ),


        # ------------------------------------------
        # Reconstructed BBL
        # ------------------------------------------

        F.sum(

            F.when(

                F.col(
                    "bbl_source"
                )
                ==
                "CONSTRUCTED",

                1
            )

            .otherwise(
                0
            )

        ).alias(
            "constructed_bbl"
        ),


        # ------------------------------------------
        # BBL could not be resolved
        # ------------------------------------------

        F.sum(

            F.when(

                F.col(
                    "bbl_source"
                )
                ==
                "MISSING",

                1
            )

            .otherwise(
                0
            )

        ).alias(
            "unresolved_bbl"
        )
    )

    .first()
)


# ==================================================
# 21. CALCULATE COUNTS
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
    f"Missing violationid: "
    f"{dq['missing_violationid']:,}"
)

print(
    f"Missing inspectiondate: "
    f"{dq['missing_inspectiondate']:,}"
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
        "Silver DataFrame is empty. "
        "Stopping before write."
    )


if dq["missing_violationid"] > 0:

    raise RuntimeError(
        "HPD Silver contains records without "
        "violationid. Stopping before write."
    )


if dq["missing_inspectiondate"] > 0:

    raise RuntimeError(
        "HPD Silver contains records without "
        "inspectiondate. Stopping before write."
    )


# ==================================================
# 24. WRITE SILVER PARQUET
#
# Partition:
#
# inspection_year
# inspection_month
#
# We intentionally do NOT partition by day
# to avoid creating too many small files.
# ==================================================

print()
print("----------------------------------------")
print("WRITING HPD SILVER PARQUET")
print("----------------------------------------")


(
    silver_df

    .write

    .mode(
        "overwrite"
    )

    .partitionBy(
        "inspection_year",
        "inspection_month"
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
print("HPD SILVER COMPLETED")
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