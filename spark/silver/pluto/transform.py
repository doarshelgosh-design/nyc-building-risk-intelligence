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
# 3. SNAPSHOT VERSION
# ==================================================

PLUTO_VERSION = "26v2"


# ==================================================
# 4. CREATE SPARK SESSION
# ==================================================

spark = create_spark_session(
    "NYC Building Risk - Silver PLUTO"
)


# ==================================================
# 5. BRONZE INPUT
# ==================================================

BRONZE_PATH = minio_path(
    f"bronze/pluto/"
    f"snapshots/"
    f"version={PLUTO_VERSION}/"
    f"page_*.json"
)


# ==================================================
# 6. SILVER OUTPUT
#
# PLUTO is a snapshot dataset.
# We preserve each version separately.
# ==================================================

SILVER_PATH = minio_path(
    f"silver/pluto/"
    f"version={PLUTO_VERSION}"
)


print()
print("========================================")
print("PLUTO BRONZE -> SILVER")
print("========================================")

print(f"Snapshot version: {PLUTO_VERSION}")
print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")


# ==================================================
# 7. READ BRONZE
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


bronze_count = bronze_df.count()

print()
print(
    f"Bronze records: "
    f"{bronze_count:,}"
)


# ==================================================
# 8. SELECT FIELDS REQUIRED FOR SILVER
# ==================================================

silver_df = (
    bronze_df

    .select(

        # ------------------------------------------
        # Building / lot identifiers
        # ------------------------------------------

        "bbl",
        "appbbl",
        "borocode",
        "borough",
        "block",
        "lot",
        "address",

        # ------------------------------------------
        # Building classification
        # ------------------------------------------

        "landuse",
        "bldgclass",

        # ------------------------------------------
        # Building characteristics
        # ------------------------------------------

        "yearbuilt",
        "yearalter1",
        "yearalter2",
        "numbldgs",
        "numfloors",
        "unitsres",
        "unitstotal",

        # ------------------------------------------
        # Areas
        # ------------------------------------------

        "lotarea",
        "bldgarea",
        "resarea",
        "comarea",
        "officearea",
        "retailarea",
        "garagearea",
        "strgearea",
        "factryarea",
        "otherarea",

        # ------------------------------------------
        # Dimensions
        # ------------------------------------------

        "lotfront",
        "lotdepth",
        "bldgfront",
        "bldgdepth",

        # ------------------------------------------
        # FAR
        # ------------------------------------------

        "builtfar",
        "residfar",
        "commfar",
        "facilfar",

        # ------------------------------------------
        # Geography
        # ------------------------------------------

        "latitude",
        "longitude",
        "xcoord",
        "ycoord",
        "zipcode",
        "cd",
        "council",

        # ------------------------------------------
        # Zoning
        # ------------------------------------------

        "zonedist1",
        "zonedist2",
        "zonedist3",
        "overlay1",
        "overlay2",
        "spdist1",
        "zonemap",
        "splitzone",

        # ------------------------------------------
        # Property information
        # ------------------------------------------

        "ownername",
        "ownertype",
        "landmark",
        "histdist",

        # ------------------------------------------
        # PLUTO metadata
        # ------------------------------------------

        "version",
        "plutomapid"
    )
)


# ==================================================
# 9. PRESERVE RAW BBL
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
# 10. NORMALIZE SOURCE BBL
#
# Example:
#
# 1000010010.00000000
#
# becomes:
#
# 1000010010
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "normalized_source_bbl",

        F.regexp_replace(
            F.col("source_bbl"),
            r"\.0+$",
            ""
        )
    )
)


# ==================================================
# 11. CLEAN BLOCK / LOT / BOROUGH CODE
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "borocode",
        F.trim(
            F.col("borocode")
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
# 12. FALLBACK BBL
#
# BBL:
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

            F.col(
                "borocode"
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
                    "borocode"
                ),

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
# 13. FINAL BBL
#
# Priority:
#
# 1. Normalized PLUTO source BBL
# 2. Constructed BBL
# 3. NULL
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "bbl",

        F.when(
            F.col(
                "normalized_source_bbl"
            ).rlike(
                "^[0-9]{10}$"
            ),

            F.col(
                "normalized_source_bbl"
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
# 14. BBL SOURCE
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "bbl_source",

        F.when(
            F.col(
                "normalized_source_bbl"
            ).rlike(
                "^[0-9]{10}$"
            ),

            F.lit(
                "SOURCE_NORMALIZED"
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
# 15. NORMALIZE APPBBL
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "appbbl",

        F.regexp_replace(
            F.trim(
                F.col("appbbl")
            ),
            r"\.0+$",
            ""
        )
    )

    .withColumn(
        "appbbl",

        F.when(
            F.col(
                "appbbl"
            ).rlike(
                "^[0-9]{10}$"
            ),

            F.col(
                "appbbl"
            )
        )

        .otherwise(
            F.lit(None)
        )
    )
)


# ==================================================
# 16. NORMALIZE BOROUGH
#
# PLUTO source uses:
#
# MN
# BX
# BK
# QN
# SI
#
# Silver uses same full borough names
# as our other datasets.
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "borough",

        F.when(
            F.col("borocode") == "1",
            F.lit("MANHATTAN")
        )

        .when(
            F.col("borocode") == "2",
            F.lit("BRONX")
        )

        .when(
            F.col("borocode") == "3",
            F.lit("BROOKLYN")
        )

        .when(
            F.col("borocode") == "4",
            F.lit("QUEENS")
        )

        .when(
            F.col("borocode") == "5",
            F.lit("STATEN ISLAND")
        )

        .otherwise(
            F.upper(
                F.trim(
                    F.col("borough")
                )
            )
        )
    )
)


# ==================================================
# 17. NORMALIZE TEXT FIELDS
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "address",
        F.upper(
            F.trim(
                F.col("address")
            )
        )
    )

    .withColumn(
        "landuse",
        F.trim(
            F.col("landuse")
        )
    )

    .withColumn(
        "bldgclass",
        F.upper(
            F.trim(
                F.col("bldgclass")
            )
        )
    )

    .withColumn(
        "zonedist1",
        F.upper(
            F.trim(
                F.col("zonedist1")
            )
        )
    )

    .withColumn(
        "ownername",
        F.upper(
            F.trim(
                F.col("ownername")
            )
        )
    )
)


# ==================================================
# 18. INTEGER FIELDS
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "unitsres",
        F.col(
            "unitsres"
        ).cast(
            "integer"
        )
    )

    .withColumn(
        "unitstotal",
        F.col(
            "unitstotal"
        ).cast(
            "integer"
        )
    )

    .withColumn(
        "numbldgs",
        F.col(
            "numbldgs"
        ).cast(
            "integer"
        )
    )
)


# ==================================================
# 19. YEAR BUILT
#
# PLUTO uses 0 for unknown year.
#
# In Silver:
#
# 0 -> NULL
# ==================================================

silver_df = (
    silver_df

    .withColumn(
        "yearbuilt",

        F.when(
            F.col(
                "yearbuilt"
            ).cast(
                "integer"
            ) > 0,

            F.col(
                "yearbuilt"
            ).cast(
                "integer"
            )
        )

        .otherwise(
            F.lit(None)
        )
    )

    .withColumn(
        "yearalter1",

        F.when(
            F.col(
                "yearalter1"
            ).cast(
                "integer"
            ) > 0,

            F.col(
                "yearalter1"
            ).cast(
                "integer"
            )
        )

        .otherwise(
            F.lit(None)
        )
    )

    .withColumn(
        "yearalter2",

        F.when(
            F.col(
                "yearalter2"
            ).cast(
                "integer"
            ) > 0,

            F.col(
                "yearalter2"
            ).cast(
                "integer"
            )
        )

        .otherwise(
            F.lit(None)
        )
    )
)


# ==================================================
# 20. DOUBLE FIELDS
# ==================================================

double_fields = [
    "numfloors",

    "lotarea",
    "bldgarea",
    "resarea",
    "comarea",
    "officearea",
    "retailarea",
    "garagearea",
    "strgearea",
    "factryarea",
    "otherarea",

    "lotfront",
    "lotdepth",
    "bldgfront",
    "bldgdepth",

    "builtfar",
    "residfar",
    "commfar",
    "facilfar",

    "latitude",
    "longitude",

    "xcoord",
    "ycoord"
]


for field_name in double_fields:

    silver_df = (
        silver_df

        .withColumn(
            field_name,

            F.col(
                field_name
            ).cast(
                "double"
            )
        )
    )


# ==================================================
# 21. SNAPSHOT VERSION
# ==================================================

silver_df = (
    silver_df

    .withColumnRenamed(
        "version",
        "snapshot_version"
    )
)


# ==================================================
# 22. REMOVE TEMPORARY FIELDS
# ==================================================

silver_df = (
    silver_df

    .drop(
        "normalized_source_bbl",
        "constructed_bbl"
    )
)


# ==================================================
# 23. DATA QUALITY
#
# PLUTO is our building / tax-lot reference.
#
# Therefore BBL uniqueness is important.
# We DO NOT silently drop duplicate BBLs.
# ==================================================

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
                    "bbl"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias(
            "missing_bbl"
        ),

        F.countDistinct(
            "bbl"
        ).alias(
            "distinct_bbl"
        ),

        F.sum(
            F.when(
                F.col(
                    "bbl_source"
                )
                ==
                "SOURCE_NORMALIZED",
                1
            ).otherwise(0)
        ).alias(
            "source_bbl"
        ),

        F.sum(
            F.when(
                F.col(
                    "bbl_source"
                )
                ==
                "CONSTRUCTED",
                1
            ).otherwise(0)
        ).alias(
            "constructed_bbl"
        ),

        F.sum(
            F.when(
                F.col(
                    "bbl_source"
                )
                ==
                "MISSING",
                1
            ).otherwise(0)
        ).alias(
            "unresolved_bbl"
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
            ).otherwise(0)
        ).alias(
            "missing_coordinates"
        ),

        F.sum(
            F.when(
                F.col(
                    "yearbuilt"
                ).isNull(),
                1
            ).otherwise(0)
        ).alias(
            "missing_yearbuilt"
        )
    )

    .first()
)


silver_count = dq["silver_rows"]

nonnull_bbl_count = (
    silver_count
    - dq["missing_bbl"]
)

duplicate_bbl = (
    nonnull_bbl_count
    - dq["distinct_bbl"]
)


# ==================================================
# 24. PRINT DATA QUALITY
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
    f"Missing BBL: "
    f"{dq['missing_bbl']:,}"
)

print(
    f"Distinct BBL: "
    f"{dq['distinct_bbl']:,}"
)

print(
    f"Duplicate BBL: "
    f"{duplicate_bbl:,}"
)

print(
    f"BBL normalized from source: "
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

print(
    f"Missing coordinates: "
    f"{dq['missing_coordinates']:,}"
)

print(
    f"Missing / unknown yearbuilt: "
    f"{dq['missing_yearbuilt']:,}"
)


# ==================================================
# 25. SAFETY CHECKS
# ==================================================

if silver_count == 0:

    raise RuntimeError(
        "PLUTO Silver DataFrame is empty. "
        "Stopping before write."
    )


if dq["missing_bbl"] > 0:

    raise RuntimeError(
        "PLUTO Silver contains records without "
        "a valid BBL. Stopping before write."
    )


if duplicate_bbl > 0:

    raise RuntimeError(
        f"PLUTO Silver contains "
        f"{duplicate_bbl:,} duplicate BBL records. "
        f"Stopping before write."
    )


# ==================================================
# 26. WRITE SILVER PARQUET
#
# PLUTO is versioned by snapshot.
#
# Example:
#
# silver/pluto/version=26v2/
# ==================================================

print()
print("----------------------------------------")
print("WRITING PLUTO SILVER PARQUET")
print("----------------------------------------")


(
    silver_df

    .write

    .mode(
        "overwrite"
    )

    .parquet(
        SILVER_PATH
    )
)


# ==================================================
# 27. FINAL RESULT
# ==================================================

print()
print("========================================")
print("PLUTO SILVER COMPLETED")
print("========================================")

print(
    f"Snapshot version: "
    f"{PLUTO_VERSION}"
)

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
# 28. STOP SPARK
# ==================================================

spark.stop()