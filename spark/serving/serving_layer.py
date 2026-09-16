import sys

from pyspark.sql import functions as F


PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from spark_session import create_spark_session
from minio_config import minio_path


MODEL_VERSION = "serving_layer_v1"


# ============================================================
# DATA QUALITY HELPERS
# ============================================================

def validate_source_uniqueness(df, id_col, label):
    stats = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct(id_col).alias("distinct_ids"),
        F.sum(
            F.when(F.col(id_col).isNull(), 1).otherwise(0)
        ).alias("null_ids"),
    ).first()

    print("\n" + "-" * 60)
    print(f"{label} DATA QUALITY")
    print("-" * 60)
    print("Rows:", f"{stats['rows']:,}")
    print(f"Distinct {id_col}:", f"{stats['distinct_ids']:,}")
    print(f"Null {id_col}:", f"{stats['null_ids'] or 0:,}")

    if stats["rows"] == 0:
        raise RuntimeError(f"DQ FAILED: {label} is empty.")

    if (stats["null_ids"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains null {id_col}."
        )

    if stats["rows"] != stats["distinct_ids"]:
        raise RuntimeError(
            f"DQ FAILED: {label} contains duplicate {id_col}."
        )

    return stats["rows"]


def get_single_risk_as_of_date(df, label):
    if "risk_as_of_date" not in df.columns:
        raise RuntimeError(
            f"DQ FAILED: {label} does not contain risk_as_of_date."
        )

    dates = [
        row["risk_as_of_date"]
        for row in (
            df
            .filter(F.col("risk_as_of_date").isNotNull())
            .select("risk_as_of_date")
            .distinct()
            .collect()
        )
    ]

    if len(dates) != 1:
        raise RuntimeError(
            f"DQ FAILED: {label} must contain exactly one "
            f"risk_as_of_date, found: {dates}"
        )

    return dates[0]


def validate_serving_building(
    serving_building,
    expected_rows,
    expected_risk_as_of_date,
    label,
):
    stats = serving_building.agg(
        F.count("*").alias("rows"),
        F.countDistinct("building_id").alias("distinct_buildings"),
        F.sum(
            F.when(F.col("building_id").isNull(), 1).otherwise(0)
        ).alias("null_building_id"),
        F.sum(
            F.when(F.col("building_risk_score").isNull(), 1).otherwise(0)
        ).alias("missing_building_risk"),
        F.sum(
            F.when(F.col("property_id").isNotNull(), 1).otherwise(0)
        ).alias("with_property"),
        F.sum(
            F.when(F.col("property_risk_score").isNotNull(), 1).otherwise(0)
        ).alias("with_property_risk"),
        F.sum(
            F.when(F.col("search_address").isNull(), 1).otherwise(0)
        ).alias("missing_search_address"),
        F.sum(
            F.when(
                F.col("latitude").isNull()
                | F.col("longitude").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_coordinates"),
        F.sum(
            F.when(
                F.col("entity_type") != "BUILDING",
                1,
            ).otherwise(0)
        ).alias("invalid_entity_type"),
        F.sum(
            F.when(
                F.col("risk_as_of_date").isNotNull()
                & (
                    F.col("risk_as_of_date")
                    != F.lit(str(expected_risk_as_of_date)).cast("date")
                ),
                1,
            ).otherwise(0)
        ).alias("wrong_risk_as_of_date"),
    ).first()

    print("\n" + "-" * 60)
    print(f"{label} DATA QUALITY")
    print("-" * 60)
    print("Expected rows:", f"{expected_rows:,}")
    print("Rows:", f"{stats['rows']:,}")
    print(
        "Distinct building_id:",
        f"{stats['distinct_buildings']:,}",
    )
    print(
        "Null building_id:",
        f"{stats['null_building_id'] or 0:,}",
    )
    print(
        "Missing Building Risk:",
        f"{stats['missing_building_risk'] or 0:,}",
    )
    print(
        "With Property:",
        f"{stats['with_property'] or 0:,}",
    )
    print(
        "With Property Risk:",
        f"{stats['with_property_risk'] or 0:,}",
    )
    print(
        "Missing search_address:",
        f"{stats['missing_search_address'] or 0:,}",
    )
    print(
        "Missing coordinates:",
        f"{stats['missing_coordinates'] or 0:,}",
    )
    print(
        "Invalid entity_type:",
        f"{stats['invalid_entity_type'] or 0:,}",
    )
    print(
        "Rows with wrong Risk As-Of Date:",
        f"{stats['wrong_risk_as_of_date'] or 0:,}",
    )

    if stats["rows"] != expected_rows:
        raise RuntimeError(
            f"DQ FAILED: {label} row count does not match dim_building."
        )

    if stats["rows"] != stats["distinct_buildings"]:
        raise RuntimeError(
            f"DQ FAILED: {label} contains duplicate building_id."
        )

    if (stats["null_building_id"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains null building_id."
        )

    if (stats["missing_building_risk"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} is missing Building Risk rows."
        )

    if (stats["invalid_entity_type"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains invalid entity_type."
        )

    if (stats["wrong_risk_as_of_date"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains unexpected risk_as_of_date."
        )


def validate_serving_property(
    serving_property,
    expected_rows,
    expected_risk_as_of_date,
    label,
):
    stats = serving_property.agg(
        F.count("*").alias("rows"),
        F.countDistinct("property_id").alias("distinct_properties"),
        F.sum(
            F.when(F.col("property_id").isNull(), 1).otherwise(0)
        ).alias("null_property_id"),
        F.sum(
            F.when(F.col("property_risk_level").isNull(), 1).otherwise(0)
        ).alias("missing_property_risk_level"),
        F.sum(
            F.when(F.col("property_risk_score").isNotNull(), 1).otherwise(0)
        ).alias("with_property_risk"),
        F.sum(
            F.when(F.col("property_risk_level") == "NO_DATA", 1).otherwise(0)
        ).alias("no_data"),
        F.sum(
            F.when(F.col("building_count") > 0, 1).otherwise(0)
        ).alias("with_building"),
        F.sum(
            F.when(F.col("search_address").isNull(), 1).otherwise(0)
        ).alias("missing_search_address"),
        F.sum(
            F.when(
                F.col("latitude").isNull()
                | F.col("longitude").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_coordinates"),
        F.sum(
            F.when(
                F.col("entity_type") != "PROPERTY",
                1,
            ).otherwise(0)
        ).alias("invalid_entity_type"),
        F.sum(
            F.when(
                F.col("risk_as_of_date")
                != F.lit(str(expected_risk_as_of_date)).cast("date"),
                1,
            ).otherwise(0)
        ).alias("wrong_risk_as_of_date"),
    ).first()

    print("\n" + "-" * 60)
    print(f"{label} DATA QUALITY")
    print("-" * 60)
    print("Expected rows:", f"{expected_rows:,}")
    print("Rows:", f"{stats['rows']:,}")
    print(
        "Distinct property_id:",
        f"{stats['distinct_properties']:,}",
    )
    print(
        "Null property_id:",
        f"{stats['null_property_id'] or 0:,}",
    )
    print(
        "Missing property_risk_level:",
        f"{stats['missing_property_risk_level'] or 0:,}",
    )
    print(
        "With Property Risk:",
        f"{stats['with_property_risk'] or 0:,}",
    )
    print(
        "NO_DATA:",
        f"{stats['no_data'] or 0:,}",
    )
    print(
        "With at least one Building:",
        f"{stats['with_building'] or 0:,}",
    )
    print(
        "Missing search_address:",
        f"{stats['missing_search_address'] or 0:,}",
    )
    print(
        "Missing coordinates:",
        f"{stats['missing_coordinates'] or 0:,}",
    )
    print(
        "Invalid entity_type:",
        f"{stats['invalid_entity_type'] or 0:,}",
    )
    print(
        "Rows with wrong Risk As-Of Date:",
        f"{stats['wrong_risk_as_of_date'] or 0:,}",
    )

    if stats["rows"] != expected_rows:
        raise RuntimeError(
            f"DQ FAILED: {label} row count does not match property input."
        )

    if stats["rows"] != stats["distinct_properties"]:
        raise RuntimeError(
            f"DQ FAILED: {label} contains duplicate property_id."
        )

    if (stats["null_property_id"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains null property_id."
        )

    if (stats["missing_property_risk_level"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} is missing property_risk_level."
        )

    if (stats["invalid_entity_type"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains invalid entity_type."
        )

    if (stats["wrong_risk_as_of_date"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains unexpected risk_as_of_date."
        )


# ============================================================
# SERVING TRANSFORMATIONS
# ============================================================

def build_serving_building(dim_building, building_risk, property_risk):
    building_risk_serving = (
        building_risk
        .select(
            "building_id",
            "building_risk_score",
            F.col("risk_level").alias("building_risk_level"),
            "score_311",
            "score_hpd_severity",
            "score_hpd_recency",
            "score_dob_active",
            "score_dob_recency",
            "score_building_age_final",
            "complaints_0_30",
            "complaints_31_90",
            "complaints_91_365",
            "hpd_open_class_a",
            "hpd_open_class_b",
            "hpd_open_class_c",
            "hpd_open_class_i",
            "hpd_active_violations",
            "dob_active_violations",
            "building_age",
            "age_imputed_flag",
        )
    )

    property_risk_serving = (
        property_risk
        .select(
            "property_id",
            "bbl",
            "property_address",
            "zipcode",
            "property_risk_score",
            "property_risk_level",
            "property_risk_source",
            "building_count",
            "max_building_risk_score",
            "avg_building_risk_score",
            "property_only_risk_score",
            "yearbuilt",
            "numbldgs",
            "numfloors",
            "unitsres",
            "unitstotal",
            "landuse",
            "bldgclass",
            "risk_as_of_date",
        )
    )

    return (
        dim_building
        .join(
            building_risk_serving,
            on="building_id",
            how="left",
        )
        .join(
            property_risk_serving,
            on="property_id",
            how="left",
        )
        .select(
            # Building identity
            "building_id",
            "bin",
            "property_id",
            "current_address",
            "borough",
            "latitude",
            "longitude",
            "resolved_bbl",
            "current_bbl",
            "address_aliases",
            "bbl_aliases",

            # Building Risk
            "building_risk_score",
            "building_risk_level",
            "score_311",
            "score_hpd_severity",
            "score_hpd_recency",
            "score_dob_active",
            "score_dob_recency",
            "score_building_age_final",

            # Building features
            "complaints_0_30",
            "complaints_31_90",
            "complaints_91_365",
            "hpd_open_class_a",
            "hpd_open_class_b",
            "hpd_open_class_c",
            "hpd_open_class_i",
            "hpd_active_violations",
            "dob_active_violations",
            "building_age",
            "age_imputed_flag",

            # Property context
            "bbl",
            "property_address",
            "zipcode",
            "property_risk_score",
            "property_risk_level",
            "property_risk_source",
            "building_count",
            "max_building_risk_score",
            "avg_building_risk_score",
            "property_only_risk_score",

            # PLUTO
            "yearbuilt",
            "numbldgs",
            "numfloors",
            "unitsres",
            "unitstotal",
            "landuse",
            "bldgclass",

            # Model / identity metadata
            "identity_status",
            "match_method",
            "match_confidence",
            "resolution_status",
            "risk_as_of_date",
        )
        .withColumn(
            "entity_type",
            F.lit("BUILDING"),
        )
        .withColumn(
            "search_address",
            F.upper(
                F.trim(F.col("current_address"))
            ),
        )
        .withColumn(
            "search_aliases",
            F.col("address_aliases"),
        )
        .withColumn(
            "serving_model_version",
            F.lit(MODEL_VERSION),
        )
        .withColumn(
            "serving_calculated_at",
            F.current_timestamp(),
        )
    )


def build_serving_property(property_risk):
    return (
        property_risk
        .select(
            # Property identity
            "property_id",
            "bbl",
            "property_address",
            "borough",
            "zipcode",
            "latitude",
            "longitude",

            # Property Risk
            "property_risk_score",
            "property_risk_level",
            "property_risk_source",

            # Building context
            "building_count",
            "max_building_risk_score",
            "avg_building_risk_score",
            "property_only_risk_score",

            # Risk components
            "property_score_311",
            "property_score_hpd_severity",
            "property_score_hpd_recency",

            # Raw signals
            "property_risk_311_raw",
            "property_risk_hpd_severity_raw",
            "property_risk_hpd_recency_raw",

            # Recent 311
            "property_311_0_30",
            "property_311_31_90",
            "property_311_91_365",

            # HPD
            "property_hpd_open_class_a",
            "property_hpd_open_class_b",
            "property_hpd_open_class_c",
            "property_hpd_0_30",
            "property_hpd_31_90",
            "property_hpd_91_365",

            # PLUTO
            "yearbuilt",
            "numbldgs",
            "numfloors",
            "unitsres",
            "unitstotal",
            "landuse",
            "bldgclass",

            # Model metadata
            "risk_as_of_date",
        )
        .withColumn(
            "entity_type",
            F.lit("PROPERTY"),
        )
        .withColumn(
            "search_address",
            F.upper(
                F.trim(F.col("property_address"))
            ),
        )
        .withColumn(
            "serving_model_version",
            F.lit(MODEL_VERSION),
        )
        .withColumn(
            "serving_calculated_at",
            F.current_timestamp(),
        )
    )


# ============================================================
# MAIN
# ============================================================

def main():
    spark = create_spark_session(
        "NYC Building Risk - Serving Layer Production"
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n" + "=" * 60)
        print("SERVING LAYER PRODUCTION JOB")
        print("=" * 60)
        print("Model version:", MODEL_VERSION)

        dim_building_path = minio_path(
            "gold/data_model/dim_building"
        )
        dim_property_path = minio_path(
            "gold/data_model/dim_property"
        )
        building_risk_path = minio_path(
            "gold/building_risk/building_risk_score"
        )
        property_risk_path = minio_path(
            "gold/property_risk/property_risk_score"
        )
        serving_building_path = minio_path(
            "serving/building"
        )
        serving_property_path = minio_path(
            "serving/property"
        )

        print("dim_building:", dim_building_path)
        print("dim_property:", dim_property_path)
        print("building_risk:", building_risk_path)
        print("property_risk:", property_risk_path)
        print("serving_building output:", serving_building_path)
        print("serving_property output:", serving_property_path)

        dim_building = spark.read.parquet(dim_building_path)
        dim_property = spark.read.parquet(dim_property_path)
        building_risk = spark.read.parquet(building_risk_path)
        property_risk = spark.read.parquet(property_risk_path)

        expected_building_rows = validate_source_uniqueness(
            dim_building,
            "building_id",
            "DIM_BUILDING",
        )
        expected_property_rows = validate_source_uniqueness(
            dim_property,
            "property_id",
            "DIM_PROPERTY",
        )

        validate_source_uniqueness(
            building_risk,
            "building_id",
            "BUILDING_RISK",
        )
        validate_source_uniqueness(
            property_risk,
            "property_id",
            "PROPERTY_RISK",
        )

        building_risk_date = get_single_risk_as_of_date(
            building_risk,
            "BUILDING_RISK",
        )
        property_risk_date = get_single_risk_as_of_date(
            property_risk,
            "PROPERTY_RISK",
        )

        print("\nMODEL DATES")
        print("Building Risk as-of date:", building_risk_date)
        print("Property Risk as-of date:", property_risk_date)

        if building_risk_date != property_risk_date:
            raise RuntimeError(
                "DQ FAILED: Building Risk and Property Risk "
                "have different risk_as_of_date values."
            )

        serving_building = build_serving_building(
            dim_building,
            building_risk,
            property_risk,
        )
        serving_property = build_serving_property(
            property_risk,
        )

        validate_serving_building(
            serving_building,
            expected_building_rows,
            building_risk_date,
            "SERVING BUILDING IN-MEMORY",
        )
        validate_serving_property(
            serving_property,
            expected_property_rows,
            property_risk_date,
            "SERVING PROPERTY IN-MEMORY",
        )

        (
            serving_building
            .write
            .mode("overwrite")
            .parquet(serving_building_path)
        )
        print(
            "\nSaved serving_building to:",
            serving_building_path,
        )

        (
            serving_property
            .write
            .mode("overwrite")
            .parquet(serving_property_path)
        )
        print(
            "Saved serving_property to:",
            serving_property_path,
        )

        saved_building = spark.read.parquet(
            serving_building_path
        )
        saved_property = spark.read.parquet(
            serving_property_path
        )

        validate_serving_building(
            saved_building,
            expected_building_rows,
            building_risk_date,
            "SERVING BUILDING PERSISTED",
        )
        validate_serving_property(
            saved_property,
            expected_property_rows,
            property_risk_date,
            "SERVING PROPERTY PERSISTED",
        )

        print("\n" + "=" * 60)
        print("SERVING LAYER COMPLETED")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
