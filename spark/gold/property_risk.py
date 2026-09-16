import sys

from pyspark.sql import functions as F


PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from spark_session import create_spark_session
from minio_config import minio_path


# ============================================================
# MODEL CONFIGURATION
# Preserves the final MAX Property Risk model from the
# validated notebook.
# ============================================================

MODEL_VERSION = "property_risk_v1_max"

CAP_PROPERTY_311 = 24.0
CAP_PROPERTY_HPD_SEVERITY = 170.0
CAP_PROPERTY_HPD_RECENCY = 110.0

PROPERTY_WEIGHT_311 = 25.0
PROPERTY_WEIGHT_HPD_SEVERITY = 28.0
PROPERTY_WEIGHT_HPD_RECENCY = 12.0
PROPERTY_WEIGHT_HPD_TOTAL = (
    PROPERTY_WEIGHT_HPD_SEVERITY + PROPERTY_WEIGHT_HPD_RECENCY
)

PROPERTY_P75 = 24.80
PROPERTY_P95 = 59.87
PROPERTY_P99 = 79.63

ALLOWED_RISK_LEVELS = [
    "NO_DATA",
    "LOW",
    "MEDIUM",
    "HIGH",
    "CRITICAL",
]

ALLOWED_RISK_SOURCES = [
    "BUILDING_PLUS_PROPERTY_EVENTS",
    "BUILDING_ONLY",
    "PROPERTY_EVENTS_ONLY",
    "NO_RISK_DATA",
]


# ============================================================
# HELPERS
# ============================================================


def log_normalize(column_name, cap_value):
    return (
        F.log1p(
            F.least(
                F.col(column_name),
                F.lit(cap_value),
            )
        )
        / F.log1p(F.lit(cap_value))
        * 100
    )


def get_building_risk_as_of_date(building_risk_df):
    if "risk_as_of_date" not in building_risk_df.columns:
        raise RuntimeError(
            "DQ FAILED: Building Risk output does not contain risk_as_of_date."
        )

    dates = [
        row["risk_as_of_date"]
        for row in (
            building_risk_df
            .filter(F.col("risk_as_of_date").isNotNull())
            .select("risk_as_of_date")
            .distinct()
            .collect()
        )
    ]

    if len(dates) != 1:
        raise RuntimeError(
            "DQ FAILED: Expected exactly one risk_as_of_date in Building Risk, "
            f"found: {dates}"
        )

    return dates[0]


def validate_dim_property(dim_property_df):
    stats = dim_property_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("property_id").alias("distinct_properties"),
        F.sum(
            F.when(F.col("property_id").isNull(), 1).otherwise(0)
        ).alias("null_property_id"),
    ).first()

    print("\n" + "-" * 60)
    print("DIM_PROPERTY DATA QUALITY")
    print("-" * 60)
    print("Rows:", f"{stats['rows']:,}")
    print("Distinct property_id:", f"{stats['distinct_properties']:,}")
    print("Null property_id:", f"{stats['null_property_id'] or 0:,}")

    if stats["rows"] == 0:
        raise RuntimeError("DQ FAILED: dim_property is empty.")

    if (stats["null_property_id"] or 0) != 0:
        raise RuntimeError("DQ FAILED: dim_property contains null property_id.")

    if stats["rows"] != stats["distinct_properties"]:
        raise RuntimeError("DQ FAILED: dim_property property_id is not unique.")

    return stats["rows"]


def validate_building_risk_input(building_risk_df):
    stats = building_risk_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("building_id").alias("distinct_buildings"),
        F.sum(
            F.when(F.col("building_id").isNull(), 1).otherwise(0)
        ).alias("null_building_id"),
        F.sum(
            F.when(F.col("building_risk_score").isNull(), 1).otherwise(0)
        ).alias("null_score"),
        F.min("building_risk_score").alias("min_score"),
        F.max("building_risk_score").alias("max_score"),
    ).first()

    print("\n" + "-" * 60)
    print("BUILDING RISK INPUT DATA QUALITY")
    print("-" * 60)
    print("Rows:", f"{stats['rows']:,}")
    print("Distinct building_id:", f"{stats['distinct_buildings']:,}")
    print("Null building_id:", f"{stats['null_building_id'] or 0:,}")
    print("Null Building Risk score:", f"{stats['null_score'] or 0:,}")
    print("Min Building Risk score:", stats["min_score"])
    print("Max Building Risk score:", stats["max_score"])

    if stats["rows"] == 0:
        raise RuntimeError("DQ FAILED: Building Risk input is empty.")

    if (stats["null_building_id"] or 0) != 0:
        raise RuntimeError(
            "DQ FAILED: Building Risk contains null building_id."
        )

    if stats["rows"] != stats["distinct_buildings"]:
        raise RuntimeError(
            "DQ FAILED: Building Risk contains duplicate building_id values."
        )

    if (stats["null_score"] or 0) != 0:
        raise RuntimeError(
            "DQ FAILED: Building Risk contains null building_risk_score."
        )

    if (
        stats["min_score"] is None
        or stats["max_score"] is None
        or stats["min_score"] < 0
        or stats["max_score"] > 100
    ):
        raise RuntimeError(
            "DQ FAILED: Building Risk score is outside the 0-100 range."
        )


def build_property_only_311(fact_311, risk_as_of_date):
    property_only_311 = (
        fact_311
        .filter(
            F.col("property_id").isNotNull()
            & F.col("building_id").isNull()
        )
    )

    property_311_risk = (
        property_only_311
        .withColumn(
            "event_age_days",
            F.datediff(
                F.lit(str(risk_as_of_date)),
                F.to_date("created_date"),
            ),
        )
        .filter(F.col("event_age_days") >= 0)
    )

    property_features_311 = (
        property_311_risk
        .groupBy("property_id")
        .agg(
            F.sum(
                F.when(
                    F.col("event_age_days").between(0, 30),
                    1,
                ).otherwise(0)
            ).alias("property_311_0_30"),
            F.sum(
                F.when(
                    F.col("event_age_days").between(31, 90),
                    1,
                ).otherwise(0)
            ).alias("property_311_31_90"),
            F.sum(
                F.when(
                    F.col("event_age_days").between(91, 365),
                    1,
                ).otherwise(0)
            ).alias("property_311_91_365"),
        )
        .withColumn(
            "property_risk_311_raw",
            F.col("property_311_0_30") * 3
            + F.col("property_311_31_90") * 2
            + F.col("property_311_91_365"),
        )
    )

    return property_only_311, property_311_risk, property_features_311


def build_property_only_hpd(fact_hpd, risk_as_of_date):
    property_only_hpd = (
        fact_hpd
        .filter(
            F.col("property_id").isNotNull()
            & F.col("building_id").isNull()
        )
    )

    property_hpd_risk = (
        property_only_hpd
        .withColumn(
            "event_age_days",
            F.datediff(
                F.lit(str(risk_as_of_date)),
                F.to_date("inspectiondate"),
            ),
        )
        .filter(F.col("event_age_days") >= 0)
    )

    property_features_hpd = (
        property_hpd_risk
        .groupBy("property_id")
        .agg(
            F.sum(
                F.when(
                    (F.col("violationstatus") == "OPEN")
                    & (F.col("class") == "A"),
                    1,
                ).otherwise(0)
            ).alias("property_hpd_open_class_a"),
            F.sum(
                F.when(
                    (F.col("violationstatus") == "OPEN")
                    & (F.col("class") == "B"),
                    1,
                ).otherwise(0)
            ).alias("property_hpd_open_class_b"),
            F.sum(
                F.when(
                    (F.col("violationstatus") == "OPEN")
                    & (F.col("class") == "C"),
                    1,
                ).otherwise(0)
            ).alias("property_hpd_open_class_c"),
            F.sum(
                F.when(
                    F.col("event_age_days").between(0, 30),
                    1,
                ).otherwise(0)
            ).alias("property_hpd_0_30"),
            F.sum(
                F.when(
                    F.col("event_age_days").between(31, 90),
                    1,
                ).otherwise(0)
            ).alias("property_hpd_31_90"),
            F.sum(
                F.when(
                    F.col("event_age_days").between(91, 365),
                    1,
                ).otherwise(0)
            ).alias("property_hpd_91_365"),
        )
        .withColumn(
            "property_risk_hpd_severity_raw",
            F.col("property_hpd_open_class_a")
            + F.col("property_hpd_open_class_b") * 2
            + F.col("property_hpd_open_class_c") * 4,
        )
        .withColumn(
            "property_risk_hpd_recency_raw",
            F.col("property_hpd_0_30") * 3
            + F.col("property_hpd_31_90") * 2
            + F.col("property_hpd_91_365"),
        )
    )

    return property_only_hpd, property_hpd_risk, property_features_hpd


def build_property_building_risk(building_risk):
    return (
        building_risk
        .filter(F.col("property_id").isNotNull())
        .groupBy("property_id")
        .agg(
            F.countDistinct("building_id").alias("building_count"),
            F.max("building_risk_score").alias("max_building_risk_score"),
            F.avg("building_risk_score").alias("avg_building_risk_score"),
        )
    )


def build_property_risk_features(
    dim_property,
    property_building_risk,
    property_features_311,
    property_features_hpd,
):
    property_event_columns = [
        "property_311_0_30",
        "property_311_31_90",
        "property_311_91_365",
        "property_risk_311_raw",
        "property_hpd_open_class_a",
        "property_hpd_open_class_b",
        "property_hpd_open_class_c",
        "property_hpd_0_30",
        "property_hpd_31_90",
        "property_hpd_91_365",
        "property_risk_hpd_severity_raw",
        "property_risk_hpd_recency_raw",
    ]

    return (
        dim_property
        .join(
            property_building_risk,
            on="property_id",
            how="left",
        )
        .join(
            property_features_311,
            on="property_id",
            how="left",
        )
        .join(
            property_features_hpd,
            on="property_id",
            how="left",
        )
        .fillna(
            0,
            subset=property_event_columns,
        )
    )


def build_property_risk_score(property_risk_features, risk_as_of_date):
    normalized = (
        property_risk_features
        .withColumn(
            "property_score_311",
            log_normalize(
                "property_risk_311_raw",
                CAP_PROPERTY_311,
            ),
        )
        .withColumn(
            "property_score_hpd_severity",
            log_normalize(
                "property_risk_hpd_severity_raw",
                CAP_PROPERTY_HPD_SEVERITY,
            ),
        )
        .withColumn(
            "property_score_hpd_recency",
            log_normalize(
                "property_risk_hpd_recency_raw",
                CAP_PROPERTY_HPD_RECENCY,
            ),
        )
        .withColumn(
            "has_property_311",
            F.when(
                F.col("property_risk_311_raw") > 0,
                1,
            ).otherwise(0),
        )
        .withColumn(
            "has_property_hpd",
            F.when(
                (F.col("property_risk_hpd_severity_raw") > 0)
                | (F.col("property_risk_hpd_recency_raw") > 0),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "property_risk_weight_available",
            F.col("has_property_311") * F.lit(PROPERTY_WEIGHT_311)
            + F.col("has_property_hpd") * F.lit(PROPERTY_WEIGHT_HPD_TOTAL),
        )
        .withColumn(
            "property_only_risk_score",
            F.when(
                F.col("property_risk_weight_available") > 0,
                (
                    F.col("property_score_311")
                    * F.col("has_property_311")
                    * F.lit(PROPERTY_WEIGHT_311)
                    + F.col("property_score_hpd_severity")
                    * F.col("has_property_hpd")
                    * F.lit(PROPERTY_WEIGHT_HPD_SEVERITY)
                    + F.col("property_score_hpd_recency")
                    * F.col("has_property_hpd")
                    * F.lit(PROPERTY_WEIGHT_HPD_RECENCY)
                )
                / F.col("property_risk_weight_available"),
            ),
        )
    )

    # Final notebook strategy:
    # Property Risk = MAX(max Building Risk, Property-only Risk).
    scored = (
        normalized
        .withColumn(
            "property_risk_score_raw",
            F.when(
                F.col("max_building_risk_score").isNotNull()
                & F.col("property_only_risk_score").isNotNull(),
                F.greatest(
                    F.col("max_building_risk_score"),
                    F.col("property_only_risk_score"),
                ),
            )
            .when(
                F.col("max_building_risk_score").isNotNull(),
                F.col("max_building_risk_score"),
            )
            .when(
                F.col("property_only_risk_score").isNotNull(),
                F.col("property_only_risk_score"),
            )
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "property_risk_score",
            F.when(
                F.col("property_risk_score_raw").isNotNull(),
                F.round(F.col("property_risk_score_raw"), 2),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "property_risk_source",
            F.when(
                F.col("max_building_risk_score").isNotNull()
                & F.col("property_only_risk_score").isNotNull(),
                F.lit("BUILDING_PLUS_PROPERTY_EVENTS"),
            )
            .when(
                F.col("max_building_risk_score").isNotNull(),
                F.lit("BUILDING_ONLY"),
            )
            .when(
                F.col("property_only_risk_score").isNotNull(),
                F.lit("PROPERTY_EVENTS_ONLY"),
            )
            .otherwise(F.lit("NO_RISK_DATA")),
        )
        .withColumn(
            "property_risk_level",
            F.when(
                F.col("property_risk_score").isNull(),
                F.lit("NO_DATA"),
            )
            .when(
                F.col("property_risk_score") < PROPERTY_P75,
                F.lit("LOW"),
            )
            .when(
                F.col("property_risk_score") < PROPERTY_P95,
                F.lit("MEDIUM"),
            )
            .when(
                F.col("property_risk_score") < PROPERTY_P99,
                F.lit("HIGH"),
            )
            .otherwise(F.lit("CRITICAL")),
        )
        .withColumn(
            "risk_as_of_date",
            F.lit(str(risk_as_of_date)).cast("date"),
        )
        .withColumn(
            "model_version",
            F.lit(MODEL_VERSION),
        )
        .withColumn(
            "calculated_at",
            F.current_timestamp(),
        )
    )

    return scored.select(
        # Identity
        "property_id",
        "bbl",
        "property_address",
        "borough",
        "zipcode",
        "latitude",
        "longitude",

        # Final Risk
        "property_risk_score",
        "property_risk_level",
        "property_risk_source",
        "risk_as_of_date",
        "model_version",
        "calculated_at",

        # Building context
        "building_count",
        "max_building_risk_score",
        "avg_building_risk_score",

        # Property-only score
        "property_only_risk_score",

        # Normalized components
        "property_score_311",
        "property_score_hpd_severity",
        "property_score_hpd_recency",

        # Raw signals
        "property_risk_311_raw",
        "property_risk_hpd_severity_raw",
        "property_risk_hpd_recency_raw",

        # 311 features
        "property_311_0_30",
        "property_311_31_90",
        "property_311_91_365",

        # HPD features
        "property_hpd_open_class_a",
        "property_hpd_open_class_b",
        "property_hpd_open_class_c",
        "property_hpd_0_30",
        "property_hpd_31_90",
        "property_hpd_91_365",

        # Property attributes
        "yearbuilt",
        "numbldgs",
        "numfloors",
        "unitsres",
        "unitstotal",
        "landuse",
        "bldgclass",
    )


def validate_property_features(
    property_only_311,
    property_311_risk,
    property_only_hpd,
    property_hpd_risk,
    property_features_311,
    property_features_hpd,
    property_risk_features,
    expected_rows,
):
    stats = property_risk_features.agg(
        F.count("*").alias("rows"),
        F.countDistinct("property_id").alias("distinct_properties"),
        F.sum(
            F.when(F.col("property_id").isNull(), 1).otherwise(0)
        ).alias("null_property_id"),
        F.sum(
            F.when(F.col("max_building_risk_score").isNotNull(), 1).otherwise(0)
        ).alias("with_building_risk"),
        F.sum(
            F.when(F.col("property_risk_311_raw") > 0, 1).otherwise(0)
        ).alias("with_property_311"),
        F.sum(
            F.when(
                (F.col("property_risk_hpd_severity_raw") > 0)
                | (F.col("property_risk_hpd_recency_raw") > 0),
                1,
            ).otherwise(0)
        ).alias("with_property_hpd"),
    ).first()

    property_only_311_rows = property_only_311.count()
    property_only_311_properties = (
        property_only_311
        .select("property_id")
        .distinct()
        .count()
    )
    eligible_311_rows = property_311_risk.count()
    eligible_311_properties = (
        property_311_risk
        .select("property_id")
        .distinct()
        .count()
    )

    property_only_hpd_rows = property_only_hpd.count()
    property_only_hpd_properties = (
        property_only_hpd
        .select("property_id")
        .distinct()
        .count()
    )
    eligible_hpd_rows = property_hpd_risk.count()
    eligible_hpd_properties = (
        property_hpd_risk
        .select("property_id")
        .distinct()
        .count()
    )

    feature_311_properties = property_features_311.count()
    feature_hpd_properties = property_features_hpd.count()

    print("\n" + "-" * 60)
    print("PROPERTY RISK FEATURES DATA QUALITY")
    print("-" * 60)
    print("Expected dim_property rows:", f"{expected_rows:,}")
    print("Feature rows:", f"{stats['rows']:,}")
    print("Distinct property_id:", f"{stats['distinct_properties']:,}")
    print("Null property_id:", f"{stats['null_property_id'] or 0:,}")
    print("311 property-only events:", f"{property_only_311_rows:,}")
    print(
        "311 properties affected:",
        f"{property_only_311_properties:,}",
    )
    print("311 eligible events as-of date:", f"{eligible_311_rows:,}")
    print(
        "311 eligible properties as-of date:",
        f"{eligible_311_properties:,}",
    )
    print(
        "311 feature properties:",
        f"{feature_311_properties:,}",
    )
    print("HPD property-only events:", f"{property_only_hpd_rows:,}")
    print(
        "HPD properties affected:",
        f"{property_only_hpd_properties:,}",
    )
    print("HPD eligible events as-of date:", f"{eligible_hpd_rows:,}")
    print(
        "HPD eligible properties as-of date:",
        f"{eligible_hpd_properties:,}",
    )
    print(
        "HPD feature properties:",
        f"{feature_hpd_properties:,}",
    )
    print(
        "Properties with Building Risk:",
        f"{stats['with_building_risk'] or 0:,}",
    )
    print(
        "Properties with property-only 311 Risk:",
        f"{stats['with_property_311'] or 0:,}",
    )
    print(
        "Properties with property-only HPD Risk:",
        f"{stats['with_property_hpd'] or 0:,}",
    )

    if stats["rows"] != expected_rows:
        raise RuntimeError(
            "DQ FAILED: Property Risk feature rows do not match dim_property."
        )

    if stats["rows"] != stats["distinct_properties"]:
        raise RuntimeError(
            "DQ FAILED: Property Risk features contain duplicate property_id."
        )

    if (stats["null_property_id"] or 0) != 0:
        raise RuntimeError(
            "DQ FAILED: Property Risk features contain null property_id."
        )

    if eligible_311_properties != feature_311_properties:
        raise RuntimeError(
            "DQ FAILED: 311 feature aggregation does not match "
            "properties eligible at the Risk As-Of Date."
        )

    if eligible_hpd_properties != feature_hpd_properties:
        raise RuntimeError(
            "DQ FAILED: HPD feature aggregation does not match "
            "properties eligible at the Risk As-Of Date."
        )


def validate_property_risk(risk_df, expected_rows, risk_as_of_date, label):
    invalid_component_condition = (
        F.col("property_score_311").isNull()
        | (F.col("property_score_311") < 0)
        | (F.col("property_score_311") > 100)
        | F.col("property_score_hpd_severity").isNull()
        | (F.col("property_score_hpd_severity") < 0)
        | (F.col("property_score_hpd_severity") > 100)
        | F.col("property_score_hpd_recency").isNull()
        | (F.col("property_score_hpd_recency") < 0)
        | (F.col("property_score_hpd_recency") > 100)
    )

    stats = risk_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("property_id").alias("distinct_properties"),
        F.sum(
            F.when(F.col("property_id").isNull(), 1).otherwise(0)
        ).alias("null_property_id"),
        F.sum(
            F.when(F.col("property_risk_score").isNotNull(), 1).otherwise(0)
        ).alias("with_risk"),
        F.sum(
            F.when(F.col("property_risk_score").isNull(), 1).otherwise(0)
        ).alias("without_risk"),
        F.min("property_risk_score").alias("min_score"),
        F.max("property_risk_score").alias("max_score"),
        F.avg("property_risk_score").alias("avg_score"),
        F.sum(
            F.when(
                ~F.col("property_risk_level").isin(*ALLOWED_RISK_LEVELS),
                1,
            ).otherwise(0)
        ).alias("invalid_risk_level"),
        F.sum(
            F.when(
                ~F.col("property_risk_source").isin(*ALLOWED_RISK_SOURCES),
                1,
            ).otherwise(0)
        ).alias("invalid_risk_source"),
        F.sum(
            F.when(
                F.col("property_risk_score").isNull()
                & (
                    (F.col("property_risk_level") != "NO_DATA")
                    | (F.col("property_risk_source") != "NO_RISK_DATA")
                ),
                1,
            ).otherwise(0)
        ).alias("invalid_no_data_consistency"),
        F.sum(
            F.when(
                F.col("property_risk_score").isNotNull()
                & (
                    (F.col("property_risk_level") == "NO_DATA")
                    | (F.col("property_risk_source") == "NO_RISK_DATA")
                ),
                1,
            ).otherwise(0)
        ).alias("invalid_risk_consistency"),
        F.sum(
            F.when(invalid_component_condition, 1).otherwise(0)
        ).alias("invalid_component_rows"),
        F.sum(
            F.when(
                F.col("property_only_risk_score").isNotNull()
                & (
                    (F.col("property_only_risk_score") < 0)
                    | (F.col("property_only_risk_score") > 100)
                ),
                1,
            ).otherwise(0)
        ).alias("invalid_property_only_score"),
        F.sum(
            F.when(
                F.col("max_building_risk_score").isNotNull()
                & (
                    (F.col("max_building_risk_score") < 0)
                    | (F.col("max_building_risk_score") > 100)
                ),
                1,
            ).otherwise(0)
        ).alias("invalid_max_building_score"),
        F.sum(
            F.when(
                F.col("risk_as_of_date") != F.lit(str(risk_as_of_date)).cast("date"),
                1,
            ).otherwise(0)
        ).alias("wrong_as_of_date"),
    ).first()

    print("\n" + "-" * 60)
    print(f"{label} DATA QUALITY")
    print("-" * 60)
    print("Expected rows:", f"{expected_rows:,}")
    print("Risk rows:", f"{stats['rows']:,}")
    print("Distinct property_id:", f"{stats['distinct_properties']:,}")
    print("Null property_id:", f"{stats['null_property_id'] or 0:,}")
    print("With Risk:", f"{stats['with_risk'] or 0:,}")
    print("Without Risk:", f"{stats['without_risk'] or 0:,}")
    print("Min score:", stats["min_score"])
    print("Max score:", stats["max_score"])
    print(
        "Average score:",
        round(stats["avg_score"], 2)
        if stats["avg_score"] is not None
        else None,
    )
    print(
        "Invalid risk levels:",
        f"{stats['invalid_risk_level'] or 0:,}",
    )
    print(
        "Invalid risk sources:",
        f"{stats['invalid_risk_source'] or 0:,}",
    )
    print(
        "Invalid NO_DATA consistency:",
        f"{stats['invalid_no_data_consistency'] or 0:,}",
    )
    print(
        "Invalid Risk consistency:",
        f"{stats['invalid_risk_consistency'] or 0:,}",
    )
    print(
        "Rows with invalid normalized components:",
        f"{stats['invalid_component_rows'] or 0:,}",
    )
    print(
        "Invalid property-only scores:",
        f"{stats['invalid_property_only_score'] or 0:,}",
    )
    print(
        "Invalid max Building Risk scores:",
        f"{stats['invalid_max_building_score'] or 0:,}",
    )
    print(
        "Rows with wrong Risk As-Of Date:",
        f"{stats['wrong_as_of_date'] or 0:,}",
    )

    if stats["rows"] != expected_rows:
        raise RuntimeError(
            f"DQ FAILED: {label} row count does not match dim_property."
        )

    if stats["rows"] != stats["distinct_properties"]:
        raise RuntimeError(
            f"DQ FAILED: {label} contains duplicate property_id values."
        )

    if (stats["null_property_id"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains null property_id values."
        )

    if (
        stats["min_score"] is not None
        and stats["min_score"] < 0
    ):
        raise RuntimeError(
            f"DQ FAILED: {label} contains Property Risk scores below 0."
        )

    if (
        stats["max_score"] is not None
        and stats["max_score"] > 100
    ):
        raise RuntimeError(
            f"DQ FAILED: {label} contains Property Risk scores above 100."
        )

    if (stats["invalid_risk_level"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains invalid property_risk_level."
        )

    if (stats["invalid_risk_source"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains invalid property_risk_source."
        )

    if (stats["invalid_no_data_consistency"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} has inconsistent NO_DATA rows."
        )

    if (stats["invalid_risk_consistency"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} has inconsistent scored rows."
        )

    if (stats["invalid_component_rows"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} has normalized components outside 0-100."
        )

    if (stats["invalid_property_only_score"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} has property-only scores outside 0-100."
        )

    if (stats["invalid_max_building_score"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} has max Building Risk outside 0-100."
        )

    if (stats["wrong_as_of_date"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains unexpected risk_as_of_date."
        )


def print_property_risk_distribution(risk_df):
    print("\nPROPERTY RISK LEVEL DISTRIBUTION")
    (
        risk_df
        .groupBy("property_risk_level")
        .count()
        .orderBy(
            F.when(F.col("property_risk_level") == "CRITICAL", 1)
            .when(F.col("property_risk_level") == "HIGH", 2)
            .when(F.col("property_risk_level") == "MEDIUM", 3)
            .when(F.col("property_risk_level") == "LOW", 4)
            .otherwise(5)
        )
        .show(truncate=False)
    )

    print("\nPROPERTY RISK SOURCE DISTRIBUTION")
    (
        risk_df
        .groupBy("property_risk_source")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )


# ============================================================
# MAIN
# ============================================================


def main():
    spark = create_spark_session(
        "NYC Building Risk - Property Risk Production"
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n" + "=" * 60)
        print("PROPERTY RISK PRODUCTION JOB")
        print("=" * 60)
        print("Model version:", MODEL_VERSION)

        dim_property_path = minio_path(
            "gold/data_model/dim_property"
        )
        fact_311_path = minio_path(
            "gold/data_model/fact_311_event"
        )
        fact_hpd_path = minio_path(
            "gold/data_model/fact_hpd_violation"
        )
        building_risk_path = minio_path(
            "gold/building_risk/building_risk_score"
        )
        property_risk_path = minio_path(
            "gold/property_risk/property_risk_score"
        )

        print("dim_property:", dim_property_path)
        print("fact_311_event:", fact_311_path)
        print("fact_hpd_violation:", fact_hpd_path)
        print("building_risk:", building_risk_path)
        print("Property Risk output:", property_risk_path)

        dim_property = spark.read.parquet(dim_property_path)
        fact_311 = spark.read.parquet(fact_311_path)
        fact_hpd = spark.read.parquet(fact_hpd_path)
        building_risk = spark.read.parquet(building_risk_path)

        expected_rows = validate_dim_property(dim_property)
        validate_building_risk_input(building_risk)

        risk_as_of_date = get_building_risk_as_of_date(
            building_risk
        )

        print("\nMODEL CONSTANTS")
        print("Risk as-of date:", risk_as_of_date)
        print(
            "Property-only caps:",
            {
                "311": CAP_PROPERTY_311,
                "hpd_severity": CAP_PROPERTY_HPD_SEVERITY,
                "hpd_recency": CAP_PROPERTY_HPD_RECENCY,
            },
        )
        print(
            "Property-only weights:",
            {
                "311": PROPERTY_WEIGHT_311,
                "hpd_severity": PROPERTY_WEIGHT_HPD_SEVERITY,
                "hpd_recency": PROPERTY_WEIGHT_HPD_RECENCY,
            },
        )
        print(
            "Final strategy:",
            "MAX(max_building_risk_score, property_only_risk_score)",
        )
        print(
            "Risk thresholds:",
            {
                "MEDIUM": PROPERTY_P75,
                "HIGH": PROPERTY_P95,
                "CRITICAL": PROPERTY_P99,
            },
        )

        (
            property_only_311,
            property_311_risk,
            property_features_311,
        ) = build_property_only_311(
            fact_311,
            risk_as_of_date,
        )

        (
            property_only_hpd,
            property_hpd_risk,
            property_features_hpd,
        ) = build_property_only_hpd(
            fact_hpd,
            risk_as_of_date,
        )

        property_building_risk = (
            build_property_building_risk(building_risk)
        )

        property_risk_features = build_property_risk_features(
            dim_property,
            property_building_risk,
            property_features_311,
            property_features_hpd,
        )

        validate_property_features(
            property_only_311,
            property_311_risk,
            property_only_hpd,
            property_hpd_risk,
            property_features_311,
            property_features_hpd,
            property_risk_features,
            expected_rows,
        )

        property_risk_output = build_property_risk_score(
            property_risk_features,
            risk_as_of_date,
        )

        validate_property_risk(
            property_risk_output,
            expected_rows,
            risk_as_of_date,
            "PROPERTY RISK IN-MEMORY",
        )
        print_property_risk_distribution(property_risk_output)

        (
            property_risk_output
            .write
            .mode("overwrite")
            .parquet(property_risk_path)
        )
        print("\nProperty Risk score saved:", property_risk_path)

        # Validate persisted output, not only the in-memory DataFrame.
        property_risk_check = spark.read.parquet(
            property_risk_path
        )

        validate_property_risk(
            property_risk_check,
            expected_rows,
            risk_as_of_date,
            "PROPERTY RISK PERSISTED",
        )
        print_property_risk_distribution(property_risk_check)

        print("\n" + "=" * 60)
        print("PROPERTY RISK COMPLETED")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
