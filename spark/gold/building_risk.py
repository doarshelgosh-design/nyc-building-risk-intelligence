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
# These constants preserve the scoring logic calibrated in the
# validated Building Risk notebook.
# ============================================================

MODEL_VERSION = "building_risk_v1"

CAP_311 = 96.0
CAP_HPD_SEVERITY = 102.0
CAP_HPD_RECENCY = 104.0
CAP_DOB_ACTIVE = 4.0
CAP_DOB_RECENCY = 6.0
CAP_BUILDING_AGE = 160.0

WEIGHT_311 = 0.25
WEIGHT_HPD_SEVERITY = 0.28
WEIGHT_HPD_RECENCY = 0.12
WEIGHT_DOB_ACTIVE = 0.15
WEIGHT_DOB_RECENCY = 0.10
WEIGHT_BUILDING_AGE = 0.10

# Risk-level thresholds calibrated in the validated notebook.
P75_SCORE = 22.98
P95_SCORE = 54.52
P99_SCORE = 74.81

ALLOWED_RISK_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


# ============================================================
# HELPERS
# ============================================================


def get_max_event_date(df, column_name, label):
    max_date = (
        df.select(F.max(F.to_date(F.col(column_name))).alias("max_date"))
        .first()["max_date"]
    )

    if max_date is None:
        raise RuntimeError(
            f"DQ FAILED: cannot calculate Risk As-Of Date because {label} "
            f"has no valid {column_name} values."
        )

    return max_date


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


def validate_building_features(dim_building_df, features_df):
    dim_stats = dim_building_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("building_id").alias("distinct_buildings"),
        F.sum(
            F.when(F.col("building_id").isNull(), 1).otherwise(0)
        ).alias("null_building_id"),
    ).first()

    feature_stats = features_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("building_id").alias("distinct_buildings"),
        F.sum(
            F.when(F.col("building_id").isNull(), 1).otherwise(0)
        ).alias("null_building_id"),
        F.sum(F.col("has_311_history")).alias("with_311"),
        F.sum(F.col("has_hpd_history")).alias("with_hpd"),
        F.sum(F.col("has_dob_history")).alias("with_dob"),
        F.sum(F.col("units_normalization_eligible")).alias(
            "units_normalization_eligible"
        ),
    ).first()

    print("\n" + "-" * 60)
    print("BUILDING RISK FEATURES DATA QUALITY")
    print("-" * 60)
    print("dim_building rows:", f"{dim_stats['rows']:,}")
    print(
        "dim_building distinct building_id:",
        f"{dim_stats['distinct_buildings']:,}",
    )
    print("Feature rows:", f"{feature_stats['rows']:,}")
    print(
        "Feature distinct building_id:",
        f"{feature_stats['distinct_buildings']:,}",
    )
    print("With 311 history:", f"{feature_stats['with_311'] or 0:,}")
    print("With HPD history:", f"{feature_stats['with_hpd'] or 0:,}")
    print("With DOB history:", f"{feature_stats['with_dob'] or 0:,}")
    print(
        "Eligible for units normalization:",
        f"{feature_stats['units_normalization_eligible'] or 0:,}",
    )

    if dim_stats["rows"] == 0:
        raise RuntimeError("DQ FAILED: dim_building is empty.")

    if (dim_stats["null_building_id"] or 0) != 0:
        raise RuntimeError("DQ FAILED: dim_building contains null building_id.")

    if dim_stats["rows"] != dim_stats["distinct_buildings"]:
        raise RuntimeError("DQ FAILED: dim_building building_id is not unique.")

    if feature_stats["rows"] != dim_stats["rows"]:
        raise RuntimeError(
            "DQ FAILED: Building Risk features row count does not match dim_building."
        )

    if feature_stats["rows"] != feature_stats["distinct_buildings"]:
        raise RuntimeError(
            "DQ FAILED: Building Risk features contain duplicate building_id values."
        )

    if (feature_stats["null_building_id"] or 0) != 0:
        raise RuntimeError(
            "DQ FAILED: Building Risk features contain null building_id values."
        )

    return dim_stats["rows"]


def validate_building_risk(risk_df, expected_rows, label):
    component_columns = [
        "score_311",
        "score_hpd_severity",
        "score_hpd_recency",
        "score_dob_active",
        "score_dob_recency",
        "score_building_age_final",
    ]

    invalid_component_condition = None
    for column_name in component_columns:
        condition = (
            F.col(column_name).isNull()
            | (F.col(column_name) < 0)
            | (F.col(column_name) > 100)
        )
        invalid_component_condition = (
            condition
            if invalid_component_condition is None
            else invalid_component_condition | condition
        )

    stats = risk_df.agg(
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
        F.avg("building_risk_score").alias("avg_score"),
        F.sum(
            F.when(
                ~F.col("risk_level").isin(*ALLOWED_RISK_LEVELS),
                1,
            ).otherwise(0)
        ).alias("invalid_risk_level"),
        F.sum(
            F.when(invalid_component_condition, 1).otherwise(0)
        ).alias("invalid_component_rows"),
    ).first()

    print("\n" + "-" * 60)
    print(f"{label} DATA QUALITY")
    print("-" * 60)
    print("Expected rows:", f"{expected_rows:,}")
    print("Risk rows:", f"{stats['rows']:,}")
    print("Distinct building_id:", f"{stats['distinct_buildings']:,}")
    print("Null building_id:", f"{stats['null_building_id'] or 0:,}")
    print("Null risk score:", f"{stats['null_score'] or 0:,}")
    print("Min score:", stats["min_score"])
    print("Max score:", stats["max_score"])
    print("Average score:", round(stats["avg_score"], 2) if stats["avg_score"] is not None else None)
    print("Invalid risk levels:", f"{stats['invalid_risk_level'] or 0:,}")
    print(
        "Rows with invalid component scores:",
        f"{stats['invalid_component_rows'] or 0:,}",
    )

    if stats["rows"] != expected_rows:
        raise RuntimeError(
            f"DQ FAILED: {label} row count does not match dim_building."
        )

    if stats["rows"] != stats["distinct_buildings"]:
        raise RuntimeError(
            f"DQ FAILED: {label} contains duplicate building_id values."
        )

    if (stats["null_building_id"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains null building_id values."
        )

    if (stats["null_score"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains null Building Risk Score values."
        )

    if stats["min_score"] is None or stats["max_score"] is None:
        raise RuntimeError(f"DQ FAILED: {label} has no score values.")

    if stats["min_score"] < 0 or stats["max_score"] > 100:
        raise RuntimeError(
            f"DQ FAILED: {label} contains scores outside the 0-100 range."
        )

    if (stats["invalid_risk_level"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains invalid risk_level values."
        )

    if (stats["invalid_component_rows"] or 0) != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} contains component scores outside 0-100 or null."
        )


def print_risk_distribution(risk_df):
    print("\nRISK LEVEL DISTRIBUTION")
    (
        risk_df.groupBy("risk_level")
        .count()
        .orderBy(
            F.when(F.col("risk_level") == "CRITICAL", 1)
            .when(F.col("risk_level") == "HIGH", 2)
            .when(F.col("risk_level") == "MEDIUM", 3)
            .otherwise(4)
        )
        .show(truncate=False)
    )


# ============================================================
# FEATURE ENGINEERING
# ============================================================


def build_risk_features(
    dim_building,
    dim_property,
    fact_311,
    fact_hpd,
    fact_dob,
    risk_as_of_date,
):
    risk_date = F.lit(str(risk_as_of_date)).cast("date")

    # ------------------------------------------------------------
    # NYC 311 features
    # Non-overlapping buckets: 0-30, 31-90, 91-365 days.
    # ------------------------------------------------------------
    fact_311_risk = (
        fact_311.filter(F.col("building_id").isNotNull())
        .withColumn(
            "event_age_days",
            F.datediff(risk_date, F.to_date("created_date")),
        )
        .filter(F.col("event_age_days") >= 0)
    )

    features_311 = fact_311_risk.groupBy("building_id").agg(
        F.sum(
            F.when(F.col("event_age_days").between(0, 30), 1).otherwise(0)
        ).alias("complaints_0_30"),
        F.sum(
            F.when(F.col("event_age_days").between(31, 90), 1).otherwise(0)
        ).alias("complaints_31_90"),
        F.sum(
            F.when(F.col("event_age_days").between(91, 365), 1).otherwise(0)
        ).alias("complaints_91_365"),
        F.sum(
            F.when(F.col("event_age_days").between(0, 365), 1).otherwise(0)
        ).alias("complaints_365"),
    )

    # ------------------------------------------------------------
    # HPD features
    # ------------------------------------------------------------
    fact_hpd_risk = (
        fact_hpd.filter(F.col("building_id").isNotNull())
        .withColumn(
            "event_age_days",
            F.datediff(risk_date, F.to_date("inspectiondate")),
        )
        .filter(F.col("event_age_days") >= 0)
    )

    features_hpd = fact_hpd_risk.groupBy("building_id").agg(
        F.sum(
            F.when(
                (F.col("violationstatus") == "OPEN")
                & (F.col("class") == "A"),
                1,
            ).otherwise(0)
        ).alias("hpd_open_class_a"),
        F.sum(
            F.when(
                (F.col("violationstatus") == "OPEN")
                & (F.col("class") == "B"),
                1,
            ).otherwise(0)
        ).alias("hpd_open_class_b"),
        F.sum(
            F.when(
                (F.col("violationstatus") == "OPEN")
                & (F.col("class") == "C"),
                1,
            ).otherwise(0)
        ).alias("hpd_open_class_c"),
        F.sum(
            F.when(
                (F.col("violationstatus") == "OPEN")
                & (F.col("class") == "I"),
                1,
            ).otherwise(0)
        ).alias("hpd_open_class_i"),
        F.sum(
            F.when(F.col("violationstatus") == "OPEN", 1).otherwise(0)
        ).alias("hpd_active_violations"),
        F.sum(
            F.when(F.col("event_age_days").between(0, 30), 1).otherwise(0)
        ).alias("hpd_0_30"),
        F.sum(
            F.when(F.col("event_age_days").between(31, 90), 1).otherwise(0)
        ).alias("hpd_31_90"),
        F.sum(
            F.when(F.col("event_age_days").between(91, 365), 1).otherwise(0)
        ).alias("hpd_91_365"),
        F.sum(
            F.when(F.col("event_age_days").between(0, 365), 1).otherwise(0)
        ).alias("hpd_365"),
    )

    # ------------------------------------------------------------
    # DOB features
    # ------------------------------------------------------------
    fact_dob_risk = (
        fact_dob.filter(F.col("building_id").isNotNull())
        .withColumn(
            "event_age_days",
            F.datediff(risk_date, F.to_date("violation_issue_date")),
        )
        .filter(F.col("event_age_days") >= 0)
    )

    features_dob = fact_dob_risk.groupBy("building_id").agg(
        F.sum(
            F.when(F.col("violation_status") == "ACTIVE", 1).otherwise(0)
        ).alias("dob_active_violations"),
        F.sum(
            F.when(F.col("event_age_days").between(0, 30), 1).otherwise(0)
        ).alias("dob_0_30"),
        F.sum(
            F.when(F.col("event_age_days").between(31, 90), 1).otherwise(0)
        ).alias("dob_31_90"),
        F.sum(
            F.when(F.col("event_age_days").between(91, 365), 1).otherwise(0)
        ).alias("dob_91_365"),
        F.sum(
            F.when(F.col("event_age_days").between(0, 365), 1).otherwise(0)
        ).alias("dob_365"),
    )

    # ------------------------------------------------------------
    # PLUTO / Property context
    # ------------------------------------------------------------
    property_features = dim_property.select(
        "property_id",
        F.col("yearbuilt").alias("property_yearbuilt"),
        F.col("yearalter1").alias("property_yearalter1"),
        F.col("yearalter2").alias("property_yearalter2"),
        F.col("numbldgs").alias("property_numbldgs"),
        F.col("numfloors").alias("property_numfloors"),
        F.col("unitsres").alias("property_unitsres"),
        F.col("unitstotal").alias("property_unitstotal"),
        F.col("lotarea").alias("property_lotarea"),
        F.col("bldgarea").alias("property_bldgarea"),
        F.col("resarea").alias("property_resarea"),
        F.col("comarea").alias("property_comarea"),
        F.col("landuse").alias("property_landuse"),
        F.col("bldgclass").alias("property_bldgclass"),
    )

    features_311_join = features_311.withColumn("has_311_history", F.lit(1))
    features_hpd_join = features_hpd.withColumn("has_hpd_history", F.lit(1))
    features_dob_join = features_dob.withColumn("has_dob_history", F.lit(1))

    building_risk_features = (
        dim_building.join(property_features, on="property_id", how="left")
        .join(features_311_join, on="building_id", how="left")
        .join(features_hpd_join, on="building_id", how="left")
        .join(features_dob_join, on="building_id", how="left")
    )

    event_feature_columns = [
        "complaints_0_30",
        "complaints_31_90",
        "complaints_91_365",
        "complaints_365",
        "hpd_open_class_a",
        "hpd_open_class_b",
        "hpd_open_class_c",
        "hpd_open_class_i",
        "hpd_active_violations",
        "hpd_0_30",
        "hpd_31_90",
        "hpd_91_365",
        "hpd_365",
        "dob_active_violations",
        "dob_0_30",
        "dob_31_90",
        "dob_91_365",
        "dob_365",
        "has_311_history",
        "has_hpd_history",
        "has_dob_history",
    ]

    risk_year = risk_as_of_date.year

    return (
        building_risk_features.fillna(0, subset=event_feature_columns)
        .withColumn(
            "building_age",
            F.when(
                (F.col("property_yearbuilt") > 0)
                & (F.col("property_yearbuilt") <= risk_year),
                F.lit(risk_year) - F.col("property_yearbuilt"),
            ),
        )
        .withColumn(
            "units_normalization_eligible",
            F.when(
                (F.col("property_numbldgs") == 1)
                & (F.col("property_unitsres") > 0),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "units_for_normalization",
            F.when(
                F.col("units_normalization_eligible") == 1,
                F.col("property_unitsres"),
            ),
        )
    )


# ============================================================
# SCORING
# ============================================================


def build_risk_score(building_risk_features, risk_as_of_date):
    risk_signals = (
        building_risk_features.withColumn(
            "risk_311_raw",
            F.col("complaints_0_30") * 3
            + F.col("complaints_31_90") * 2
            + F.col("complaints_91_365"),
        )
        .withColumn(
            "risk_hpd_severity_raw",
            F.col("hpd_open_class_a")
            + F.col("hpd_open_class_b") * 2
            + F.col("hpd_open_class_c") * 4,
        )
        .withColumn(
            "risk_hpd_recency_raw",
            F.col("hpd_0_30") * 3
            + F.col("hpd_31_90") * 2
            + F.col("hpd_91_365"),
        )
        .withColumn(
            "risk_dob_active_raw",
            F.col("dob_active_violations"),
        )
        .withColumn(
            "risk_dob_recency_raw",
            F.col("dob_0_30") * 3
            + F.col("dob_31_90") * 2
            + F.col("dob_91_365"),
        )
    )

    risk_normalized = (
        risk_signals.withColumn(
            "score_311",
            log_normalize("risk_311_raw", CAP_311),
        )
        .withColumn(
            "score_hpd_severity",
            log_normalize("risk_hpd_severity_raw", CAP_HPD_SEVERITY),
        )
        .withColumn(
            "score_hpd_recency",
            log_normalize("risk_hpd_recency_raw", CAP_HPD_RECENCY),
        )
        .withColumn(
            "score_dob_active",
            log_normalize("risk_dob_active_raw", CAP_DOB_ACTIVE),
        )
        .withColumn(
            "score_dob_recency",
            log_normalize("risk_dob_recency_raw", CAP_DOB_RECENCY),
        )
        .withColumn(
            "score_building_age",
            F.when(
                F.col("building_age").isNotNull(),
                F.least(
                    F.col("building_age"),
                    F.lit(CAP_BUILDING_AGE),
                )
                / F.lit(CAP_BUILDING_AGE)
                * 100,
            ),
        )
    )

    risk_scored = (
        risk_normalized.withColumn(
            "age_imputed_flag",
            F.when(F.col("score_building_age").isNull(), F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "score_building_age_final",
            F.coalesce(F.col("score_building_age"), F.lit(60.0)),
        )
        .withColumn(
            "risk_points_311",
            F.col("score_311") * F.lit(WEIGHT_311),
        )
        .withColumn(
            "risk_points_hpd_severity",
            F.col("score_hpd_severity") * F.lit(WEIGHT_HPD_SEVERITY),
        )
        .withColumn(
            "risk_points_hpd_recency",
            F.col("score_hpd_recency") * F.lit(WEIGHT_HPD_RECENCY),
        )
        .withColumn(
            "risk_points_dob_active",
            F.col("score_dob_active") * F.lit(WEIGHT_DOB_ACTIVE),
        )
        .withColumn(
            "risk_points_dob_recency",
            F.col("score_dob_recency") * F.lit(WEIGHT_DOB_RECENCY),
        )
        .withColumn(
            "risk_points_building_age",
            F.col("score_building_age_final") * F.lit(WEIGHT_BUILDING_AGE),
        )
        .withColumn(
            "building_risk_score",
            F.round(
                F.col("risk_points_311")
                + F.col("risk_points_hpd_severity")
                + F.col("risk_points_hpd_recency")
                + F.col("risk_points_dob_active")
                + F.col("risk_points_dob_recency")
                + F.col("risk_points_building_age"),
                2,
            ),
        )
        .withColumn(
            "risk_level",
            F.when(
                F.col("building_risk_score") >= P99_SCORE,
                F.lit("CRITICAL"),
            )
            .when(
                F.col("building_risk_score") >= P95_SCORE,
                F.lit("HIGH"),
            )
            .when(
                F.col("building_risk_score") >= P75_SCORE,
                F.lit("MEDIUM"),
            )
            .otherwise(F.lit("LOW")),
        )
    )

    return risk_scored.select(
        # Identity
        "building_id",
        "bin",
        "property_id",
        "current_address",
        "borough",
        "latitude",
        "longitude",
        # Final Risk
        "building_risk_score",
        "risk_level",
        # Component Scores
        "score_311",
        "score_hpd_severity",
        "score_hpd_recency",
        "score_dob_active",
        "score_dob_recency",
        "score_building_age_final",
        # Weighted Contributions
        "risk_points_311",
        "risk_points_hpd_severity",
        "risk_points_hpd_recency",
        "risk_points_dob_active",
        "risk_points_dob_recency",
        "risk_points_building_age",
        # Raw Signals
        "risk_311_raw",
        "risk_hpd_severity_raw",
        "risk_hpd_recency_raw",
        "risk_dob_active_raw",
        "risk_dob_recency_raw",
        # Main Features
        "complaints_0_30",
        "complaints_31_90",
        "complaints_91_365",
        "hpd_open_class_a",
        "hpd_open_class_b",
        "hpd_open_class_c",
        "hpd_open_class_i",
        "hpd_active_violations",
        "dob_active_violations",
        # Building Context
        "property_yearbuilt",
        "building_age",
        "property_numbldgs",
        "property_unitsres",
        "property_unitstotal",
        "age_imputed_flag",
        "units_normalization_eligible",
        # Production metadata
        F.lit(str(risk_as_of_date)).cast("date").alias("risk_as_of_date"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.current_timestamp().alias("calculated_at"),
    )


# ============================================================
# MAIN
# ============================================================


def main():
    spark = create_spark_session("NYC Building Risk - Building Risk Production")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n" + "=" * 60)
        print("BUILDING RISK PRODUCTION JOB")
        print("=" * 60)
        print("Model version:", MODEL_VERSION)

        dim_building_path = minio_path("gold/data_model/dim_building")
        dim_property_path = minio_path("gold/data_model/dim_property")
        fact_311_path = minio_path("gold/data_model/fact_311_event")
        fact_hpd_path = minio_path("gold/data_model/fact_hpd_violation")
        fact_dob_path = minio_path("gold/data_model/fact_dob_violation")

        features_path = minio_path("gold/building_risk/features")
        risk_score_path = minio_path(
            "gold/building_risk/building_risk_score"
        )

        print("dim_building:", dim_building_path)
        print("dim_property:", dim_property_path)
        print("fact_311_event:", fact_311_path)
        print("fact_hpd_violation:", fact_hpd_path)
        print("fact_dob_violation:", fact_dob_path)
        print("Features output:", features_path)
        print("Risk output:", risk_score_path)

        dim_building = spark.read.parquet(dim_building_path)
        dim_property = spark.read.parquet(dim_property_path)
        fact_311 = spark.read.parquet(fact_311_path)
        fact_hpd = spark.read.parquet(fact_hpd_path)
        fact_dob = spark.read.parquet(fact_dob_path)

        max_311_date = get_max_event_date(
            fact_311,
            "created_date",
            "fact_311_event",
        )
        max_hpd_date = get_max_event_date(
            fact_hpd,
            "inspectiondate",
            "fact_hpd_violation",
        )
        max_dob_date = get_max_event_date(
            fact_dob,
            "violation_issue_date",
            "fact_dob_violation",
        )

        # Preserve notebook logic: the score is evaluated on the latest
        # date that is supported by all three event sources.
        risk_as_of_date = min(
            max_311_date,
            max_hpd_date,
            max_dob_date,
        )

        print("\nSOURCE MAX DATES")
        print("311 max date:", max_311_date)
        print("HPD max date:", max_hpd_date)
        print("DOB max date:", max_dob_date)
        print("Risk as-of date:", risk_as_of_date)

        print("\nMODEL CONSTANTS")
        print(
            "Weights:",
            {
                "311": WEIGHT_311,
                "hpd_severity": WEIGHT_HPD_SEVERITY,
                "hpd_recency": WEIGHT_HPD_RECENCY,
                "dob_active": WEIGHT_DOB_ACTIVE,
                "dob_recency": WEIGHT_DOB_RECENCY,
                "building_age": WEIGHT_BUILDING_AGE,
            },
        )
        print(
            "Risk thresholds:",
            {
                "MEDIUM": P75_SCORE,
                "HIGH": P95_SCORE,
                "CRITICAL": P99_SCORE,
            },
        )

        building_risk_features = build_risk_features(
            dim_building,
            dim_property,
            fact_311,
            fact_hpd,
            fact_dob,
            risk_as_of_date,
        )

        expected_rows = validate_building_features(
            dim_building,
            building_risk_features,
        )

        building_risk_score = build_risk_score(
            building_risk_features,
            risk_as_of_date,
        )

        validate_building_risk(
            building_risk_score,
            expected_rows,
            "BUILDING RISK IN-MEMORY",
        )
        print_risk_distribution(building_risk_score)

        # Publish only after all in-memory blocking DQ gates pass.
        building_risk_features.write.mode("overwrite").parquet(features_path)
        print("Building Risk features saved:", features_path)

        building_risk_score.write.mode("overwrite").parquet(risk_score_path)
        print("Building Risk score saved:", risk_score_path)

        # Validate the persisted datasets, not only the DataFrames in memory.
        features_check = spark.read.parquet(features_path)
        risk_check = spark.read.parquet(risk_score_path)

        validate_building_features(dim_building, features_check)
        validate_building_risk(
            risk_check,
            expected_rows,
            "BUILDING RISK PERSISTED",
        )
        print_risk_distribution(risk_check)

        print("\n" + "=" * 60)
        print("BUILDING RISK COMPLETED")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
