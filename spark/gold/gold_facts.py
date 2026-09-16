import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from spark_session import create_spark_session
from minio_config import minio_path


# ============================================================
# NORMALIZATION HELPERS
# ============================================================


def normalize_full_address(column):
    value = F.upper(F.trim(column))
    value = F.regexp_replace(value, r"[^A-Z0-9 ]", " ")

    value = F.regexp_replace(value, r"\bWEST\b", "W")
    value = F.regexp_replace(value, r"\bEAST\b", "E")
    value = F.regexp_replace(value, r"\bNORTH\b", "N")
    value = F.regexp_replace(value, r"\bSOUTH\b", "S")

    value = F.regexp_replace(value, r"\bSTREET\b", "ST")
    value = F.regexp_replace(value, r"\bAVENUE\b", "AVE")
    value = F.regexp_replace(value, r"\bBOULEVARD\b", "BLVD")
    value = F.regexp_replace(value, r"\bROAD\b", "RD")
    value = F.regexp_replace(value, r"\bDRIVE\b", "DR")
    value = F.regexp_replace(value, r"\bPLACE\b", "PL")
    value = F.regexp_replace(value, r"\bCOURT\b", "CT")
    value = F.regexp_replace(value, r"\bLANE\b", "LN")
    value = F.regexp_replace(value, r"\bPARKWAY\b", "PKWY")
    value = F.regexp_replace(value, r"\bTERRACE\b", "TER")

    return F.trim(F.regexp_replace(value, r"\s+", " "))


def normalize_borough(column):
    value = F.upper(F.trim(column))
    return (
        F.when(value.isin("1", "MN", "MANHATTAN"), "MANHATTAN")
        .when(value.isin("2", "BX", "BRONX"), "BRONX")
        .when(value.isin("3", "BK", "BROOKLYN"), "BROOKLYN")
        .when(value.isin("4", "QN", "QUEENS"), "QUEENS")
        .when(value.isin("5", "SI", "STATEN ISLAND"), "STATEN ISLAND")
        .otherwise(value)
    )


def write_parquet(df, path, label):
    df.write.mode("overwrite").parquet(path)
    print(f"{label} saved: {path}")


def count_fk_orphans(df, key_name, dim_df):
    return (
        df
        .filter(F.col(key_name).isNotNull())
        .select(key_name)
        .distinct()
        .join(
            dim_df.select(key_name).distinct(),
            on=key_name,
            how="left_anti",
        )
        .count()
    )


# ============================================================
# NYC 311 RESOLUTION
# Rebuilds all resolution intermediates from current Silver +
# current Building Identity before fact_311_event is created.
# ============================================================


def build_311_resolution(spark, nyc311_df, building_identity_df):
    print("\n" + "=" * 60)
    print("311 BUILDING RESOLUTION")
    print("=" * 60)

    nyc311 = nyc311_df.withColumn(
        "bbl",
        F.col("bbl").cast("string"),
    )

    identity = (
        building_identity_df
        .withColumn("bin", F.col("bin").cast("string"))
        .withColumn("current_bbl", F.col("current_bbl").cast("string"))
        .withColumn("resolved_bbl", F.col("resolved_bbl").cast("string"))
    )

    empty_string_array = F.expr("CAST(array() AS array<string>)")

    # ------------------------------------------------------------
    # 1. UNIQUE BBL -> BIN
    # ------------------------------------------------------------
    building_bbl_ref = (
        identity
        .withColumn(
            "all_bbls",
            F.array_distinct(
                F.concat(
                    F.array("current_bbl", "resolved_bbl"),
                    F.coalesce(F.col("bbl_aliases"), empty_string_array),
                )
            ),
        )
        .withColumn("bbl", F.explode_outer("all_bbls"))
        .select("bin", F.col("bbl").cast("string").alias("bbl"))
        .filter(
            F.col("bbl").isNotNull()
            & (F.trim(F.col("bbl")) != "")
        )
        .dropDuplicates(["bin", "bbl"])
    )

    bbl_bin_stats = (
        building_bbl_ref
        .groupBy("bbl")
        .agg(
            F.countDistinct("bin").alias("bin_count"),
            F.first("bin").alias("single_bin"),
        )
    )

    unique_bbl_to_bin = (
        bbl_bin_stats
        .filter(F.col("bin_count") == 1)
        .select("bbl", F.col("single_bin").alias("resolved_bin"))
    )

    bbl_ref_path = minio_path(
        "gold/building_risk/intermediate/bbl_to_bin_reference"
    )
    write_parquet(unique_bbl_to_bin, bbl_ref_path, "BBL -> BIN reference")

    nyc311_bbl_resolved = (
        nyc311
        .filter(
            F.col("bbl").isNotNull()
            & (F.trim(F.col("bbl")) != "")
        )
        .join(unique_bbl_to_bin, on="bbl", how="inner")
        .withColumn("match_method", F.lit("UNIQUE_BBL"))
        .withColumn("match_confidence", F.lit("HIGH"))
        .withColumn("resolution_status", F.lit("RESOLVED"))
    )

    unique_bbl_path = minio_path(
        "gold/building_risk/intermediate/311_unique_bbl_resolved"
    )
    write_parquet(
        nyc311_bbl_resolved,
        unique_bbl_path,
        "311 unique-BBL resolution",
    )

    nyc311_bbl_status = (
        nyc311
        .join(
            bbl_bin_stats.select("bbl", "bin_count"),
            on="bbl",
            how="left",
        )
        .withColumn(
            "bbl_resolution_reason",
            F.when(
                F.col("bbl").isNull() | (F.trim(F.col("bbl")) == ""),
                F.lit("MISSING_BBL"),
            )
            .when(F.col("bin_count") == 1, F.lit("UNIQUE_BBL"))
            .when(F.col("bin_count") > 1, F.lit("MULTI_BIN_BBL"))
            .otherwise(F.lit("BBL_NOT_FOUND")),
        )
    )

    nyc311_unresolved = nyc311_bbl_status.filter(
        F.col("bbl_resolution_reason") != "UNIQUE_BBL"
    )

    unresolved_bbl_path = minio_path(
        "gold/building_risk/intermediate/311_unresolved_after_bbl"
    )
    write_parquet(
        nyc311_unresolved,
        unresolved_bbl_path,
        "311 unresolved after BBL stage",
    )

    # ------------------------------------------------------------
    # 2. MULTI-BIN BBL + EXACT ADDRESS
    # ------------------------------------------------------------
    building_address_ref = (
        identity
        .withColumn(
            "all_bbls",
            F.array_distinct(
                F.concat(
                    F.array("current_bbl", "resolved_bbl"),
                    F.coalesce(F.col("bbl_aliases"), empty_string_array),
                )
            ),
        )
        .withColumn(
            "all_addresses",
            F.array_distinct(
                F.concat(
                    F.array("current_address"),
                    F.coalesce(F.col("address_aliases"), empty_string_array),
                )
            ),
        )
        .withColumn("candidate_bbl", F.explode_outer("all_bbls"))
        .withColumn("candidate_address", F.explode_outer("all_addresses"))
        .select(
            "bin",
            "borough",
            F.col("candidate_bbl").cast("string").alias("candidate_bbl"),
            "candidate_address",
        )
        .filter(
            F.col("candidate_bbl").isNotNull()
            & F.col("candidate_address").isNotNull()
            & (F.trim(F.col("candidate_address")) != "")
        )
        .withColumn(
            "normalized_address",
            normalize_full_address(F.col("candidate_address")),
        )
        .withColumn(
            "normalized_borough",
            normalize_borough(F.col("borough")),
        )
        .dropDuplicates(
            [
                "bin",
                "candidate_bbl",
                "normalized_address",
                "normalized_borough",
            ]
        )
    )

    nyc311_multi_bin = (
        nyc311_unresolved
        .filter(F.col("bbl_resolution_reason") == "MULTI_BIN_BBL")
        .filter(
            F.col("incident_address").isNotNull()
            & (F.trim(F.col("incident_address")) != "")
        )
        .withColumn(
            "normalized_address",
            normalize_full_address(F.col("incident_address")),
        )
        .withColumn(
            "normalized_borough",
            normalize_borough(F.col("borough")),
        )
    )

    multi_bbl_matches_raw = (
        nyc311_multi_bin.alias("n")
        .join(
            building_address_ref.alias("b"),
            (
                (F.col("n.bbl") == F.col("b.candidate_bbl"))
                & (
                    F.col("n.normalized_address")
                    == F.col("b.normalized_address")
                )
                & (
                    F.col("n.normalized_borough")
                    == F.col("b.normalized_borough")
                )
            ),
            how="inner",
        )
        .select(
            F.col("n.unique_key").alias("unique_key"),
            F.col("b.bin").alias("candidate_bin"),
        )
        .dropDuplicates()
    )

    multi_bbl_match_summary = (
        multi_bbl_matches_raw
        .groupBy("unique_key")
        .agg(
            F.countDistinct("candidate_bin").alias("candidate_bin_count"),
            F.first("candidate_bin").alias("resolved_bin"),
        )
    )

    multi_bbl_resolved_keys = (
        multi_bbl_match_summary
        .filter(F.col("candidate_bin_count") == 1)
        .select("unique_key", "resolved_bin")
    )

    nyc311_multi_bbl_resolved = (
        nyc311_unresolved
        .join(multi_bbl_resolved_keys, on="unique_key", how="inner")
        .withColumn("match_method", F.lit("BBL_EXACT_ADDRESS"))
        .withColumn("match_confidence", F.lit("HIGH"))
        .withColumn("resolution_status", F.lit("RESOLVED"))
    )

    nyc311_still_unresolved = (
        nyc311_unresolved
        .join(
            multi_bbl_resolved_keys.select("unique_key"),
            on="unique_key",
            how="left_anti",
        )
    )

    multi_bbl_path = minio_path(
        "gold/building_risk/intermediate/311_multi_bbl_address_resolved"
    )
    unresolved_multi_path = minio_path(
        "gold/building_risk/intermediate/311_unresolved_after_multi_bbl_address"
    )
    write_parquet(
        nyc311_multi_bbl_resolved,
        multi_bbl_path,
        "311 multi-BBL exact-address resolution",
    )
    write_parquet(
        nyc311_still_unresolved,
        unresolved_multi_path,
        "311 unresolved after multi-BBL address stage",
    )

    # ------------------------------------------------------------
    # 3. GLOBAL EXACT ADDRESS + BOROUGH
    # ------------------------------------------------------------
    building_global_address_ref = (
        identity
        .withColumn(
            "all_addresses",
            F.array_distinct(
                F.concat(
                    F.array("current_address"),
                    F.coalesce(F.col("address_aliases"), empty_string_array),
                )
            ),
        )
        .withColumn("candidate_address", F.explode_outer("all_addresses"))
        .filter(
            F.col("candidate_address").isNotNull()
            & (F.trim(F.col("candidate_address")) != "")
            & F.col("borough").isNotNull()
        )
        .select(
            "bin",
            normalize_full_address(F.col("candidate_address")).alias(
                "normalized_address"
            ),
            normalize_borough(F.col("borough")).alias("normalized_borough"),
        )
        .filter(
            F.col("normalized_address").isNotNull()
            & (F.col("normalized_address") != "")
            & F.col("normalized_borough").isNotNull()
        )
        .dropDuplicates(["bin", "normalized_address", "normalized_borough"])
    )

    global_address_stats = (
        building_global_address_ref
        .groupBy("normalized_address", "normalized_borough")
        .agg(
            F.countDistinct("bin").alias("candidate_bin_count"),
            F.first("bin").alias("resolved_bin"),
        )
    )

    unique_address_to_bin = (
        global_address_stats
        .filter(F.col("candidate_bin_count") == 1)
        .select("normalized_address", "normalized_borough", "resolved_bin")
    )

    nyc311_address_unresolved = (
        nyc311_still_unresolved
        .filter(
            F.col("incident_address").isNotNull()
            & (F.trim(F.col("incident_address")) != "")
        )
        .withColumn(
            "normalized_address",
            normalize_full_address(F.col("incident_address")),
        )
        .withColumn(
            "normalized_borough",
            normalize_borough(F.col("borough")),
        )
    )

    global_address_resolved_keys = (
        nyc311_address_unresolved.alias("n")
        .join(
            F.broadcast(unique_address_to_bin).alias("b"),
            (
                F.col("n.normalized_address")
                == F.col("b.normalized_address")
            )
            & (
                F.col("n.normalized_borough")
                == F.col("b.normalized_borough")
            ),
            how="inner",
        )
        .select(
            F.col("n.unique_key").alias("unique_key"),
            F.col("b.resolved_bin").alias("resolved_bin"),
        )
        .dropDuplicates(["unique_key"])
    )

    nyc311_global_address_resolved = (
        nyc311_still_unresolved
        .join(global_address_resolved_keys, on="unique_key", how="inner")
        .withColumn("match_method", F.lit("EXACT_ADDRESS_BOROUGH"))
        .withColumn("match_confidence", F.lit("HIGH"))
        .withColumn("resolution_status", F.lit("RESOLVED"))
    )

    nyc311_after_global_address = (
        nyc311_still_unresolved
        .join(
            global_address_resolved_keys.select("unique_key"),
            on="unique_key",
            how="left_anti",
        )
    )

    global_address_path = minio_path(
        "gold/building_risk/intermediate/311_global_address_resolved"
    )
    unresolved_address_path = minio_path(
        "gold/building_risk/intermediate/311_unresolved_after_address"
    )
    write_parquet(
        nyc311_global_address_resolved,
        global_address_path,
        "311 global exact-address resolution",
    )
    write_parquet(
        nyc311_after_global_address,
        unresolved_address_path,
        "311 unresolved after address stage",
    )

    # ------------------------------------------------------------
    # 4. GEO PROFILE (<= 100m candidates)
    # ------------------------------------------------------------
    geo_source_valid = (
        nyc311_after_global_address
        .filter(
            F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
        .withColumn(
            "normalized_borough",
            normalize_borough(F.col("borough")),
        )
        .withColumn("grid_lat", F.floor(F.col("latitude") * 1000))
        .withColumn("grid_lon", F.floor(F.col("longitude") * 1000))
    )

    building_geo_ref = (
        identity
        .filter(
            F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
        .select(
            F.col("bin").alias("candidate_bin"),
            F.col("resolved_bbl").alias("candidate_bbl"),
            F.col("current_address").alias("candidate_address"),
            normalize_borough(F.col("borough")).alias("normalized_borough"),
            F.col("latitude").alias("candidate_latitude"),
            F.col("longitude").alias("candidate_longitude"),
        )
        .withColumn(
            "grid_lat",
            F.floor(F.col("candidate_latitude") * 1000),
        )
        .withColumn(
            "grid_lon",
            F.floor(F.col("candidate_longitude") * 1000),
        )
    )

    offsets = spark.createDataFrame(
        [
            (-1, -1), (-1, 0), (-1, 1),
            (0, -1), (0, 0), (0, 1),
            (1, -1), (1, 0), (1, 1),
        ],
        ["lat_offset", "lon_offset"],
    )

    geo_source_expanded = (
        geo_source_valid
        .crossJoin(F.broadcast(offsets))
        .withColumn("join_grid_lat", F.col("grid_lat") + F.col("lat_offset"))
        .withColumn("join_grid_lon", F.col("grid_lon") + F.col("lon_offset"))
    )

    geo_candidates = (
        geo_source_expanded.alias("s")
        .join(
            building_geo_ref.alias("b"),
            (
                F.col("s.normalized_borough")
                == F.col("b.normalized_borough")
            )
            & (F.col("s.join_grid_lat") == F.col("b.grid_lat"))
            & (F.col("s.join_grid_lon") == F.col("b.grid_lon")),
            how="inner",
        )
        .select(
            F.col("s.unique_key").alias("unique_key"),
            F.col("s.bbl_resolution_reason").alias("bbl_resolution_reason"),
            F.col("s.latitude").alias("source_latitude"),
            F.col("s.longitude").alias("source_longitude"),
            F.col("b.candidate_bin"),
            F.col("b.candidate_bbl"),
            F.col("b.candidate_address"),
            F.col("b.candidate_latitude"),
            F.col("b.candidate_longitude"),
        )
    )

    lat1 = F.radians(F.col("source_latitude"))
    lon1 = F.radians(F.col("source_longitude"))
    lat2 = F.radians(F.col("candidate_latitude"))
    lon2 = F.radians(F.col("candidate_longitude"))

    haversine_a = (
        F.pow(F.sin((lat2 - lat1) / 2), 2)
        + F.cos(lat1)
        * F.cos(lat2)
        * F.pow(F.sin((lon2 - lon1) / 2), 2)
    )

    geo_candidates = (
        geo_candidates
        .withColumn(
            "distance_m",
            2 * F.lit(6371000.0) * F.asin(F.sqrt(haversine_a)),
        )
        .filter(F.col("distance_m") <= 100)
        .dropDuplicates(["unique_key", "candidate_bin"])
    )

    distance_window = (
        Window
        .partitionBy("unique_key")
        .orderBy(F.col("distance_m").asc())
    )

    ranked_geo_candidates = geo_candidates.withColumn(
        "geo_rank",
        F.row_number().over(distance_window),
    )

    geo_profile = (
        ranked_geo_candidates
        .groupBy("unique_key")
        .agg(
            F.countDistinct("candidate_bin").alias("candidate_count_100m"),
            F.max(
                F.when(F.col("geo_rank") == 1, F.col("candidate_bin"))
            ).alias("nearest_bin"),
            F.max(
                F.when(F.col("geo_rank") == 1, F.col("distance_m"))
            ).alias("nearest_distance_m"),
            F.max(
                F.when(F.col("geo_rank") == 2, F.col("distance_m"))
            ).alias("second_distance_m"),
        )
    )

    geo_profile_path = minio_path(
        "gold/building_risk/intermediate/311_geo_profile"
    )
    write_parquet(geo_profile, geo_profile_path, "311 GEO profile")

    # ------------------------------------------------------------
    # 5. CONSERVATIVE GEO AUTO-RESOLUTION
    # Notebook rules:
    #   A) exactly one candidate within 100m and nearest <= 25m
    #   B) nearest <= 10m and second-nearest gap >= 20m
    # ------------------------------------------------------------
    geo_resolution = (
        geo_profile
        .withColumn(
            "distance_gap_m",
            F.col("second_distance_m") - F.col("nearest_distance_m"),
        )
        .withColumn(
            "match_method",
            F.when(
                (F.col("candidate_count_100m") == 1)
                & (F.col("nearest_distance_m") <= 25),
                F.lit("GEO_UNIQUE_25M"),
            ).when(
                (F.col("nearest_distance_m") <= 10)
                & (F.col("distance_gap_m") >= 20),
                F.lit("GEO_DOMINANT_10M"),
            ),
        )
        .filter(F.col("match_method").isNotNull())
        .withColumn("resolved_bin", F.col("nearest_bin"))
        .withColumn("match_confidence", F.lit("HIGH"))
        .withColumn("resolution_status", F.lit("AUTO_RESOLVED"))
        .select(
            "unique_key",
            "resolved_bin",
            "match_method",
            "match_confidence",
            "resolution_status",
            "candidate_count_100m",
            "nearest_distance_m",
            "second_distance_m",
            "distance_gap_m",
        )
    )

    geo_resolution_path = minio_path(
        "gold/data_model/intermediate/311_geo_resolved"
    )
    write_parquet(
        geo_resolution,
        geo_resolution_path,
        "311 conservative GEO resolution",
    )

    # ------------------------------------------------------------
    # 6. UNIFIED RESOLUTION MAP
    # ------------------------------------------------------------
    resolution_columns = [
        "unique_key",
        "resolved_bin",
        "match_method",
        "match_confidence",
        "resolution_status",
    ]

    resolution_map = (
        nyc311_bbl_resolved.select(*resolution_columns)
        .unionByName(nyc311_multi_bbl_resolved.select(*resolution_columns))
        .unionByName(
            nyc311_global_address_resolved.select(*resolution_columns)
        )
        .unionByName(geo_resolution.select(*resolution_columns))
        .withColumn("resolved_bin", F.col("resolved_bin").cast("string"))
    )

    resolution_stats = resolution_map.agg(
        F.count("*").alias("rows"),
        F.countDistinct("unique_key").alias("distinct_keys"),
    ).first()

    total_311 = nyc311.count()
    unique_bbl_count = nyc311_bbl_resolved.count()
    multi_bbl_count = nyc311_multi_bbl_resolved.count()
    global_address_count = nyc311_global_address_resolved.count()
    geo_count = geo_resolution.count()
    unresolved_count = nyc311_after_global_address.join(
        geo_resolution.select("unique_key"),
        on="unique_key",
        how="left_anti",
    ).count()

    print("311 total:", f"{total_311:,}")
    print("Resolved UNIQUE_BBL:", f"{unique_bbl_count:,}")
    print("Resolved BBL_EXACT_ADDRESS:", f"{multi_bbl_count:,}")
    print("Resolved EXACT_ADDRESS_BOROUGH:", f"{global_address_count:,}")
    print("Resolved by GEO:", f"{geo_count:,}")
    print("Still unresolved:", f"{unresolved_count:,}")
    print("Resolution map rows:", f"{resolution_stats['rows']:,}")
    print("Resolution map distinct keys:", f"{resolution_stats['distinct_keys']:,}")

    if resolution_stats["rows"] != resolution_stats["distinct_keys"]:
        raise RuntimeError(
            "DQ FAILED: 311 resolution map contains duplicate unique_key values."
        )

    if resolution_stats["rows"] > total_311:
        raise RuntimeError(
            "DQ FAILED: 311 resolution map has more rows than Silver 311."
        )

    if (
        resolution_stats["rows"] + unresolved_count
        != total_311
    ):
        raise RuntimeError(
            "DQ FAILED: resolved + unresolved 311 rows do not equal Silver 311 rows."
        )

    return resolution_map


# ============================================================
# FACT_311_EVENT
# ============================================================


def build_fact_311(nyc311_df, resolution_map, dim_property_df, dim_building_df):
    nyc311_base = (
        nyc311_df
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
            F.col("bbl").cast("string").alias("source_bbl"),
            "latitude",
            "longitude",
            "community_board",
            "council_district",
            "location_type",
            "open_data_channel_type",
            "created_day",
            "created_year",
            "created_month",
        )
    )

    building_lookup = (
        dim_building_df
        .select(
            F.col("bin").cast("string").alias("resolved_bin"),
            "building_id",
            F.col("property_id").alias("building_property_id"),
        )
        .dropDuplicates(["resolved_bin"])
    )

    property_lookup = (
        dim_property_df
        .select(
            F.col("bbl").cast("string").alias("source_bbl"),
            F.col("property_id").alias("source_property_id"),
        )
        .dropDuplicates(["source_bbl"])
    )

    stage = (
        nyc311_base
        .join(resolution_map, on="unique_key", how="left")
        .join(building_lookup, on="resolved_bin", how="left")
        .join(property_lookup, on="source_bbl", how="left")
        .withColumn(
            "property_id",
            F.when(
                F.col("building_property_id").isNotNull(),
                F.col("building_property_id"),
            ).when(
                F.col("building_id").isNull()
                & F.col("source_property_id").isNotNull(),
                F.col("source_property_id"),
            ),
        )
        .withColumn(
            "property_resolution_method",
            F.when(
                F.col("building_property_id").isNotNull(),
                F.lit("BUILDING_CANONICAL"),
            )
            .when(
                F.col("building_id").isNull()
                & F.col("source_property_id").isNotNull(),
                F.lit("SOURCE_BBL"),
            )
            .otherwise(F.lit("UNRESOLVED")),
        )
        .withColumn(
            "property_conflict_flag",
            F.when(
                F.col("building_property_id").isNotNull()
                & F.col("source_property_id").isNotNull()
                & (
                    F.col("building_property_id")
                    != F.col("source_property_id")
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "building_resolution_status",
            F.when(
                F.col("building_id").isNotNull(),
                F.lit("RESOLVED"),
            ).otherwise(F.lit("UNRESOLVED")),
        )
    )

    return (
        stage
        .withColumn(
            "event_id",
            F.concat(F.lit("311:"), F.col("unique_key")),
        )
        .select(
            "event_id",
            "unique_key",
            "property_id",
            "building_id",
            "resolved_bin",
            "source_bbl",
            "source_property_id",
            "building_property_id",
            "match_method",
            "match_confidence",
            "resolution_status",
            "building_resolution_status",
            "property_resolution_method",
            "property_conflict_flag",
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
            "latitude",
            "longitude",
            "community_board",
            "council_district",
            "location_type",
            "open_data_channel_type",
            "created_day",
            "created_year",
            "created_month",
        )
    )


# ============================================================
# FACT_HPD_VIOLATION
# ============================================================


def build_fact_hpd(hpd_df, dim_property_df, dim_building_df):
    building_lookup = (
        dim_building_df
        .select(
            F.trim(F.col("bin").cast("string")).alias("source_bin"),
            "building_id",
            F.col("property_id").alias("building_property_id"),
        )
        .dropDuplicates(["source_bin"])
    )

    property_lookup = (
        dim_property_df
        .select(
            F.col("bbl").cast("string").alias("source_bbl_original"),
            F.col("property_id").alias("source_property_id"),
        )
        .dropDuplicates(["source_bbl_original"])
    )

    stage = (
        hpd_df
        .withColumn(
            "source_bin",
            F.trim(F.col("bin").cast("string")),
        )
        .withColumn(
            "source_bbl_original",
            F.col("bbl").cast("string"),
        )
        .join(building_lookup, on="source_bin", how="left")
        .join(property_lookup, on="source_bbl_original", how="left")
        .withColumn(
            "property_id",
            F.when(
                F.col("building_property_id").isNotNull(),
                F.col("building_property_id"),
            ).when(
                F.col("building_id").isNull()
                & F.col("source_property_id").isNotNull(),
                F.col("source_property_id"),
            ),
        )
        .withColumn(
            "property_resolution_method",
            F.when(
                F.col("building_property_id").isNotNull(),
                F.lit("BUILDING_CANONICAL"),
            )
            .when(
                F.col("building_id").isNull()
                & F.col("source_property_id").isNotNull(),
                F.lit("SOURCE_BBL"),
            )
            .otherwise(F.lit("UNRESOLVED")),
        )
        .withColumn(
            "property_conflict_flag",
            F.when(
                F.col("building_property_id").isNotNull()
                & F.col("source_property_id").isNotNull()
                & (
                    F.col("building_property_id")
                    != F.col("source_property_id")
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "building_resolution_status",
            F.when(
                F.col("building_id").isNotNull(),
                F.lit("RESOLVED"),
            ).otherwise(F.lit("UNRESOLVED")),
        )
    )

    return (
        stage
        .withColumn(
            "hpd_event_id",
            F.concat(F.lit("HPD:"), F.col("violationid")),
        )
        .select(
            "hpd_event_id",
            "violationid",
            "novid",
            "property_id",
            "building_id",
            "source_bin",
            "source_bbl_original",
            "source_property_id",
            "building_property_id",
            "property_resolution_method",
            "property_conflict_flag",
            "building_resolution_status",
            "buildingid",
            "registrationid",
            "class",
            "violationstatus",
            "currentstatus",
            "currentstatusid",
            "inspectiondate",
            "novissueddate",
            "currentstatusdate",
            "approveddate",
            "certifieddate",
            "originalcorrectbydate",
            "originalcertifybydate",
            "novtype",
            "novdescription",
            "rentimpairing",
            "boroid",
            "boro",
            "block",
            "lot",
            "housenumber",
            "lowhousenumber",
            "highhousenumber",
            "streetname",
            "apartment",
            "story",
            "zip",
            "latitude",
            "longitude",
            "communityboard",
            "councildistrict",
            "censustract",
            "nta",
            "bbl_source",
            "inspection_day",
            "inspection_year",
            "inspection_month",
        )
    )


# ============================================================
# FACT_DOB_VIOLATION
# ============================================================


def build_fact_dob(dob_df, dim_property_df, dim_building_df):
    building_lookup = (
        dim_building_df
        .select(
            F.trim(F.col("bin").cast("string")).alias("source_bin"),
            "building_id",
            F.col("property_id").alias("building_property_id"),
        )
        .dropDuplicates(["source_bin"])
    )

    property_lookup = (
        dim_property_df
        .select(
            F.col("bbl").cast("string").alias("source_bbl_original"),
            F.col("property_id").alias("source_property_id"),
        )
        .dropDuplicates(["source_bbl_original"])
    )

    stage = (
        dob_df
        .withColumn(
            "source_bin",
            F.trim(F.col("bin").cast("string")),
        )
        .withColumn(
            "source_bbl_original",
            F.col("bbl").cast("string"),
        )
        .join(building_lookup, on="source_bin", how="left")
        .join(property_lookup, on="source_bbl_original", how="left")
        .withColumn(
            "property_id",
            F.when(
                F.col("building_property_id").isNotNull(),
                F.col("building_property_id"),
            ).when(
                F.col("building_id").isNull()
                & F.col("source_property_id").isNotNull(),
                F.col("source_property_id"),
            ),
        )
        .withColumn(
            "property_resolution_method",
            F.when(
                F.col("building_property_id").isNotNull(),
                F.lit("BUILDING_CANONICAL"),
            )
            .when(
                F.col("building_id").isNull()
                & F.col("source_property_id").isNotNull(),
                F.lit("SOURCE_BBL"),
            )
            .otherwise(F.lit("UNRESOLVED")),
        )
        .withColumn(
            "property_conflict_flag",
            F.when(
                F.col("building_property_id").isNotNull()
                & F.col("source_property_id").isNotNull()
                & (
                    F.col("building_property_id")
                    != F.col("source_property_id")
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "building_resolution_status",
            F.when(
                F.col("building_id").isNotNull(),
                F.lit("RESOLVED"),
            ).otherwise(F.lit("UNRESOLVED")),
        )
    )

    return (
        stage
        .withColumn(
            "dob_event_id",
            F.concat(F.lit("DOB:"), F.col("violation_number")),
        )
        .select(
            "dob_event_id",
            "violation_number",
            "property_id",
            "building_id",
            "source_bin",
            "source_bbl_original",
            "source_property_id",
            "building_property_id",
            "property_resolution_method",
            "property_conflict_flag",
            "building_resolution_status",
            "violation_issue_date",
            "violation_status",
            "violation_type",
            "device_type",
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
            "neighborhood_tabulation_area_nta_2020_",
            "source_bbl",
            "bbl_source",
            "violation_day",
            "violation_year",
            "violation_month",
        )
    )


# ============================================================
# FACT VALIDATION
# ============================================================


def validate_fact(
    label,
    source_df,
    fact_df,
    source_pk,
    fact_pk,
    dim_property_df,
    dim_building_df,
):
    source_stats = source_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct(source_pk).alias("distinct_pk"),
        F.sum(F.when(F.col(source_pk).isNull(), 1).otherwise(0)).alias(
            "null_pk"
        ),
    ).first()

    fact_stats = fact_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct(fact_pk).alias("distinct_pk"),
        F.sum(F.when(F.col(fact_pk).isNull(), 1).otherwise(0)).alias(
            "null_pk"
        ),
        F.sum(F.when(F.col("building_id").isNotNull(), 1).otherwise(0)).alias(
            "with_building"
        ),
        F.sum(F.when(F.col("property_id").isNotNull(), 1).otherwise(0)).alias(
            "with_property"
        ),
        F.sum(
            F.when(F.col("property_conflict_flag") == 1, 1).otherwise(0)
        ).alias("property_conflicts"),
    ).first()

    building_orphans = count_fk_orphans(
        fact_df,
        "building_id",
        dim_building_df,
    )
    property_orphans = count_fk_orphans(
        fact_df,
        "property_id",
        dim_property_df,
    )

    print("\n" + "-" * 60)
    print(f"{label} DATA QUALITY")
    print("-" * 60)
    print("Source rows:", f"{source_stats['rows']:,}")
    print("Source distinct PK:", f"{source_stats['distinct_pk']:,}")
    print("Source null PK:", f"{source_stats['null_pk'] or 0:,}")
    print("Fact rows:", f"{fact_stats['rows']:,}")
    print("Fact distinct event ID:", f"{fact_stats['distinct_pk']:,}")
    print("Fact null event ID:", f"{fact_stats['null_pk'] or 0:,}")
    print("With building_id:", f"{fact_stats['with_building'] or 0:,}")
    print("With property_id:", f"{fact_stats['with_property'] or 0:,}")
    print(
        "Property conflicts:",
        f"{fact_stats['property_conflicts'] or 0:,}",
    )
    print("Building FK orphans:", building_orphans)
    print("Property FK orphans:", property_orphans)

    if source_stats["rows"] == 0:
        raise RuntimeError(f"DQ FAILED: {label} source is empty.")

    if (source_stats["null_pk"] or 0) != 0:
        raise RuntimeError(f"DQ FAILED: {label} source contains null primary keys.")

    if source_stats["rows"] != source_stats["distinct_pk"]:
        raise RuntimeError(f"DQ FAILED: {label} source primary key is not unique.")

    if fact_stats["rows"] != source_stats["rows"]:
        raise RuntimeError(
            f"DQ FAILED: {label} fact row count does not match its Silver source."
        )

    if (fact_stats["null_pk"] or 0) != 0:
        raise RuntimeError(f"DQ FAILED: {label} fact contains null event IDs.")

    if fact_stats["rows"] != fact_stats["distinct_pk"]:
        raise RuntimeError(f"DQ FAILED: {label} fact event ID is not unique.")

    if building_orphans != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} fact contains building_id values absent from dim_building."
        )

    if property_orphans != 0:
        raise RuntimeError(
            f"DQ FAILED: {label} fact contains property_id values absent from dim_property."
        )


# ============================================================
# MAIN
# ============================================================


def main():
    spark = create_spark_session("NYC Building Risk - Gold Facts")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n" + "=" * 60)
        print("GOLD FACTS PRODUCTION JOB")
        print("=" * 60)

        # Current conformed sources.
        nyc311_path = minio_path("silver/nyc_311")
        hpd_path = minio_path("silver/hpd")
        dob_path = minio_path("silver/dob")
        building_identity_path = minio_path("gold/building_identity/final")
        dim_property_path = minio_path("gold/data_model/dim_property")
        dim_building_path = minio_path("gold/data_model/dim_building")

        print("NYC 311:", nyc311_path)
        print("HPD:", hpd_path)
        print("DOB:", dob_path)
        print("Building Identity:", building_identity_path)
        print("dim_property:", dim_property_path)
        print("dim_building:", dim_building_path)

        nyc311_df = spark.read.parquet(nyc311_path)
        hpd_df = spark.read.parquet(hpd_path)
        dob_df = spark.read.parquet(dob_path)
        building_identity_df = spark.read.parquet(building_identity_path)
        dim_property_df = spark.read.parquet(dim_property_path)
        dim_building_df = spark.read.parquet(dim_building_path)

        # Rebuild 311 identity resolution from the current datasets.
        resolution_map = build_311_resolution(
            spark,
            nyc311_df,
            building_identity_df,
        )

        fact_311_event = build_fact_311(
            nyc311_df,
            resolution_map,
            dim_property_df,
            dim_building_df,
        )
        fact_hpd_violation = build_fact_hpd(
            hpd_df,
            dim_property_df,
            dim_building_df,
        )
        fact_dob_violation = build_fact_dob(
            dob_df,
            dim_property_df,
            dim_building_df,
        )

        fact_311_path = minio_path("gold/data_model/fact_311_event")
        fact_hpd_path = minio_path("gold/data_model/fact_hpd_violation")
        fact_dob_path = minio_path("gold/data_model/fact_dob_violation")

        write_parquet(fact_311_event, fact_311_path, "fact_311_event")
        write_parquet(fact_hpd_violation, fact_hpd_path, "fact_hpd_violation")
        write_parquet(fact_dob_violation, fact_dob_path, "fact_dob_violation")

        # Validate persisted outputs, not only in-memory DataFrames.
        fact_311_check = spark.read.parquet(fact_311_path)
        fact_hpd_check = spark.read.parquet(fact_hpd_path)
        fact_dob_check = spark.read.parquet(fact_dob_path)

        validate_fact(
            "FACT_311_EVENT",
            nyc311_df,
            fact_311_check,
            "unique_key",
            "event_id",
            dim_property_df,
            dim_building_df,
        )
        validate_fact(
            "FACT_HPD_VIOLATION",
            hpd_df,
            fact_hpd_check,
            "violationid",
            "hpd_event_id",
            dim_property_df,
            dim_building_df,
        )
        validate_fact(
            "FACT_DOB_VIOLATION",
            dob_df,
            fact_dob_check,
            "violation_number",
            "dob_event_id",
            dim_property_df,
            dim_building_df,
        )

        # Additional 311 consistency gate: every resolved BIN must resolve to dim_building.
        resolved_311_without_building = (
            fact_311_check
            .filter(
                F.col("resolved_bin").isNotNull()
                & F.col("building_id").isNull()
            )
            .count()
        )

        print("\n311 resolved BIN without building_id:", resolved_311_without_building)

        if resolved_311_without_building != 0:
            raise RuntimeError(
                "DQ FAILED: resolved 311 BIN values are missing from dim_building."
            )

        print("\n" + "=" * 60)
        print("GOLD FACTS COMPLETED")
        print("=" * 60)
        print("FACT_311_EVENT:", fact_311_path)
        print("FACT_HPD_VIOLATION:", fact_hpd_path)
        print("FACT_DOB_VIOLATION:", fact_dob_path)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
