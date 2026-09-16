import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from spark_session import create_spark_session
from minio_config import minio_path


def normalize_address(column):
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.upper(column),
                r"[^A-Z0-9 ]",
                " ",
            ),
            r"\s+",
            " ",
        )
    )


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


def path_exists(spark, path):
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return filesystem.exists(hadoop_path)


def get_latest_pluto_version(spark):
    success_pattern = minio_path("silver/pluto/version=*/_SUCCESS")
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(success_pattern)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    success_files = filesystem.globStatus(hadoop_path)

    if not success_files:
        raise RuntimeError("No successful PLUTO Silver snapshot was found.")

    latest_success = max(
        success_files,
        key=lambda file_status: file_status.getModificationTime(),
    )
    latest_success_path = latest_success.getPath().toString()
    return latest_success_path.split("version=", 1)[1].split("/", 1)[0]


def main():
    spark = create_spark_session("NYC Building Risk - Building Identity")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n" + "=" * 60)
        print("BUILDING IDENTITY PRODUCTION JOB")
        print("=" * 60)

        pluto_version = get_latest_pluto_version(spark)
        pluto_path = minio_path(f"silver/pluto/version={pluto_version}")
        hpd_path = minio_path("silver/hpd")
        dob_path = minio_path("silver/dob")

        print("PLUTO version:", pluto_version)
        print("PLUTO path:", pluto_path)
        print("HPD path:", hpd_path)
        print("DOB path:", dob_path)

        pluto_df = spark.read.parquet(pluto_path)
        hpd_df = spark.read.parquet(hpd_path)
        dob_df = spark.read.parquet(dob_path)

        hpd_buildings = (
            hpd_df
            .filter(
                F.col("bin").isNotNull()
                & (F.trim(F.col("bin").cast("string")) != "")
            )
            .select(
                F.trim(F.col("bin").cast("string")).alias("bin"),
                F.col("bbl"),
                F.trim(
                    F.concat_ws(" ", F.col("housenumber"), F.col("streetname"))
                ).alias("address"),
                normalize_borough(F.col("boro")).alias("borough"),
                F.col("latitude"),
                F.col("longitude"),
                F.col("inspectiondate").alias("event_date"),
            )
            .groupBy("bin", "bbl", "address", "borough", "latitude", "longitude")
            .agg(F.max("event_date").alias("event_date"))
            .withColumn("source", F.lit("HPD"))
        )

        dob_buildings = (
            dob_df
            .filter(
                F.col("bin").isNotNull()
                & (F.trim(F.col("bin").cast("string")) != "")
            )
            .select(
                F.trim(F.col("bin").cast("string")).alias("bin"),
                F.col("bbl"),
                F.trim(
                    F.concat_ws(" ", F.col("house_number"), F.col("street"))
                ).alias("address"),
                normalize_borough(F.col("borough")).alias("borough"),
                F.col("latitude"),
                F.col("longitude"),
                F.col("violation_issue_date").alias("event_date"),
            )
            .groupBy("bin", "bbl", "address", "borough", "latitude", "longitude")
            .agg(F.max("event_date").alias("event_date"))
            .withColumn("source", F.lit("DOB"))
        )

        building_observations = hpd_buildings.unionByName(dob_buildings)
        observations_path = minio_path(
            "gold/building_identity/intermediate/building_observations"
        )
        building_observations.write.mode("overwrite").parquet(observations_path)
        print("Building observations:", building_observations.count())

        obs_df = spark.read.parquet(observations_path)

        latest_window = (
            Window.partitionBy("bin")
            .orderBy(
                F.col("event_date").desc_nulls_last(),
                F.col("source").asc(),
                F.col("bbl").asc_nulls_last(),
                F.col("address").asc_nulls_last(),
            )
        )

        latest_building = (
            obs_df
            .withColumn("rn", F.row_number().over(latest_window))
            .filter(F.col("rn") == 1)
            .select(
                "bin",
                F.col("bbl").alias("current_bbl"),
                F.col("address").alias("current_address"),
                "borough",
                "latitude",
                "longitude",
                F.col("event_date").alias("latest_event_date"),
                F.col("source").alias("latest_source"),
            )
        )

        building_history = (
            obs_df
            .groupBy("bin")
            .agg(
                F.collect_set("bbl").alias("bbl_aliases"),
                F.collect_set("address").alias("address_aliases"),
                F.collect_set("source").alias("sources"),
                F.countDistinct("bbl").alias("bbl_count"),
                F.countDistinct("address").alias("address_count"),
            )
        )

        building_master = (
            latest_building
            .join(building_history, on="bin", how="left")
            .withColumn("building_key", F.concat(F.lit("BIN:"), F.col("bin")))
            .withColumn(
                "identity_status",
                F.when(F.col("bbl_count") > 1, F.lit("BBL_HISTORY"))
                .when(F.col("address_count") > 1, F.lit("ADDRESS_ALIAS"))
                .otherwise(F.lit("STABLE")),
            )
        )

        building_master_path = minio_path("gold/building_identity/building_master")
        building_master.write.mode("overwrite").parquet(building_master_path)
        building_master_df = spark.read.parquet(building_master_path)

        print("Canonical buildings:", building_master_df.count())
        (
            building_master_df.groupBy("identity_status")
            .count()
            .orderBy("identity_status")
            .show(truncate=False)
        )

        pluto_bbl_ref = (
            pluto_df
            .select(F.col("bbl").alias("pluto_bbl"))
            .filter(F.col("pluto_bbl").isNotNull())
            .dropDuplicates()
        )

        direct_resolved = (
            building_master_df
            .join(
                pluto_bbl_ref,
                building_master_df["current_bbl"] == pluto_bbl_ref["pluto_bbl"],
                how="inner",
            )
            .select("bin", F.col("current_bbl").alias("resolved_bbl"))
            .withColumn("match_method", F.lit("DIRECT_BBL"))
            .withColumn("match_confidence", F.lit("HIGH"))
            .withColumn("resolution_status", F.lit("RESOLVED"))
        )

        direct_unmatched = (
            building_master_df
            .join(
                pluto_bbl_ref,
                building_master_df["current_bbl"] == pluto_bbl_ref["pluto_bbl"],
                how="left_anti",
            )
        )

        direct_unmatched_path = minio_path(
            "gold/building_identity/intermediate/direct_bbl_unmatched"
        )
        direct_unmatched.write.mode("overwrite").parquet(direct_unmatched_path)
        direct_unmatched_df = spark.read.parquet(direct_unmatched_path)

        print("Direct BBL resolved:", direct_resolved.count())
        print("Direct BBL unmatched:", direct_unmatched_df.count())

        alias_candidates = (
            direct_unmatched_df
            .select(
                "bin",
                "current_bbl",
                F.explode_outer("bbl_aliases").alias("alias_bbl"),
            )
            .filter(F.col("alias_bbl").isNotNull())
        )

        alias_matches_raw = (
            alias_candidates
            .join(
                pluto_bbl_ref,
                F.col("alias_bbl") == F.col("pluto_bbl"),
                how="inner",
            )
        )

        alias_match_summary = (
            alias_matches_raw
            .groupBy("bin")
            .agg(
                F.collect_set("pluto_bbl").alias("matched_alias_bbls"),
                F.countDistinct("pluto_bbl").alias("alias_match_count"),
            )
        )

        alias_recovered_keys = (
            alias_match_summary
            .filter(F.col("alias_match_count") == 1)
            .select(
                "bin",
                F.element_at(F.col("matched_alias_bbls"), 1).alias("resolved_bbl"),
            )
        )

        alias_recovered = (
            direct_unmatched_df
            .join(alias_recovered_keys, on="bin", how="inner")
            .withColumn("match_method", F.lit("BBL_HISTORY"))
            .withColumn("match_confidence", F.lit("HIGH"))
        )

        unresolved_after_alias = (
            direct_unmatched_df
            .join(alias_recovered_keys.select("bin"), on="bin", how="left_anti")
        )

        alias_recovered_path = minio_path(
            "gold/building_identity/intermediate/bbl_history_recovered"
        )
        unresolved_alias_path = minio_path(
            "gold/building_identity/intermediate/unresolved_after_bbl_history"
        )

        alias_recovered.write.mode("overwrite").parquet(alias_recovered_path)
        unresolved_after_alias.write.mode("overwrite").parquet(unresolved_alias_path)

        alias_recovered_df = spark.read.parquet(alias_recovered_path)
        unresolved_after_alias_df = spark.read.parquet(unresolved_alias_path)

        print("Recovered by BBL history:", alias_recovered_df.count())
        print("Still unresolved after BBL history:", unresolved_after_alias_df.count())

        unresolved_address_keys = (
            unresolved_after_alias_df
            .withColumn(
                "all_addresses",
                F.array_distinct(
                    F.concat(F.array(F.col("current_address")), F.col("address_aliases"))
                ),
            )
            .select(
                "bin",
                normalize_borough(F.col("borough")).alias("normalized_borough"),
                F.explode_outer("all_addresses").alias("source_address"),
            )
            .filter(F.col("source_address").isNotNull())
            .withColumn("normalized_address", normalize_address(F.col("source_address")))
            .filter(F.col("normalized_address") != "")
            .dropDuplicates(["bin", "normalized_borough", "normalized_address"])
        )

        pluto_address_ref = (
            pluto_df
            .select(
                F.col("bbl").alias("pluto_bbl"),
                normalize_borough(F.col("borough")).alias("normalized_borough"),
                normalize_address(F.col("address")).alias("normalized_address"),
            )
            .filter(
                F.col("pluto_bbl").isNotNull()
                & F.col("normalized_address").isNotNull()
                & (F.col("normalized_address") != "")
            )
            .dropDuplicates(["pluto_bbl", "normalized_borough", "normalized_address"])
        )

        exact_address_matches_raw = (
            pluto_address_ref
            .join(
                F.broadcast(unresolved_address_keys),
                on=["normalized_borough", "normalized_address"],
                how="inner",
            )
        )

        exact_address_summary = (
            exact_address_matches_raw
            .groupBy("bin")
            .agg(
                F.collect_set("pluto_bbl").alias("matched_pluto_bbls"),
                F.countDistinct("pluto_bbl").alias("address_match_count"),
            )
        )

        exact_recovered_keys = (
            exact_address_summary
            .filter(F.col("address_match_count") == 1)
            .select(
                "bin",
                F.element_at(F.col("matched_pluto_bbls"), 1).alias("resolved_bbl"),
            )
        )

        exact_ambiguous = exact_address_summary.filter(F.col("address_match_count") > 1)

        exact_address_recovered = (
            unresolved_after_alias_df
            .join(exact_recovered_keys, on="bin", how="inner")
            .withColumn("match_method", F.lit("EXACT_ADDRESS"))
            .withColumn("match_confidence", F.lit("HIGH"))
        )

        still_unresolved = (
            unresolved_after_alias_df
            .join(exact_recovered_keys.select("bin"), on="bin", how="left_anti")
        )

        exact_recovered_path = minio_path(
            "gold/building_identity/intermediate/exact_address_recovered"
        )
        exact_ambiguous_path = minio_path(
            "gold/building_identity/intermediate/exact_address_ambiguous"
        )
        unresolved_path = minio_path("gold/building_identity/unresolved")

        exact_address_recovered.write.mode("overwrite").parquet(exact_recovered_path)
        exact_ambiguous.write.mode("overwrite").parquet(exact_ambiguous_path)
        still_unresolved.write.mode("overwrite").parquet(unresolved_path)

        exact_recovered_df = spark.read.parquet(exact_recovered_path)
        still_unresolved_df = spark.read.parquet(unresolved_path)

        print("Recovered by exact address:", exact_recovered_df.count())
        print("Still unresolved after exact address:", still_unresolved_df.count())

        resolver_path = minio_path("gold/building_identity/elasticsearch_resolver")

        if path_exists(spark, resolver_path):
            resolver_df = spark.read.parquet(resolver_path)
            resolver_candidates = (
                resolver_df
                .filter(F.col("resolution_status") == "AUTO_RESOLVED")
                .select("bin", "candidate_bbl")
                .join(still_unresolved_df.select("bin"), on="bin", how="inner")
                .join(
                    pluto_bbl_ref,
                    F.col("candidate_bbl") == F.col("pluto_bbl"),
                    how="inner",
                )
            )

            resolver_summary = (
                resolver_candidates
                .groupBy("bin")
                .agg(
                    F.collect_set("candidate_bbl").alias("candidate_bbls"),
                    F.countDistinct("candidate_bbl").alias("candidate_count"),
                )
            )

            elastic_resolved = (
                resolver_summary
                .filter(F.col("candidate_count") == 1)
                .select(
                    "bin",
                    F.element_at(F.col("candidate_bbls"), 1).alias("resolved_bbl"),
                )
                .withColumn("match_method", F.lit("ELASTICSEARCH"))
                .withColumn("match_confidence", F.lit("HIGH"))
                .withColumn("resolution_status", F.lit("RESOLVED"))
            )
            print("Recovered by existing ES resolver:", elastic_resolved.count())
        else:
            elastic_resolved = direct_resolved.limit(0)
            print("Elasticsearch resolver not found; continuing without it.")

        alias_resolved = (
            alias_recovered_df
            .select("bin", "resolved_bbl")
            .withColumn("match_method", F.lit("BBL_HISTORY"))
            .withColumn("match_confidence", F.lit("HIGH"))
            .withColumn("resolution_status", F.lit("RESOLVED"))
        )

        exact_resolved = (
            exact_recovered_df
            .select("bin", "resolved_bbl")
            .withColumn("match_method", F.lit("EXACT_ADDRESS"))
            .withColumn("match_confidence", F.lit("HIGH"))
            .withColumn("resolution_status", F.lit("RESOLVED"))
        )

        resolved_map_raw = (
            direct_resolved
            .unionByName(alias_resolved)
            .unionByName(exact_resolved)
            .unionByName(elastic_resolved)
        )

        method_priority = (
            F.when(F.col("match_method") == "DIRECT_BBL", 1)
            .when(F.col("match_method") == "BBL_HISTORY", 2)
            .when(F.col("match_method") == "EXACT_ADDRESS", 3)
            .otherwise(4)
        )

        resolution_window = Window.partitionBy("bin").orderBy(method_priority.asc())

        resolved_map = (
            resolved_map_raw
            .withColumn("resolution_rank", F.row_number().over(resolution_window))
            .filter(F.col("resolution_rank") == 1)
            .drop("resolution_rank")
        )

        print("Resolved buildings:", resolved_map.count())
        (
            resolved_map.groupBy("match_method")
            .count()
            .orderBy("match_method")
            .show(truncate=False)
        )

        building_identity_final = (
            building_master_df
            .join(resolved_map, on="bin", how="left")
            .withColumn(
                "resolution_status",
                F.coalesce(F.col("resolution_status"), F.lit("UNRESOLVED")),
            )
            .withColumn(
                "match_method",
                F.coalesce(F.col("match_method"), F.lit("UNRESOLVED")),
            )
            .withColumn(
                "match_confidence",
                F.coalesce(F.col("match_confidence"), F.lit("NONE")),
            )
        )

        pluto_gold_ref = (
            pluto_df
            .select(
                F.col("bbl").alias("resolved_bbl"),
                F.col("address").alias("pluto_address"),
                F.col("borough").alias("pluto_borough"),
                F.col("latitude").alias("pluto_latitude"),
                F.col("longitude").alias("pluto_longitude"),
                "yearbuilt",
                "landuse",
                "bldgclass",
                "numbldgs",
                "numfloors",
                "unitsres",
                "unitstotal",
                "lotarea",
                "bldgarea",
                "ownername",
                "ownertype",
                "zipcode",
                "snapshot_version",
            )
            .dropDuplicates(["resolved_bbl"])
        )

        building_identity_gold = building_identity_final.join(
            pluto_gold_ref, on="resolved_bbl", how="left"
        )

        final_gold_path = minio_path("gold/building_identity/final")
        building_identity_gold.write.mode("overwrite").parquet(final_gold_path)

        final_gold_df = spark.read.parquet(final_gold_path)
        total_buildings = final_gold_df.count()
        distinct_bins = final_gold_df.select("bin").distinct().count()
        resolved_buildings = (
            final_gold_df.filter(F.col("resolution_status") == "RESOLVED").count()
        )
        unresolved_buildings = (
            final_gold_df.filter(F.col("resolution_status") == "UNRESOLVED").count()
        )

        if total_buildings == 0:
            raise RuntimeError("Building Identity is empty.")
        if total_buildings != distinct_bins:
            raise RuntimeError("Building Identity grain violation: BIN is not unique.")

        resolution_coverage = resolved_buildings / total_buildings * 100
        if resolution_coverage < 95:
            raise RuntimeError(
                f"Building Identity resolution coverage is unexpectedly low: "
                f"{resolution_coverage:.2f}%"
            )

        print("\n" + "=" * 60)
        print("BUILDING IDENTITY DATA QUALITY")
        print("=" * 60)
        print(f"PLUTO version:       {pluto_version}")
        print(f"Total buildings:     {total_buildings:,}")
        print(f"Distinct BIN:        {distinct_bins:,}")
        print(f"Resolved:            {resolved_buildings:,}")
        print(f"Unresolved:          {unresolved_buildings:,}")
        print(f"Resolution coverage: {resolution_coverage:.2f}%")

        (
            final_gold_df
            .groupBy("resolution_status", "match_method")
            .count()
            .orderBy("resolution_status", "match_method")
            .show(100, truncate=False)
        )

        print("Final path:", final_gold_path)
        print("BUILDING IDENTITY COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
