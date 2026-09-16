import sys

from pyspark.sql import functions as F

PROJECT_ROOT = "/workspace/nyc-building-risk"
COMMON_PATH = f"{PROJECT_ROOT}/spark/common"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

from spark_session import create_spark_session
from minio_config import minio_path


def get_identity_pluto_version(building_identity_df):
    versions = [
        row["snapshot_version"]
        for row in (
            building_identity_df
            .filter(F.col("snapshot_version").isNotNull())
            .select("snapshot_version")
            .distinct()
            .collect()
        )
    ]

    if len(versions) != 1:
        raise RuntimeError(
            f"Expected exactly one PLUTO snapshot_version in Building Identity, found: {versions}"
        )

    return versions[0]


def count_property_orphans(df, property_keys):
    return (
        df
        .filter(F.col("property_id").isNotNull())
        .select("property_id")
        .join(property_keys, on="property_id", how="left_anti")
        .count()
    )


def count_building_orphans(df, building_keys):
    return (
        df
        .filter(F.col("building_id").isNotNull())
        .select("building_id")
        .join(building_keys, on="building_id", how="left_anti")
        .count()
    )


def main():
    spark = create_spark_session("NYC Building Risk - Gold Dimensions")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n" + "=" * 60)
        print("GOLD DIMENSIONS PRODUCTION JOB")
        print("=" * 60)

        building_identity_path = minio_path("gold/building_identity/final")
        building_identity_df = spark.read.parquet(building_identity_path)

        pluto_version = get_identity_pluto_version(building_identity_df)
        pluto_path = minio_path(f"silver/pluto/version={pluto_version}")
        pluto_df = spark.read.parquet(pluto_path)

        print("Building Identity path:", building_identity_path)
        print("PLUTO version:", pluto_version)
        print("PLUTO path:", pluto_path)

        # ============================================================
        # DIM_PROPERTY
        # Grain: 1 row = 1 Property / BBL
        # ============================================================

        dim_property = (
            pluto_df
            .filter(
                F.col("bbl").isNotNull()
                & (F.trim(F.col("bbl").cast("string")) != "")
            )
            .withColumn(
                "property_id",
                F.concat(
                    F.lit("PROP:"),
                    F.col("bbl").cast("string"),
                ),
            )
            .select(
                "property_id",
                "bbl",
                F.col("address").alias("property_address"),
                "borough",
                "zipcode",
                "latitude",
                "longitude",
                "landuse",
                "bldgclass",
                "yearbuilt",
                "yearalter1",
                "yearalter2",
                "numbldgs",
                "numfloors",
                "unitsres",
                "unitstotal",
                "lotarea",
                "bldgarea",
                "resarea",
                "comarea",
                "ownername",
                "ownertype",
                "snapshot_version",
            )
            .dropDuplicates(["property_id"])
        )

        dim_property_path = minio_path("gold/data_model/dim_property")
        dim_property.write.mode("overwrite").parquet(dim_property_path)
        print("dim_property saved:", dim_property_path)

        # ============================================================
        # DIM_BUILDING
        # Grain: 1 row = 1 physical Building / BIN
        # ============================================================

        dim_building = (
            building_identity_df
            .filter(
                F.col("bin").isNotNull()
                & (F.trim(F.col("bin").cast("string")) != "")
            )
            .withColumn(
                "building_id",
                F.concat(
                    F.lit("BLD:"),
                    F.col("bin").cast("string"),
                ),
            )
            .withColumn(
                "property_id",
                F.when(
                    F.col("resolved_bbl").isNotNull(),
                    F.concat(
                        F.lit("PROP:"),
                        F.col("resolved_bbl").cast("string"),
                    ),
                ),
            )
            .select(
                "building_id",
                "bin",
                "property_id",
                "resolved_bbl",
                "current_bbl",
                "current_address",
                "borough",
                "latitude",
                "longitude",
                "bbl_aliases",
                "address_aliases",
                "identity_status",
                "match_method",
                "match_confidence",
                "resolution_status",
            )
            .dropDuplicates(["building_id"])
        )

        dim_building_path = minio_path("gold/data_model/dim_building")
        dim_building.write.mode("overwrite").parquet(dim_building_path)
        print("dim_building saved:", dim_building_path)

        # ============================================================
        # BRIDGE_PROPERTY_BUILDING
        # Grain: 1 row = 1 canonical Property-Building relation
        # ============================================================

        bridge_property_building = (
            dim_building
            .filter(F.col("property_id").isNotNull())
            .select(
                "property_id",
                "building_id",
                F.col("resolved_bbl").alias("bbl"),
                "bin",
            )
            .withColumn(
                "relationship_type",
                F.lit("CANONICAL"),
            )
            .dropDuplicates(["property_id", "building_id"])
        )

        bridge_path = minio_path(
            "gold/data_model/bridge_property_building"
        )
        bridge_property_building.write.mode("overwrite").parquet(bridge_path)
        print("bridge_property_building saved:", bridge_path)

        # Reload written Gold datasets for validation.
        dim_property_df = spark.read.parquet(dim_property_path)
        dim_building_df = spark.read.parquet(dim_building_path)
        bridge_df = spark.read.parquet(bridge_path)

        identity_count = building_identity_df.count()

        dim_property_count = dim_property_df.count()
        dim_property_distinct = (
            dim_property_df.select("property_id").distinct().count()
        )
        dim_property_bbl_distinct = (
            dim_property_df.select("bbl").distinct().count()
        )

        dim_building_count = dim_building_df.count()
        dim_building_distinct = (
            dim_building_df.select("building_id").distinct().count()
        )
        buildings_with_property = (
            dim_building_df
            .filter(F.col("property_id").isNotNull())
            .count()
        )
        buildings_without_property = (
            dim_building_df
            .filter(F.col("property_id").isNull())
            .count()
        )

        bridge_count = bridge_df.count()
        bridge_distinct_pairs = (
            bridge_df
            .select("property_id", "building_id")
            .distinct()
            .count()
        )
        bridge_distinct_buildings = (
            bridge_df.select("building_id").distinct().count()
        )
        bridge_distinct_properties = (
            bridge_df.select("property_id").distinct().count()
        )

        property_keys = dim_property_df.select("property_id").distinct()
        building_keys = dim_building_df.select("building_id").distinct()

        dim_building_property_orphans = count_property_orphans(
            dim_building_df,
            property_keys,
        )
        bridge_property_orphans = count_property_orphans(
            bridge_df,
            property_keys,
        )
        bridge_building_orphans = count_building_orphans(
            bridge_df,
            building_keys,
        )

        print("\n" + "=" * 60)
        print("GOLD DIMENSIONS DATA QUALITY")
        print("=" * 60)
        print("PLUTO version:", pluto_version)
        print()
        print("Building Identity rows:", f"{identity_count:,}")
        print()
        print("dim_property rows:", f"{dim_property_count:,}")
        print("dim_property distinct property_id:", f"{dim_property_distinct:,}")
        print("dim_property distinct BBL:", f"{dim_property_bbl_distinct:,}")
        print()
        print("dim_building rows:", f"{dim_building_count:,}")
        print("dim_building distinct building_id:", f"{dim_building_distinct:,}")
        print("Buildings with property_id:", f"{buildings_with_property:,}")
        print("Buildings without property_id:", f"{buildings_without_property:,}")
        print()
        print("Bridge rows:", f"{bridge_count:,}")
        print("Bridge distinct pairs:", f"{bridge_distinct_pairs:,}")
        print("Bridge distinct buildings:", f"{bridge_distinct_buildings:,}")
        print("Bridge distinct properties:", f"{bridge_distinct_properties:,}")
        print()
        print(
            "dim_building property orphans:",
            dim_building_property_orphans,
        )
        print("Bridge property orphans:", bridge_property_orphans)
        print("Bridge building orphans:", bridge_building_orphans)

        # ============================================================
        # HARD DATA QUALITY GATES
        # ============================================================

        if dim_property_count != dim_property_distinct:
            raise RuntimeError(
                "DQ FAILED: dim_property contains duplicate property_id values."
            )

        if dim_property_count != dim_property_bbl_distinct:
            raise RuntimeError(
                "DQ FAILED: dim_property is not unique by BBL."
            )

        if dim_building_count != dim_building_distinct:
            raise RuntimeError(
                "DQ FAILED: dim_building contains duplicate building_id values."
            )

        if dim_building_count != identity_count:
            raise RuntimeError(
                "DQ FAILED: dim_building row count does not match Building Identity."
            )

        if bridge_count != bridge_distinct_pairs:
            raise RuntimeError(
                "DQ FAILED: bridge contains duplicate Property-Building pairs."
            )

        if bridge_count != buildings_with_property:
            raise RuntimeError(
                "DQ FAILED: bridge row count does not match buildings with property_id."
            )

        if bridge_distinct_buildings != bridge_count:
            raise RuntimeError(
                "DQ FAILED: a building appears in more than one canonical bridge relation."
            )

        if dim_building_property_orphans != 0:
            raise RuntimeError(
                "DQ FAILED: dim_building contains property_id values absent from dim_property."
            )

        if bridge_property_orphans != 0:
            raise RuntimeError(
                "DQ FAILED: bridge contains property_id values absent from dim_property."
            )

        if bridge_building_orphans != 0:
            raise RuntimeError(
                "DQ FAILED: bridge contains building_id values absent from dim_building."
            )

        print()
        print("DIM_PROPERTY:", dim_property_path)
        print("DIM_BUILDING:", dim_building_path)
        print("BRIDGE:", bridge_path)
        print("GOLD DIMENSIONS COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
