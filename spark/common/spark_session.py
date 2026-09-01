from pyspark.sql import SparkSession

from minio_config import configure_minio


# --------------------------------------------------
# 1. Create SparkSession
# --------------------------------------------------

def create_spark_session(
    app_name="NYC Building Risk"
):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.session.timeZone",
            "UTC"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .getOrCreate()
    )


    # --------------------------------------------------
    # 2. Configure connection to MinIO
    # --------------------------------------------------

    configure_minio(
        spark
    )


    # --------------------------------------------------
    # 3. Reduce unnecessary Spark console output
    # --------------------------------------------------

    spark.sparkContext.setLogLevel(
        "WARN"
    )


    return spark