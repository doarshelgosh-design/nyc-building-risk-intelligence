import os
from pathlib import Path

from dotenv import load_dotenv


# --------------------------------------------------
# 1. Locate project root and load .env
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


# --------------------------------------------------
# 2. Read MinIO configuration
# --------------------------------------------------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

MINIO_SECURE = (
    os.getenv("MINIO_SECURE", "false").lower() == "true"
)


# --------------------------------------------------
# 3. Validate required variables
# --------------------------------------------------

required_variables = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_BUCKET": MINIO_BUCKET,
}

missing_variables = [
    name
    for name, value in required_variables.items()
    if not value
]

if missing_variables:
    raise ValueError(
        "Missing MinIO environment variables: "
        + ", ".join(missing_variables)
    )


# --------------------------------------------------
# 4. Convert localhost endpoint for Docker
# --------------------------------------------------

spark_endpoint = MINIO_ENDPOINT

if spark_endpoint.startswith("localhost"):
    spark_endpoint = spark_endpoint.replace(
        "localhost",
        "host.docker.internal",
        1
    )

elif spark_endpoint.startswith("127.0.0.1"):
    spark_endpoint = spark_endpoint.replace(
        "127.0.0.1",
        "host.docker.internal",
        1
    )


protocol = (
    "https"
    if MINIO_SECURE
    else "http"
)

MINIO_SPARK_ENDPOINT = (
    f"{protocol}://{spark_endpoint}"
)


# --------------------------------------------------
# 5. Configure Spark Hadoop S3A for MinIO
# --------------------------------------------------

def configure_minio(spark):

    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    hadoop_conf.set(
        "fs.s3a.endpoint",
        MINIO_SPARK_ENDPOINT
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        MINIO_ACCESS_KEY
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        MINIO_SECRET_KEY
    )

    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true"
    )

    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        str(MINIO_SECURE).lower()
    )

    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    return spark


# --------------------------------------------------
# 6. Build S3A paths
# --------------------------------------------------

def minio_path(object_path=""):

    object_path = object_path.lstrip("/")

    if object_path:
        return (
            f"s3a://{MINIO_BUCKET}/"
            f"{object_path}"
        )

    return f"s3a://{MINIO_BUCKET}"