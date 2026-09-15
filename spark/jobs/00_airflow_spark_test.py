from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("AirflowSparkConnectivityTest")
    .master("local[2]")
    .getOrCreate()
)


print("=" * 60)
print("AIRFLOW -> SPARK TEST")
print(f"Spark version: {spark.version}")
print("=" * 60)


df = spark.range(0, 1000)

row_count = df.count()

print(f"Spark test row count: {row_count}")


if row_count != 1000:
    raise ValueError(
        f"Unexpected row count: {row_count}"
    )


print("SUCCESS: Spark job completed correctly.")


spark.stop()