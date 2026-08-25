import os

from dotenv import load_dotenv
from minio import Minio


# Load variables from .env
load_dotenv()


# Read MinIO configuration
endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("MINIO_BUCKET")


# Create connection to MinIO
client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)


# Check whether the project bucket exists
if client.bucket_exists(bucket_name):
    print(f"SUCCESS: Bucket '{bucket_name}' exists.")
    print("Python -> MinIO connection works.")
else:
    print(f"ERROR: Bucket '{bucket_name}' was not found.")