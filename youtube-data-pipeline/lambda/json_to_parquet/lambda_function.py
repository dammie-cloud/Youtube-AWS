
import boto3
import pandas as pd
import json
import os
import logging
import io
from urllib.parse import unquote_plus

from datetime import datetime, timezone

# AWS S3 clients
s3 = boto3.client("s3")

# Bucket configuration
SOURCE_BUCKET = "test-dammie"
DESTINATION_BUCKET = "test-dammie-silver"


def convert_csv_to_parquet(file_content):
    """
    Convert CSV data into Parquet format.
    """
    df = pd.read_csv(
        io.BytesIO(file_content),
        low_memory=False
    )

    output_buffer = io.BytesIO()

    df.to_parquet(
        output_buffer,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    output_buffer.seek(0)

    return output_buffer


def convert_json_to_parquet(file_content):
    """
    Convert YouTube category JSON into Parquet format.
    """
    data = json.loads(file_content.decode("utf-8"))

    # YouTube category files contain records inside 'items'
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        records = data["items"]

    elif isinstance(data, list):
        records = data

    elif isinstance(data, dict):
        records = [data]

    else:
        raise ValueError("Unsupported JSON structure")

    # Flatten nested JSON structures
    df = pd.json_normalize(records, sep="_")

    output_buffer = io.BytesIO()

    df.to_parquet(
        output_buffer,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    output_buffer.seek(0)

    return output_buffer


def lambda_handler(event, context):

    for record in event["Records"]:

        # Get source bucket and file name
        source_bucket = record["s3"]["bucket"]["name"]

        file_key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )

        # Only process files from the expected raw bucket
        if source_bucket != SOURCE_BUCKET:
            print(f"Skipping unexpected bucket: {source_bucket}")
            continue

        # Identify file extension
        file_extension = file_key.lower().split(".")[-1]

        if file_extension not in ["csv", "json"]:
            print(f"Skipping unsupported file: {file_key}")
            continue

        print(f"Processing: {file_key}")

        # Read file from raw S3 bucket
        response = s3.get_object(
            Bucket=SOURCE_BUCKET,
            Key=file_key
        )

        file_content = response["Body"].read()

        # Convert CSV or JSON into Parquet
        if file_extension == "csv":

            parquet_buffer = convert_csv_to_parquet(
                file_content
            )

        else:

            parquet_buffer = convert_json_to_parquet(
                file_content
            )

        # Change file extension to parquet
        destination_key = (
            file_key.rsplit(".", 1)[0] + ".parquet"
        )

        # Upload converted file into silver bucket
        s3.put_object(
            Bucket=DESTINATION_BUCKET,
            Key=destination_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/vnd.apache.parquet"
        )

        print(
            f"Successfully converted {file_key} "
            f"to s3://{DESTINATION_BUCKET}/{destination_key}"
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "File conversion completed",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    }
