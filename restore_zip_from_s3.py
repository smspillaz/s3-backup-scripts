import argparse
import boto3
import zstandard
import os
import datetime
import itertools
from tqdm.auto import tqdm
import multiprocessing
import zipfile
from io import BytesIO


def paginate_bucket_and_get_properties(client, bucket, path):
    # Initialize paginator for listing objects in the bucket
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=path)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                # Extract key and timestamp
                yield obj['Key']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket")
    parser.add_argument("--path")
    parser.add_argument("--destination")
    args = parser.parse_args()

    client = boto3.client('s3')

    os.makedirs(args.destination, exist_ok=True)

    keys = list(paginate_bucket_and_get_properties(client, args.bucket, args.path))

    for key in tqdm(keys):
        stream = BytesIO()
        zstandard.ZstdDecompressor().copy_stream(
            client.get_object(
                Bucket=args.bucket,
                Key=key
            )['Body'],
            stream
        )

        with zipfile.ZipFile(stream, "r") as zf:
            zf.extractall(path=args.destination)


if __name__ == "__main__":
    main()