import argparse
import boto3
import zstandard
import os
import datetime
import itertools
from tqdm.auto import tqdm
import multiprocessing

def paginate_bucket_and_get_properties(client, bucket, path):
    # Initialize paginator for listing objects in the bucket
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=path)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                # Extract key and timestamp
                yield obj['Key'][len(path):].lstrip("/")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str)
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--destination", type=str)
    args = parser.parse_args()

    client = boto3.client('s3')

    paths = list(paginate_bucket_and_get_properties(client, args.bucket, args.path))

    for path in tqdm(paths):
        local_path = os.path.join(args.destination, path)
        s3_path = os.path.join(args.path, path)
        print(f"Restoring to {args.bucket}:{s3_path} to {local_path}")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            zstandard.ZstdDecompressor().copy_stream(
                client.get_object(
                    Bucket=args.bucket,
                    Key=s3_path
                )['Body'],
                f
            )

if __name__ == "__main__":
    main()