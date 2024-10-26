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
                key = obj['Key']
                timestamp = obj['LastModified']
                yield (os.path.splitext(key[len(path):])[0], timestamp.timestamp())


def upload_to_s3(local_path_prefix, s3_path_prefix, bucket, path):
    client = boto3.client('s3')

    with open(os.path.join(local_path_prefix, path), "rb") as f:
        with zstandard.ZstdCompressor().stream_reader(f) as cf:
            # Silly that we have to read the whole thing into memory, but
            # checksums are computed client side
            compressed_bytes = cf.read()
            tqdm.write(f"{os.path.join(local_path_prefix, path)} -> {bucket}:{os.path.join(s3_path_prefix, path + '.zst')}\n")
            return path, client.put_object(
                Body=compressed_bytes,
                StorageClass="GLACIER",
                Bucket=bucket,
                Key=os.path.join(s3_path_prefix, path) + ".zst"
            )


def upload_to_s3_wrapper(args):
    return upload_to_s3(*args)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--path", type=str, required=True)
    parser.add_argument("--backup-directory", type=str, required=True)
    args = parser.parse_args()

    client = boto3.client('s3')

    # Prepare a list to hold key-timestamp pairs
    objects_timestamps = dict(paginate_bucket_and_get_properties(client, args.bucket, args.path))

    src_timestamps = dict(itertools.chain.from_iterable(
        [
            (os.path.join(root, fname)[len(args.backup_directory):], os.stat(os.path.join(root, fname)).st_mtime)
            for fname in fnames
        ]
        for root, dnames, fnames in os.walk(args.backup_directory)
    ))

    paths_to_upload = [
        path for path, timestamp in src_timestamps.items()
        if path not in objects_timestamps or objects_timestamps[path] < timestamp
    ]

    print(f"Found {len(objects_timestamps)} objects on S3")
    print(f"Found {len(src_timestamps)} objects locally")
    print(f"Uploading {len(paths_to_upload)} objects")

    print(f"Using {os.cpu_count()} threads")

    with multiprocessing.Pool() as pool:
        responses = list(
            tqdm(
                pool.map(
                    upload_to_s3_wrapper,
                    map(
                        lambda x: (args.backup_directory, args.path, args.bucket, x),
                        paths_to_upload
                    )
                ),
                total=len(paths_to_upload)
            )
        )


if __name__ == "__main__":
    main()