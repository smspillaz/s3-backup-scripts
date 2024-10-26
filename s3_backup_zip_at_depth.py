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
                key = obj['Key']
                timestamp = obj['LastModified']
                yield (key[len(path):], timestamp.timestamp())


def upload_to_s3(local_path_prefix, s3_path_prefix, bucket, path):
    client = boto3.client('s3')
    stream = BytesIO()

    directory_to_compress = os.path.join(local_path_prefix, path)
    s3_key = os.path.join(s3_path_prefix, path) + ".zip.zst"

    with zipfile.ZipFile(stream, "a", compression=zipfile.ZIP_STORED) as zip_writer:
        for root, dirnames, filenames in os.walk(directory_to_compress):
            for filename in filenames:
                #print(f"Add {os.path.join(root, filename)} to {s3_key}")
                zip_writer.write(
                    os.path.join(root, filename),
                    arcname=os.path.join(root[len(local_path_prefix):].lstrip("/"), filename)
                )

    # Silly that we have to read the whole thing into memory, but
    # checksums are computed client side
    stream.seek(0)
    with zstandard.ZstdCompressor().stream_reader(stream) as cf:
        compressed_bytes = cf.read()
        tqdm.write(f"{os.path.join(local_path_prefix, path)} -> {bucket}:{s3_key} {len(compressed_bytes)} bytes\n")
        return path, client.put_object(
            Body=compressed_bytes,
            Bucket=bucket,
            Key=s3_key
        )


def upload_to_s3_wrapper(args):
    return upload_to_s3(*args)


def file_mtimes(path):
    for root, dnames, fnames in os.walk(path):
        for fname in fnames:
            yield os.stat(os.path.join(root, fname)).st_mtime

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--path", type=str, required=True)
    parser.add_argument("--backup-directory", type=str, required=True)
    parser.add_argument("--depth", type=int, required=True)
    args = parser.parse_args()

    client = boto3.client('s3')

    # Prepare a list to hold key-timestamp pairs
    objects_timestamps = dict([
        # strip .zip and .zst
        (os.path.splitext(os.path.splitext(path)[0])[0], timestamp)
        for path, timestamp in paginate_bucket_and_get_properties(client, args.bucket, args.path)
    ])

    src_timestamps = dict(itertools.chain.from_iterable(
        [
            (os.path.join(root, dname)[len(args.backup_directory):].strip("/"), max(file_mtimes(os.path.join(root, dname))))
            for dname in dnames
        ]
        for root, dnames, fnames in os.walk(args.backup_directory)
        if len(root[len(args.backup_directory):].strip("/").split("/")) == (args.depth - 1)
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