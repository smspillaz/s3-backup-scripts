# Some backup scripts (local to s3 / s3 to local)

`backup_to_s3.py` will backup using the `GLACIER` tier. This is much cheaper, but you need to make a request
beforehand if you want to access your data.

Use `request_restore_from_s3.py` to do that recursively for a given prefix. Check the status in the console,
then once the files are ready for download, use `restore_from_s3.py` to download them.

All the backups are compressed using zstandard compression.

If you want to back up an entire directory and not the individual files, use `s3_backup_zip_at_depth.py` which
will recurse into the tree at a certain depth, then create zip files, compresss them with zstandard
and upload them to s3.
