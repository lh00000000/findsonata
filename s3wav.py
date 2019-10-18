import boto3
import os
key_prefix = "2019/findsonata/"
bucket_name = os.environ["lh00000000-public"]
s3_client = boto3.client("s3")


def s3ls():
    for p in s3_client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket_name, Prefix=key_prefix
    ):
        for e in p["Contents"]:
            if e["Key"] != key_prefix:
                yield e["Key"]


def s3download(s3key):
    filename = s3key.split("/")[-1]
    s3_client.download_file(bucket_name, s3key, os.path.join("./wav", filename))


def s3newkeys(since):

    return [
        s3key
        for s3key in s3ls()
        if s3key.split("/")[-1] > since.isoformat()
    ]