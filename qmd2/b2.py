"""Immutable S3 packs with server-verified checksums and bounded network calls."""

import base64
from contextlib import contextmanager
import hashlib
import os
import threading
from pathlib import Path


SINGLE_MAX = 5_000_000_000
PART_SIZE = 256 * 1024 * 1024


class B2:
    def __init__(self, bucket=None, prefix=None):
        import boto3
        from botocore.config import Config

        env = dict(os.environ)
        path = Path('/etc/qmd-agent/qmd-agent.env')
        if path.exists():
            for line in path.read_text().splitlines():
                if line.startswith('export '):
                    line = line[7:]
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    env.setdefault(k.strip(), v.strip().strip('"\''))
        region = env.get('QMD_B2_REGION')
        if not all(
            env.get(k) for k in ('QMD_B2_REGION', 'QMD_B2_KEY_ID', 'QMD_B2_APPLICATION_KEY')
        ):
            raise RuntimeError('B2 credentials unavailable')
        self.bucket = bucket or env.get('QMD_B2_BUCKET')
        self.prefix = prefix or env.get('QMD_B2_PREFIX', 'packs/v1')  # the pack-layout version, not an identity
        for key, value in (('QMD_B2_BUCKET', self.bucket),):
            if not value:
                raise RuntimeError(f'B2 config requires {key}')
        self.slots = threading.BoundedSemaphore(16)
        self.s3 = boto3.client(
            's3',
            endpoint_url=f'https://s3.{region}.backblazeb2.com',
            aws_access_key_id=env['QMD_B2_KEY_ID'],
            aws_secret_access_key=env['QMD_B2_APPLICATION_KEY'],
            region_name=region,
            config=Config(
                connect_timeout=5,
                read_timeout=20,
                max_pool_connections=16,
                retries={'mode': 'standard', 'total_max_attempts': 3},
            ),
        )

    def key(self, stream, source, seq):
        return f'{self.prefix}/{stream}/{source}/{seq:010d}.zst'

    def list(self, prefix=None):
        with self.slots:
            for page in self.s3.get_paginator('list_objects_v2').paginate(
                Bucket=self.bucket, Prefix=prefix or self.prefix + '/'
            ):
                yield from page.get('Contents', [])

    def put(self, key, path):
        if Path(path).stat().st_size > SINGLE_MAX:
            return self.put_multipart(key, path)
        md5 = hashlib.md5()
        with open(path, 'rb') as f:
            while chunk := f.read(1024 * 1024):
                md5.update(chunk)
        with self.slots, open(path, 'rb') as f:
            result = self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=f,
                ContentMD5=base64.b64encode(md5.digest()).decode(),
            )
        if result['ETag'].strip('"') != md5.hexdigest():
            raise RuntimeError('B2 checksum mismatch')

    def put_multipart(self, key, path):
        with self.slots, open(path, 'rb') as f:
            args = dict(Bucket=self.bucket, Key=key)
            args['UploadId'] = self.s3.create_multipart_upload(**args)['UploadId']
            try:
                parts = []
                while chunk := f.read(PART_SIZE):
                    md5 = hashlib.md5(chunk)
                    number = len(parts) + 1
                    result = self.s3.upload_part(
                        **args,
                        PartNumber=number,
                        Body=chunk,
                        ContentMD5=base64.b64encode(md5.digest()).decode(),
                    )
                    if result['ETag'].strip('"') != md5.hexdigest():
                        raise RuntimeError('B2 checksum mismatch')
                    parts.append(dict(PartNumber=number, ETag=result['ETag']))
                self.s3.complete_multipart_upload(**args, MultipartUpload={'Parts': parts})
            except BaseException:
                self.s3.abort_multipart_upload(**args)
                raise

    @contextmanager
    def open_range(self, key, offset, length):
        with self.slots:
            r = self.s3.get_object(
                Bucket=self.bucket, Key=key, Range=f'bytes={offset}-{offset + length - 1}'
            )
            try:
                yield r['Body']
            finally:
                r['Body'].close()

    def read(self, key, offset, length):
        with self.open_range(key, offset, length) as body:
            value = body.read()
        if len(value) != length:
            raise OSError('short B2 range')
        return value

    def download(self, bucket, key, target):
        with self.slots:
            r = self.s3.get_object(Bucket=bucket, Key=key)
            try:
                with open(target, 'wb') as f:
                    while chunk := r['Body'].read(1024 * 1024):
                        f.write(chunk)
            finally:
                r['Body'].close()
