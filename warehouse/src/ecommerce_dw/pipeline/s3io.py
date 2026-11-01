"""Lectura/escritura en S3 (bytes, JSON y Parquet) con boto3 + pyarrow."""
from __future__ import annotations

import io
import json
from typing import Any, Iterable

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError


class AlreadyExists(Exception):
    """Se intentó crear un objeto que ya existe (escritura condicional)."""


class S3:
    def __init__(self, bucket: str, region: str | None = None, client=None):
        self.bucket = bucket
        self.client = client or boto3.client("s3", region_name=region)

    # ---------- básicos ----------
    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream",
                  if_none_match: bool = False, metadata: dict[str, str] | None = None) -> None:
        kwargs: dict[str, Any] = dict(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        if metadata:
            kwargs["Metadata"] = metadata
        if if_none_match:
            # Escritura condicional de S3: falla si el objeto ya existe.
            # Así Bronze nunca se sobrescribe en silencio.
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if if_none_match and code in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
                raise AlreadyExists(key) from e
            raise

    def get_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        return keys

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list_keys(prefix)
        for i in range(0, len(keys), 1000):
            self.client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True},
            )
        return len(keys)

    # ---------- JSON ----------
    def put_json(self, key: str, obj: Any, if_none_match: bool = False) -> None:
        self.put_bytes(key, json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
                       "application/json", if_none_match=if_none_match)

    def get_json(self, key: str) -> Any:
        return json.loads(self.get_bytes(key))

    def get_json_or_none(self, key: str) -> Any | None:
        try:
            return self.get_json(key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def put_jsonl(self, key: str, rows: Iterable[dict]) -> None:
        body = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows) + "\n"
        self.put_bytes(key, body.encode("utf-8"), "application/x-ndjson")

    # ---------- Parquet ----------
    def write_parquet(self, key: str, rows: list[dict], schema: pa.Schema) -> None:
        table = pa.Table.from_pylist(rows, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        self.put_bytes(key, buf.getvalue(), "application/vnd.apache.parquet")

    def read_parquet_prefix(self, prefix: str, schema: pa.Schema | None = None) -> list[dict]:
        """Lee todos los .parquet directamente bajo `prefix` (no recursivo hacia otras versiones)."""
        if not prefix.endswith("/"):
            prefix += "/"
        rows: list[dict] = []
        for key in self.list_keys(prefix):
            rest = key[len(prefix):]
            if "/" in rest or not rest.endswith(".parquet"):
                continue
            table = pq.read_table(io.BytesIO(self.get_bytes(key)))
            if schema is not None:
                table = table.select([f.name for f in schema]).cast(schema)
            rows.extend(table.to_pylist())
        return rows


def s3_uri_to_prefix(uri: str, bucket: str) -> str:
    head = f"s3://{bucket}/"
    if not uri.startswith(head):
        raise ValueError(f"{uri} no pertenece al bucket {bucket}")
    return uri[len(head):]
