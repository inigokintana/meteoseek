# -*- coding: utf-8 -*-
"""
fabric_utils.py — Shared helpers for the MeteoSeek Fabric notebooks.

Purpose
-------
Single source of truth for everything the Bronze/Silver/Gold notebooks share:
  * Lakehouse / OneLake paths (Bronze / Silver / Gold)
  * Fabric secret access (Seeketing login/password, Euskalmet private key)
  * Euskalmet JWT (RS256) generation
  * Seeketing REST auth (authRest -> session key)
  * Delta table read/write with schema evolution + idempotent merge

Conventions
-----------
* Secrets are NEVER embedded in code. They are read from the Fabric-managed
  secret store (mssparkutils.credentials.getSecret), which itself fronts
  Azure Key Vault. See CMDB.md SEC-01 / SEC-02.
* The Seeketing session `key` (SEC-03) is derived per run and never persisted.
* Every table write is idempotent: Bronze uses append with a dedupe guard,
  Silver/Gold use MERGE keyed on the grain columns from CMDB.md section 4.

This module is a plain Python file intended to be imported by notebooks via
`%run` is NOT used; instead notebooks do `from fabric_utils import ...` after
adding this file to the Lakehouse `Files/fabric/` path or the notebook
resources. See Fabric/README.md for the recommended import strategy.
"""

from __future__ import annotations

import json
import time
import base64
import hashlib
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from typing import Iterable
# --------------------------------------------------------------------------- #
# Lakehouse paths
# --------------------------------------------------------------------------- #
# OneLake paths are lakehouse-relative. The default Lakehouse is assumed to be
# "meteoseek_lh" (the workspace DEMO Lakehouse). Override via widget/env.
DEFAULT_LAKEHOUSE = "demo"

BRONZE = "Files/meteoseek/bronze"
SILVER = "Tables"
GOLD = "Tables"


def lakehouse_paths(lakehouse: str = DEFAULT_LAKEHOUSE) -> Dict[str, str]:
    """Return the OneLake base paths for each medallion layer."""
    return {
        "bronze": f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{BRONZE}",
        "silver": f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{SILVER}",
        "gold": f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{GOLD}",
    }


# --------------------------------------------------------------------------- #
# Fabric secret access
# --------------------------------------------------------------------------- #
def get_secret(secret_name: str) -> str:
    """Read a secret from the Fabric secret store (fronts Azure Key Vault).

    Falls back to a Spark config / env var of the same name so notebooks can be
    smoke-tested outside Fabric. In production the secret MUST be a Fabric
    secret, never a config value.
    """
    try:
        # `mssparkutils` is available in Fabric notebooks at runtime.
        from notebookutils import mssparkutils  # type: ignore

        return mssparkutils.credentials.getSecret(
            "https://fabric-meteoseek-kv.vault.azure.net/",
            secret_name
    )
    except Exception:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            conf_val = spark.conf.get(f"meteoseek.{secret_name}", "")
            if conf_val:
                return conf_val
        import os

        return os.environ.get(secret_name, "")


# --------------------------------------------------------------------------- #
# Euskalmet JWT (RS256) helpers
# --------------------------------------------------------------------------- #
def _b64url(data: bytes) -> str:
    """Base64url-encode bytes without padding (RFC 7515)."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def build_euskalmet_jwt(
    private_key_pem: str,
    fingerprint: Optional[str] = None,
    aud: str = "met01.apikey",
    version: str = "1.0.0",
    email: str = "meteoseek@example.com",
    iss: str = "meteoseek",
    ttl_seconds: int = 300,
) -> str:
    """Build a signed RS256 JWT for the Euskalmet API.

    Claims follow CMDB.md SRC-EUS-04..06:
        aud=met01.apikey, version=1.0.0, iss/iat/exp/email.

    The Euskadi API Manager authenticates via an RSA keypair registered with the
    portal; the key's fingerprint is carried in the JWT header as `kid`. We sign
    with the private key using `cryptography` (ships with every Fabric Spark
    runtime) so no PyJWT dependency is required.
    """
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    header: Dict[str, Any] = {"alg": "RS256", "typ": "JWT"}
    if fingerprint:
        header["kid"] = fingerprint

    now = int(time.time())
    payload = {
        "aud": aud,
        "version": version,
        "iss": iss,
        "iat": now,
        "exp": now + ttl_seconds,
        "email": email,
    }

    signing_input = (
        _b64url(json.dumps(header, separators=(",", ":")).encode("ascii"))
        + "."
        + _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    )

    key = serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
    signature = key.sign(
        signing_input.encode("ascii"),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    return signing_input + "." + _b64url(signature)


def euskalmet_headers(
    private_key_pem: str, fingerprint: Optional[str] = None, **kwargs: Any
) -> Dict[str, str]:
    """Return Authorization headers for Euskalmet calls."""
    token = build_euskalmet_jwt(private_key_pem, fingerprint=fingerprint, **kwargs)
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


# --------------------------------------------------------------------------- #
# Seeketing REST auth
# --------------------------------------------------------------------------- #
SEEKETING_BASE_URL = "https://www6.siketing.com/observer/rest/"


def seeketing_auth(login: str, password: str) -> str:
    """Call authRest and return the session `key` (SEC-03, in-memory only)."""
    import requests

    resp = requests.post(
        SEEKETING_BASE_URL,
        data={
            "method": "authRest",
            "params": json.dumps({"login": login, "password": password}),
        },
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("isError"):
        raise RuntimeError(
            f"Seeketing auth failed: {body.get('errorType')} "
            f"{body.get('errorDesc')} ({body.get('errorNum')})"
        )
    return body["response"]["data"]["key"]


def seeketing_call(key: str, method: str, params: Dict[str, Any]) -> Any:
    """Call a Seeketing service and return the `response.data` payload.

    Raises on `isError` so notebooks fail fast rather than landing empty data.
    """
    import requests

    params = dict(params)
    params.setdefault("key", key)
    resp = requests.post(
        SEEKETING_BASE_URL,
        data={"method": method, "params": json.dumps(params)},
        timeout=120,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("isError"):
        raise RuntimeError(
            f"Seeketing {method} failed: {body.get('errorType')} "
            f"{body.get('errorDesc')} ({body.get('errorNum')})"
        )
    return body["response"]["data"]


# --------------------------------------------------------------------------- #
# Delta helpers
# --------------------------------------------------------------------------- #
def read_table(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """
    Read a Delta table if it exists, else None.
    """
    try:
        return spark.table(table_name)
    except AnalysisException:
        return None


def write_bronze(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    dedupe_cols: Iterable[str],
    ingestion_ts_col: str = "ingested_at",
) -> None:
    """
    Append raw rows to a Bronze Delta table, deduplicating on dedupe_cols.

    Bronze is append-only with an ingestion timestamp.
    Re-runs are idempotent because existing hashes are excluded.
    """
    dedupe_cols = list(dedupe_cols)

    existing = read_table(spark, table_name)

    if existing is None:
        (
            df.withColumn(ingestion_ts_col, F.current_timestamp())
              .write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(table_name)
        )
        return

    new_rows = df.join(
        existing.select(*dedupe_cols),
        on=dedupe_cols,
        how="left_anti"
    )

    if new_rows.isEmpty():
        return

    (
        new_rows.withColumn(ingestion_ts_col, F.current_timestamp())
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(table_name)
    )


def upsert_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    key_cols: Iterable[str],
) -> None:
    """
    Idempotent MERGE into a managed Delta table.
    """

    key_cols = list(key_cols)

    if not spark.catalog.tableExists(table_name):
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(table_name)
        )
        return

    target = DeltaTable.forName(spark, table_name)

    merge_condition = " AND ".join(
        [f"t.{c} = s.{c}" for c in key_cols]
    )

    (
        target.alias("t")
              .merge(df.alias("s"), merge_condition)
              .whenMatchedUpdateAll()
              .whenNotMatchedInsertAll()
              .execute()
    )

# --------------------------------------------------------------------------- #
# Misc helpers
# --------------------------------------------------------------------------- #
def hourly_floor(ts_col: Column) -> Column:
    """Truncate a timestamp to the hour (aligns weather + footfall grain)."""
    return F.date_trunc("hour", ts_col)


def parse_ts(df: DataFrame, col: str, fmt: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
    """Parse a string timestamp into a typed timestamp column."""
    return df.withColumn(col, F.to_timestamp(F.col(col), fmt))


def json_str(obj: Any) -> str:
    """Serialize an object to a compact JSON string (for raw Bronze storage)."""
    return json.dumps(obj, ensure_ascii=False, default=str)
