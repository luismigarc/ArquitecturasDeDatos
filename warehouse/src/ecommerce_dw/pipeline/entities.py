"""Definición única de las entidades extraídas: columnas, tipos Parquet/Redshift y modo de extracción.

Modo de extracción:
  * full        -> se copia la tabla completa en cada lote (dimensiones pequeñas: clientes y productos).
                   Así los cambios para SCD2 y las bajas se detectan sin depender de marcas de agua.
  * incremental -> sólo registros con llave > marca de agua guardada (pedidos, líneas, reseñas, eventos).
                   La marca de agua se actualiza al FINAL de un lote exitoso.
Cada fila lleva además _batch_id, _extracted_at (UTC) y _source.
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class Column:
    name: str
    arrow: pa.DataType
    redshift: str


@dataclass(frozen=True)
class Entity:
    source: str          # mysql | mongodb
    name: str            # customers, reviews, ...
    key: str             # llave de negocio
    mode: str            # full | incremental
    columns: tuple[Column, ...]

    @property
    def raw_table(self) -> str:
        prefix = "mysql" if self.source == "mysql" else "mongo"
        # RAW es palabra reservada en Redshift: el esquema va entre comillas.
        return f'"raw".{prefix}_{self.name}'

    @property
    def source_label(self) -> str:
        return f"{self.source}.{self.name}"

    def s3_prefix(self, batch_id: str) -> str:
        return f"raw/{self.source}/{self.name}/batch_id={batch_id}/"

    @property
    def all_columns(self) -> tuple[Column, ...]:
        return self.columns + META_COLUMNS

    @property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema([(c.name, c.arrow) for c in self.all_columns])

    def ddl(self) -> str:
        cols = ",\n    ".join(f"{c.name} {c.redshift}" for c in self.all_columns)
        return f"CREATE TABLE IF NOT EXISTS {self.raw_table} (\n    {cols}\n)"


TS = pa.timestamp("us")
META_COLUMNS = (
    Column("_batch_id", pa.string(), "VARCHAR(160)"),
    Column("_extracted_at", TS, "TIMESTAMP"),
    Column("_source", pa.string(), "VARCHAR(60)"),
)

ENTITIES: tuple[Entity, ...] = (
    Entity("mysql", "customers", "customer_id", "full", (
        Column("customer_id", pa.int64(), "BIGINT"),
        Column("name", pa.string(), "VARCHAR(400)"),
        Column("email", pa.string(), "VARCHAR(400)"),
        Column("phone", pa.string(), "VARCHAR(64)"),
        Column("city", pa.string(), "VARCHAR(200)"),
        Column("department", pa.string(), "VARCHAR(200)"),
        Column("registration_date", pa.date32(), "DATE"),
        Column("updated_at", TS, "TIMESTAMP"),
    )),
    Entity("mysql", "products", "product_id", "full", (
        Column("product_id", pa.int64(), "BIGINT"),
        Column("name", pa.string(), "VARCHAR(400)"),
        Column("category", pa.string(), "VARCHAR(200)"),
        Column("price", pa.decimal128(12, 2), "DECIMAL(12,2)"),
        Column("description", pa.string(), "VARCHAR(1000)"),
        Column("is_active", pa.bool_(), "BOOLEAN"),
        Column("updated_at", TS, "TIMESTAMP"),
    )),
    Entity("mysql", "orders", "order_id", "incremental", (
        Column("order_id", pa.int64(), "BIGINT"),
        Column("customer_id", pa.int64(), "BIGINT"),
        Column("order_date", TS, "TIMESTAMP"),
        Column("status", pa.string(), "VARCHAR(60)"),
        Column("sales_channel", pa.string(), "VARCHAR(60)"),
        Column("payment_method", pa.string(), "VARCHAR(60)"),
        Column("total", pa.decimal128(14, 2), "DECIMAL(14,2)"),
    )),
    Entity("mysql", "order_items", "order_item_id", "incremental", (
        Column("order_item_id", pa.int64(), "BIGINT"),
        Column("order_id", pa.int64(), "BIGINT"),
        Column("product_id", pa.int64(), "BIGINT"),
        Column("quantity", pa.int32(), "INTEGER"),
        Column("unit_price", pa.decimal128(12, 2), "DECIMAL(12,2)"),
        Column("subtotal", pa.decimal128(14, 2), "DECIMAL(14,2)"),
    )),
    Entity("mongodb", "reviews", "review_id", "incremental", (
        Column("review_id", pa.int64(), "BIGINT"),
        Column("customer_id", pa.int64(), "BIGINT"),
        Column("product_id", pa.int64(), "BIGINT"),
        Column("rating", pa.int32(), "INTEGER"),
        Column("reviewed_at", TS, "TIMESTAMP"),          # UTC
        Column("is_verified_purchase", pa.bool_(), "BOOLEAN"),
        Column("document_json", pa.string(), "VARCHAR(65535)"),
    )),
    Entity("mongodb", "navigation_events", "event_id", "incremental", (
        Column("event_id", pa.int64(), "BIGINT"),
        Column("event_type", pa.string(), "VARCHAR(100)"),
        Column("occurred_at", TS, "TIMESTAMP"),          # UTC
        Column("customer_id", pa.int64(), "BIGINT"),
        Column("product_id", pa.int64(), "BIGINT"),
        Column("sales_channel", pa.string(), "VARCHAR(60)"),
        Column("document_json", pa.string(), "VARCHAR(65535)"),
    )),
)

BY_NAME = {e.name: e for e in ENTITIES}
