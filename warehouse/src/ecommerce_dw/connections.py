"""Conexiones a las fuentes (RDS MySQL, DocumentDB) y al warehouse (Redshift).

Todo se configura con variables de entorno; en la EC2 el archivo docker/.env se genera
desde AWS SSM Parameter Store al arrancar (no hay secretos en el repositorio).
"""
from __future__ import annotations

import os

import pymysql
from pymongo import MongoClient


def connect_mysql(cursorclass=None, autocommit=False):
    """RDS for MySQL. Si MYSQL_SSL_CA apunta al bundle de AWS, la conexión va cifrada con TLS."""
    kwargs = dict(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "ecommerce"),
        charset="utf8mb4",
        autocommit=autocommit,
        connect_timeout=15,
    )
    ca = os.getenv("MYSQL_SSL_CA")
    if ca and os.path.exists(ca):
        kwargs["ssl"] = {"ca": ca}
    if cursorclass is not None:
        kwargs["cursorclass"] = cursorclass
    return pymysql.connect(**kwargs)


def connect_mongo(**kwargs) -> MongoClient:
    """Amazon DocumentDB (API compatible con MongoDB).

    Diferencias que obligan a ajustar la conexión respecto a MongoDB local:
      * TLS obligatorio con el CA bundle de AWS (tlsCAFile=/opt/certs/global-bundle.pem).
      * retryWrites=false: DocumentDB no soporta escrituras reintentables.
      * replicaSet=rs0 y readPreference=secondaryPreferred (recomendado por AWS).
      * Sólo accesible dentro de la VPC (no hay endpoint público).
    La URI completa viene en MONGODB_URI.
    """
    kwargs.setdefault("serverSelectionTimeoutMS", 15000)
    return MongoClient(os.environ["MONGODB_URI"], **kwargs)


def mongo_database(client: MongoClient):
    return client[os.getenv("MONGO_DATABASE", "ecommerce")]


def connect_warehouse():
    """Redshift (protocolo PostgreSQL). En pruebas locales apunta a un PostgreSQL."""
    import psycopg2

    return psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        dbname=os.getenv("REDSHIFT_DB", "dev"),
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        connect_timeout=20,
        sslmode=os.getenv("REDSHIFT_SSLMODE", "require"),
    )
