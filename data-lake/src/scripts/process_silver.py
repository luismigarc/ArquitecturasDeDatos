import boto3
import feedparser
import hashlib
import io
import os
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import pandas as pd


BUCKET = os.getenv(
    "DATA_LAKE_BUCKET",
    "data-lake-arquitecturas-luismi-2026"
)

REGION = os.getenv("AWS_REGION", "us-east-2")

s3 = boto3.client("s3", region_name=REGION)


def limpiar_html(texto):
    if not texto:
        return None

    texto = BeautifulSoup(texto, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", texto).strip()


def crear_news_id(identifier):
    return hashlib.sha256(
        identifier.encode("utf-8")
    ).hexdigest()


def crear_text_hash(title, description):
    texto = f"{title or ''}|{description or ''}"

    return hashlib.sha256(
        texto.encode("utf-8")
    ).hexdigest()


def normalizar_fecha(fecha):
    if not fecha:
        return None

    try:
        timestamp = pd.to_datetime(fecha, utc=True)
        return timestamp
    except Exception:
        return None


def listar_xml_bronze():
    paginator = s3.get_paginator("list_objects_v2")

    archivos = []

    for pagina in paginator.paginate(
        Bucket=BUCKET,
        Prefix="bronze/"
    ):
        for objeto in pagina.get("Contents", []):
            key = objeto["Key"]

            if key.endswith("/rss.xml"):
                archivos.append(key)

    return archivos


def extraer_valor_ruta(key, campo):
    patron = rf"{campo}=([^/]+)"
    resultado = re.search(patron, key)

    return resultado.group(1) if resultado else None


def procesar_xml(key):
    print(f"\nProcesando: s3://{BUCKET}/{key}")

    respuesta = s3.get_object(
        Bucket=BUCKET,
        Key=key
    )

    contenido = respuesta["Body"].read()

    feed = feedparser.parse(contenido)

    if feed.bozo:
        print(f"ADVERTENCIA XML: {feed.bozo_exception}")

    source = extraer_valor_ruta(key, "source")
    section = extraer_valor_ruta(key, "section")
    run_id = extraer_valor_ruta(key, "run_id")
    ingestion_date = extraer_valor_ruta(key, "ingestion_date")

    registros = []
    rechazados = []

    for entry in feed.entries:

        identifier = (
            entry.get("id")
            or entry.get("guid")
            or entry.get("link")
        )

        title = limpiar_html(entry.get("title"))
        link = entry.get("link")
        description = limpiar_html(entry.get("summary"))

        published_original = entry.get("published")
        published_at = normalizar_fecha(published_original)

        errores = []

        if not identifier:
            errores.append("missing_identifier")

        if not title:
            errores.append("missing_title")

        if not link:
            errores.append("missing_link")

        if published_at is None:
            errores.append("invalid_or_missing_published_date")

        if errores:
            rechazados.append({
                "source": source,
                "section": section,
                "run_id": run_id,
                "bronze_file": key,
                "identifier": identifier,
                "title": title,
                "link": link,
                "published_original": published_original,
                "rejection_reason": "|".join(errores),
            })

            continue

        news_id = crear_news_id(identifier)
        text_hash = crear_text_hash(title, description)

        registros.append({
            "news_id": news_id,
            "identifier": identifier,
            "source": source,
            "section": section,
            "title": title,
            "link": link,
            "description": description,
            "published_original": published_original,
            "published_at_utc": published_at,
            "text_hash": text_hash,
            "bronze_run_id": run_id,
            "bronze_file": key,
            "ingestion_date": ingestion_date,
        })

    return registros, rechazados


def escribir_parquet_s3(df, key):
    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow"
    )

    buffer.seek(0)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(f"Publicado: s3://{BUCKET}/{key}")


def main():

    print("=" * 65)
    print("DATA LAKE - BRONZE -> SILVER")
    print("=" * 65)

    archivos = listar_xml_bronze()

    print(f"Archivos Bronze encontrados: {len(archivos)}")

    todos = []
    rechazados = []

    for archivo in archivos:
        registros, errores = procesar_xml(archivo)

        todos.extend(registros)
        rechazados.extend(errores)

    if not todos:
        raise RuntimeError(
            "No se encontraron noticias válidas."
        )

    df = pd.DataFrame(todos)

    print(f"\nRegistros recibidos: {len(df)}")

    # Una noticia puede aparecer en varios lotes.
    # Conservamos la versión más reciente.
    df["published_at_utc"] = pd.to_datetime(
        df["published_at_utc"],
        utc=True
    )

    df = (
        df.sort_values(
            ["ingestion_date", "bronze_run_id"]
        )
        .drop_duplicates(
            subset=["news_id"],
            keep="last"
        )
    )

    print(f"Noticias únicas vigentes: {len(df)}")
    print(f"Rechazados: {len(rechazados)}")

    processing_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    silver_key = (
        f"silver/news/"
        f"processing_date={processing_date}/"
        f"news.parquet"
    )

    escribir_parquet_s3(df, silver_key)

    if rechazados:
        df_rechazados = pd.DataFrame(rechazados)

        quarantine_key = (
            f"quarantine/"
            f"processing_date={processing_date}/"
            f"rejected_news.parquet"
        )

        escribir_parquet_s3(
            df_rechazados,
            quarantine_key
        )

    print("\n" + "=" * 65)
    print("SILVER COMPLETADO")
    print("=" * 65)
    print(f"Noticias vigentes: {len(df)}")
    print(f"Rechazados: {len(rechazados)}")


if __name__ == "__main__":
    main()
