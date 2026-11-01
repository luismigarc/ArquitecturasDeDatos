import boto3
import io
import os
from datetime import datetime, timezone

import pandas as pd


BUCKET = os.getenv(
    "DATA_LAKE_BUCKET",
    "data-lake-arquitecturas-luismi-2026"
)

REGION = os.getenv("AWS_REGION", "us-east-2")

s3 = boto3.client("s3", region_name=REGION)


def obtener_ultimo_parquet(prefix, filename):
    respuesta = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=prefix
    )

    archivos = [
        obj for obj in respuesta.get("Contents", [])
        if obj["Key"].endswith(filename)
    ]

    if not archivos:
        raise RuntimeError(
            f"No se encontró {filename} en {prefix}"
        )

    ultimo = max(
        archivos,
        key=lambda obj: obj["LastModified"]
    )

    print(f"Leyendo: s3://{BUCKET}/{ultimo['Key']}")

    objeto = s3.get_object(
        Bucket=BUCKET,
        Key=ultimo["Key"]
    )

    return pd.read_parquet(
        io.BytesIO(objeto["Body"].read())
    )


def guardar_parquet(df, key):
    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(
        f"Publicado: s3://{BUCKET}/{key} "
        f"({len(df)} filas)"
    )


def main():

    print("=" * 70)
    print("DATA LAKE - SILVER -> GOLD")
    print("=" * 70)

    # ---------------------------------------------------------
    # Leer Silver
    # ---------------------------------------------------------

    news = obtener_ultimo_parquet(
        "silver/news/",
        "news.parquet"
    )

    entities = obtener_ultimo_parquet(
        "silver/news_entities/",
        "news_entities.parquet"
    )

    phrases = obtener_ultimo_parquet(
        "silver/news_key_phrases/",
        "news_key_phrases.parquet"
    )

    print()
    print(f"Noticias Silver: {len(news)}")
    print(f"Entidades Silver: {len(entities)}")
    print(f"Frases Silver: {len(phrases)}")

    # Convertir fecha de publicación
    news["published_at_utc"] = pd.to_datetime(
        news["published_at_utc"],
        utc=True,
        errors="coerce"
    )

    news["publication_date"] = (
        news["published_at_utc"]
        .dt.strftime("%Y-%m-%d")
    )

    # Información necesaria para enriquecer entidades/frases
    news_dimension = news[
        [
            "news_id",
            "source",
            "section",
            "publication_date"
        ]
    ].drop_duplicates(
        subset=["news_id"]
    )

    # ---------------------------------------------------------
    # GOLD 1
    # Noticias por día, medio y sección
    # ---------------------------------------------------------

    gold_news_daily = (
        news.groupby(
            [
                "publication_date",
                "source",
                "section"
            ],
            dropna=False
        )["news_id"]
        .nunique()
        .reset_index(
            name="distinct_news_count"
        )
    )

    # ---------------------------------------------------------
    # GOLD 2
    # Entidades presentes en más noticias
    # ---------------------------------------------------------

    entities_enriched = entities.merge(
        news_dimension,
        on="news_id",
        how="inner"
    )

    # Una entidad puede aparecer varias veces en una noticia.
    # Para Gold solo cuenta una vez por noticia.
    entity_news = entities_enriched.drop_duplicates(
        subset=[
            "news_id",
            "entity_type",
            "entity_text"
        ]
    )

    gold_top_entities = (
        entity_news.groupby(
            [
                "entity_type",
                "entity_text"
            ],
            dropna=False
        )["news_id"]
        .nunique()
        .reset_index(
            name="distinct_news_count"
        )
        .sort_values(
            "distinct_news_count",
            ascending=False
        )
    )

    # ---------------------------------------------------------
    # GOLD 3
    # Frases clave más frecuentes por sección
    # ---------------------------------------------------------

    phrases_enriched = phrases.merge(
        news_dimension,
        on="news_id",
        how="inner"
    )

    # Una frase cuenta máximo una vez por noticia.
    phrase_news = phrases_enriched.drop_duplicates(
        subset=[
            "news_id",
            "section",
            "key_phrase"
        ]
    )

    gold_key_phrases_section = (
        phrase_news.groupby(
            [
                "source",
                "section",
                "key_phrase"
            ],
            dropna=False
        )["news_id"]
        .nunique()
        .reset_index(
            name="distinct_news_count"
        )
        .sort_values(
            [
                "section",
                "distinct_news_count"
            ],
            ascending=[True, False]
        )
    )

    # ---------------------------------------------------------
    # GOLD 4
    # Presencia de entidades a través de fechas
    # ---------------------------------------------------------

    entity_date_news = entity_news.drop_duplicates(
        subset=[
            "publication_date",
            "news_id",
            "entity_type",
            "entity_text"
        ]
    )

    gold_entity_daily = (
        entity_date_news.groupby(
            [
                "publication_date",
                "entity_type",
                "entity_text"
            ],
            dropna=False
        )["news_id"]
        .nunique()
        .reset_index(
            name="distinct_news_count"
        )
        .sort_values(
            [
                "publication_date",
                "distinct_news_count"
            ],
            ascending=[True, False]
        )
    )

    # ---------------------------------------------------------
    # Publicación
    # ---------------------------------------------------------

    processing_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    guardar_parquet(
        gold_news_daily,
        (
            "gold/news_daily/"
            f"processing_date={processing_date}/"
            "news_daily.parquet"
        )
    )

    guardar_parquet(
        gold_top_entities,
        (
            "gold/top_entities/"
            f"processing_date={processing_date}/"
            "top_entities.parquet"
        )
    )

    guardar_parquet(
        gold_key_phrases_section,
        (
            "gold/key_phrases_by_section/"
            f"processing_date={processing_date}/"
            "key_phrases_by_section.parquet"
        )
    )

    guardar_parquet(
        gold_entity_daily,
        (
            "gold/entity_daily/"
            f"processing_date={processing_date}/"
            "entity_daily.parquet"
        )
    )

    # ---------------------------------------------------------
    # Validación Silver vs Gold
    # ---------------------------------------------------------

    silver_distinct = news["news_id"].nunique()

    gold_distinct = int(
        gold_news_daily[
            "distinct_news_count"
        ].sum()
    )

    print()
    print("=" * 70)
    print("VALIDACIÓN SILVER -> GOLD")
    print("=" * 70)

    print(
        f"Noticias distintas Silver: {silver_distinct}"
    )

    print(
        f"Noticias contabilizadas Gold: {gold_distinct}"
    )

    if silver_distinct == gold_distinct:
        print("RECONCILIACIÓN: OK")
    else:
        print("RECONCILIACIÓN: ERROR")

    print()
    print("=" * 70)
    print("RESUMEN GOLD")
    print("=" * 70)

    print(
        f"Filas news_daily: "
        f"{len(gold_news_daily)}"
    )

    print(
        f"Filas top_entities: "
        f"{len(gold_top_entities)}"
    )

    print(
        f"Filas key_phrases_by_section: "
        f"{len(gold_key_phrases_section)}"
    )

    print(
        f"Filas entity_daily: "
        f"{len(gold_entity_daily)}"
    )

    print("GOLD COMPLETADO")


if __name__ == "__main__":
    main()
