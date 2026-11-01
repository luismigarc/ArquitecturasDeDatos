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
comprehend = boto3.client("comprehend", region_name=REGION)


def obtener_ultimo_news():
    respuesta = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix="silver/news/"
    )

    archivos = [
        obj for obj in respuesta.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not archivos:
        raise RuntimeError("No existe news.parquet en Silver.")

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


def leer_cache():
    """
    Recupera hashes de textos que ya fueron analizados.
    Esto evita llamadas innecesarias a Comprehend.
    """

    respuesta = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix="silver/comprehend_cache/"
    )

    archivos = [
        obj for obj in respuesta.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not archivos:
        return pd.DataFrame()

    ultimo = max(
        archivos,
        key=lambda obj: obj["LastModified"]
    )

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

    print(f"Publicado: s3://{BUCKET}/{key}")


def preparar_texto(row):
    title = str(row.get("title") or "")
    description = str(row.get("description") or "")

    texto = f"{title}. {description}".strip()

    # DetectEntities y DetectKeyPhrases tienen límites
    # de tamaño para procesamiento síncrono.
    return texto[:4500]


def main():

    print("=" * 70)
    print("DATA LAKE - ENRIQUECIMIENTO AMAZON COMPREHEND")
    print("=" * 70)

    news = obtener_ultimo_news()

    print(f"Noticias vigentes: {len(news)}")

    cache_anterior = leer_cache()

    hashes_analizados = set()

    if not cache_anterior.empty:
        hashes_analizados = set(
            cache_anterior["text_hash"].astype(str)
        )

    print(
        f"Textos previamente analizados: "
        f"{len(hashes_analizados)}"
    )

    entities = []
    key_phrases = []
    cache_nuevo = []

    llamadas = 0
    reutilizadas = 0
    errores = 0

    fecha_analisis = datetime.now(
        timezone.utc
    ).isoformat()

    for numero, (_, row) in enumerate(
        news.iterrows(),
        start=1
    ):
        news_id = row["news_id"]
        text_hash = row["text_hash"]

        print(
            f"[{numero}/{len(news)}] "
            f"{str(row['title'])[:60]}"
        )

        if str(text_hash) in hashes_analizados:
            print("  CACHE - texto ya analizado")
            reutilizadas += 1
            continue

        texto = preparar_texto(row)

        if not texto:
            print("  OMITIDA - texto vacío")
            errores += 1
            continue

        try:
            idioma_response = (
                comprehend.detect_dominant_language(
                    Text=texto
                )
            )

            languages = idioma_response.get(
                "Languages",
                []
            )

            if not languages:
                print("  OMITIDA - idioma no detectado")
                errores += 1
                continue

            idioma = languages[0]["LanguageCode"]
            idioma_score = languages[0]["Score"]

            # Para esta actividad esperamos principalmente español.
            if idioma != "es":
                print(
                    f"  OMITIDA - idioma no contemplado: "
                    f"{idioma}"
                )

                cache_nuevo.append({
                    "news_id": news_id,
                    "text_hash": text_hash,
                    "language": idioma,
                    "language_score": idioma_score,
                    "analysis_date_utc": fecha_analisis,
                    "status": "UNSUPPORTED_LANGUAGE",
                    "analyzed_text": texto,
                })

                continue

            entities_response = comprehend.detect_entities(
                Text=texto,
                LanguageCode=idioma
            )

            phrases_response = (
                comprehend.detect_key_phrases(
                    Text=texto,
                    LanguageCode=idioma
                )
            )

            llamadas += 1

            # Personas, organizaciones y lugares
            for entity in entities_response.get(
                "Entities",
                []
            ):
                if entity["Type"] not in {
                    "PERSON",
                    "ORGANIZATION",
                    "LOCATION"
                }:
                    continue

                entities.append({
                    "news_id": news_id,
                    "text_hash": text_hash,
                    "entity_text": entity["Text"],
                    "entity_type": entity["Type"],
                    "confidence_score": entity["Score"],
                    "language": idioma,
                    "analysis_date_utc": fecha_analisis,
                })

            # Frases clave
            for phrase in phrases_response.get(
                "KeyPhrases",
                []
            ):
                key_phrases.append({
                    "news_id": news_id,
                    "text_hash": text_hash,
                    "key_phrase": phrase["Text"],
                    "confidence_score": phrase["Score"],
                    "language": idioma,
                    "analysis_date_utc": fecha_analisis,
                })

            cache_nuevo.append({
                "news_id": news_id,
                "text_hash": text_hash,
                "language": idioma,
                "language_score": idioma_score,
                "analysis_date_utc": fecha_analisis,
                "status": "SUCCESS",
                "analyzed_text": texto,
            })

            print(
                f"  OK - {idioma} | "
                f"entidades="
                f"{len(entities_response.get('Entities', []))} | "
                f"frases="
                f"{len(phrases_response.get('KeyPhrases', []))}"
            )

        except Exception as error:
            errores += 1
            print(f"  ERROR: {error}")

    processing_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    # Eliminar repeticiones dentro del mismo resultado.
    if entities:
        df_entities = pd.DataFrame(entities)

        df_entities = df_entities.drop_duplicates(
            subset=[
                "news_id",
                "entity_text",
                "entity_type"
            ]
        )

        guardar_parquet(
            df_entities,
            (
                "silver/news_entities/"
                f"processing_date={processing_date}/"
                "news_entities.parquet"
            )
        )

    if key_phrases:
        df_phrases = pd.DataFrame(key_phrases)

        df_phrases = df_phrases.drop_duplicates(
            subset=[
                "news_id",
                "key_phrase"
            ]
        )

        guardar_parquet(
            df_phrases,
            (
                "silver/news_key_phrases/"
                f"processing_date={processing_date}/"
                "news_key_phrases.parquet"
            )
        )

    if cache_nuevo:
        df_cache_nuevo = pd.DataFrame(cache_nuevo)

        if not cache_anterior.empty:
            df_cache = pd.concat(
                [
                    cache_anterior,
                    df_cache_nuevo
                ],
                ignore_index=True
            )

            df_cache = df_cache.drop_duplicates(
                subset=["text_hash"],
                keep="last"
            )
        else:
            df_cache = df_cache_nuevo

        guardar_parquet(
            df_cache,
            (
                "silver/comprehend_cache/"
                f"processing_date={processing_date}/"
                "cache.parquet"
            )
        )

    print("\n" + "=" * 70)
    print("RESUMEN COMPREHEND")
    print("=" * 70)
    print(f"Noticias: {len(news)}")
    print(f"Nuevos textos analizados: {llamadas}")
    print(f"Reutilizados por hash: {reutilizadas}")
    print(f"Errores/omitidos: {errores}")
    print(f"Entidades distintas: {len(entities)}")
    print(f"Frases clave detectadas: {len(key_phrases)}")


if __name__ == "__main__":
    main()
