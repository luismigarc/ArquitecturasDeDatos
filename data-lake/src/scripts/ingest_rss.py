import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / "config" / "sources.json"

BUCKET = os.getenv(
    "DATA_LAKE_BUCKET",
    "data-lake-arquitecturas-luismi-2026"
)

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

s3 = boto3.client("s3", region_name=AWS_REGION)


def cargar_fuentes():
    with open(CONFIG_FILE, "r", encoding="utf-8") as archivo:
        config = json.load(archivo)

    return config["sources"]


def calcular_hash(contenido):
    return hashlib.sha256(contenido).hexdigest()


def extraer_fuente(fuente, run_id):
    source = fuente["source"]
    section = fuente["section"]
    url = fuente["url"]

    ahora = datetime.now(timezone.utc)
    ingestion_date = ahora.strftime("%Y-%m-%d")
    ingestion_timestamp = ahora.isoformat()

    print(f"\nExtrayendo: {section}")
    print(f"URL: {url}")

    try:
        respuesta = requests.get(url, timeout=30)
        respuesta.raise_for_status()

        contenido = respuesta.content

        if not contenido:
            raise ValueError("La respuesta del RSS está vacía")

        content_hash = calcular_hash(contenido)

        prefix = (
            f"bronze/"
            f"source={source}/"
            f"section={section}/"
            f"ingestion_date={ingestion_date}/"
            f"run_id={run_id}/"
        )

        xml_key = prefix + "rss.xml"
        metadata_key = prefix + "metadata.json"

        # Guardar RSS original
        s3.put_object(
            Bucket=BUCKET,
            Key=xml_key,
            Body=contenido,
            ContentType="application/xml",
        )

        metadata = {
            "run_id": run_id,
            "source": source,
            "section": section,
            "url": url,
            "ingestion_timestamp_utc": ingestion_timestamp,
            "http_status": respuesta.status_code,
            "content_hash_sha256": content_hash,
            "s3_original_file": f"s3://{BUCKET}/{xml_key}",
            "status": "SUCCESS",
        }

        metadata_bytes = json.dumps(
            metadata,
            indent=2,
            ensure_ascii=False
        ).encode("utf-8")

        s3.put_object(
            Bucket=BUCKET,
            Key=metadata_key,
            Body=metadata_bytes,
            ContentType="application/json",
        )

        print(f"OK - HTTP {respuesta.status_code}")
        print(f"SHA-256: {content_hash}")
        print(f"S3: s3://{BUCKET}/{xml_key}")

        return metadata

    except requests.RequestException as error:
        print(f"ERROR HTTP: {error}")

        return {
            "run_id": run_id,
            "source": source,
            "section": section,
            "url": url,
            "status": "HTTP_ERROR",
            "error": str(error),
        }

    except Exception as error:
        print(f"ERROR: {error}")

        return {
            "run_id": run_id,
            "source": source,
            "section": section,
            "url": url,
            "status": "ERROR",
            "error": str(error),
        }


def main():
    run_id = str(uuid.uuid4())

    print("=" * 60)
    print("DATA LAKE - INGESTA RSS -> S3 BRONZE")
    print(f"Bucket: {BUCKET}")
    print(f"Región: {AWS_REGION}")
    print(f"RUN ID: {run_id}")
    print("=" * 60)

    fuentes = cargar_fuentes()
    resultados = []

    for fuente in fuentes:
        resultado = extraer_fuente(fuente, run_id)
        resultados.append(resultado)

    exitosas = sum(
        resultado["status"] == "SUCCESS"
        for resultado in resultados
    )

    print("\n" + "=" * 60)
    print("RESUMEN")
    print("=" * 60)
    print(f"Run ID: {run_id}")
    print(f"Fuentes procesadas: {len(resultados)}")
    print(f"Exitosas: {exitosas}")
    print(f"Fallidas: {len(resultados) - exitosas}")

    if exitosas != len(resultados):
        raise RuntimeError(
            "La ingesta terminó con una o más fuentes fallidas."
        )


if __name__ == "__main__":
    main()
