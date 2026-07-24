# search/index_es.py

import sys
import json
import logging
import hashlib
from datetime import datetime
from elasticsearch import Elasticsearch
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ES_HOST        = "localhost"
ES_PORT        = 9200
MINIO_HOST     = "localhost:9000"
MINIO_USER     = "admin"
MINIO_PASSWORD = "password123"
INDEX_NAME     = "university_content"


def get_es_client():
    return Elasticsearch(
        host=ES_HOST,
        port=ES_PORT,
        scheme="http"
    )

def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

def create_index(es):
    if es.indices.exists(index=INDEX_NAME):
        logger.info(f"✅ Index {INDEX_NAME} existe déjà")
        return
    with open("search/mapping.json", "r") as f:
        mapping = json.load(f)
    es.indices.create(index=INDEX_NAME, body=mapping)
    logger.info(f"✅ Index {INDEX_NAME} créé")

def list_minio_objects(client, bucket, university, faculty):
    prefix  = f"university={university}/faculty={faculty}/"
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects
            if not obj.object_name.endswith(".meta.json")]

def get_metadata(client, bucket, obj_name):
    try:
        meta_path = obj_name + ".meta.json"
        response  = client.get_object(bucket, meta_path)
        return json.loads(response.read().decode("utf-8"))
    except:
        return {}

def index_document(es, doc_id, document):
    es.index(index=INDEX_NAME, id=doc_id, document=document)

def index_to_elasticsearch(university, faculty):
    logger.info(f"🚀 Indexation ES : {faculty} — {university}")

    es    = get_es_client()
    minio = get_minio_client()

    create_index(es)

    indexed = 0
    errors  = 0

    # ── Indexer les HTML ──
    html_objects = list_minio_objects(minio, "raw-web-html", university, faculty)
    logger.info(f"📄 {len(html_objects)} fichiers HTML à indexer")

    for obj_name in html_objects:
        try:
            response = minio.get_object("raw-web-html", obj_name)
            content  = response.read().decode("utf-8", errors="ignore")
            meta     = get_metadata(minio, "raw-web-html", obj_name)

            doc_id   = hashlib.md5(obj_name.encode()).hexdigest()
            document = {
                "record_id"       : doc_id,
                "content"         : content[:10000],
                "university"      : university,
                "faculty"         : faculty,
                "file_type"       : "html",
                "source_url"      : meta.get("source_url", ""),
                "depth_level"     : meta.get("depth_level", 0),
                "language"        : meta.get("language", "fr"),
                "storage_path"    : f"s3://raw-web-html/{obj_name}",
                "crawl_timestamp" : meta.get("crawl_timestamp", datetime.now().isoformat())
            }
            index_document(es, doc_id, document)
            indexed += 1

        except Exception as e:
            logger.error(f"❌ Erreur HTML {obj_name} : {e}")
            errors += 1

    # ── Indexer les JSON (extractions PDF) ──
    json_objects = list_minio_objects(minio, "raw-json", university, faculty)
    logger.info(f"📄 {len(json_objects)} fichiers JSON à indexer")

    for obj_name in json_objects:
        try:
            response = minio.get_object("raw-json", obj_name)
            data     = json.loads(response.read().decode("utf-8"))

            meta    = data.get("metadata", {})
            content = data.get("text", "")

            doc_id   = hashlib.md5(obj_name.encode()).hexdigest()
            document = {
                "record_id"       : doc_id,
                "content"         : content[:10000],
                "university"      : university,
                "faculty"         : faculty,
                "file_type"       : "pdf",
                "source_url"      : meta.get("source_url", ""),
                "language"        : meta.get("language", "fr"),
                "storage_path"    : f"s3://raw-json/{obj_name}",
                "crawl_timestamp" : meta.get("crawl_timestamp", datetime.now().isoformat())
            }
            index_document(es, doc_id, document)
            indexed += 1

        except Exception as e:
            logger.error(f"❌ Erreur JSON {obj_name} : {e}")
            errors += 1

    logger.info(f"""
    ✅ Indexation terminée pour {faculty}
    ─────────────────────────────────────
    Indexés  : {indexed}
    Erreurs  : {errors}
    """)

    return {"indexed": indexed, "errors": errors}


if __name__ == "__main__":
    university = sys.argv[1] if len(sys.argv) > 1 else "hassan2"
    faculty    = sys.argv[2] if len(sys.argv) > 2 else "FSAC"
    index_to_elasticsearch(university=university, faculty=faculty)