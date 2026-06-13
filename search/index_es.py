# search/index_es.py

from elasticsearch import Elasticsearch
from minio import Minio
import json
import logging
import hashlib
from datetime import datetime
from io import BytesIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
ES_HOST        = "localhost"
ES_PORT        = 9200
MINIO_HOST     = "localhost:9000"
MINIO_USER     = "admin"
MINIO_PASSWORD = "password123"
INDEX_NAME     = "university_content"

# ─────────────────────────────────────────
# Connexions
# ─────────────────────────────────────────
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

# ─────────────────────────────────────────
# Créer l'index si il n'existe pas
# ─────────────────────────────────────────
def create_index(es):
    if es.indices.exists(index=INDEX_NAME):
        logger.info(f"✅ Index {INDEX_NAME} existe déjà")
        return

    with open("search/mapping.json", "r") as f:
        mapping = json.load(f)

    es.indices.create(index=INDEX_NAME, body=mapping)
    logger.info(f"✅ Index {INDEX_NAME} créé")

# ─────────────────────────────────────────
# Lire les fichiers depuis MinIO
# ─────────────────────────────────────────
def list_minio_objects(client, bucket, university, faculty):
    prefix = f"university={university}/faculty={faculty}/"
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects
            if not obj.object_name.endswith(".meta.json")]

# ─────────────────────────────────────────
# Indexer un document
# ─────────────────────────────────────────
def index_document(es, doc_id, document):
    es.index(
        index    = INDEX_NAME,
        id       = doc_id,
        document = document
    )

# ─────────────────────────────────────────
# FONCTION PRINCIPALE
# ─────────────────────────────────────────
def index_to_elasticsearch(university, faculty):

    logger.info(f"🚀 Indexation ES : {faculty} — {university}")

    es     = get_es_client()
    minio  = get_minio_client()

    create_index(es)

    indexed = 0
    errors  = 0

    # Indexer les HTML
    html_objects = list_minio_objects(
        minio, "raw-web-html", university, faculty
    )

    for obj_name in html_objects:
        try:
            response = minio.get_object("raw-web-html", obj_name)
            content  = response.read().decode("utf-8", errors="ignore")

            doc_id   = hashlib.md5(obj_name.encode()).hexdigest()
            document = {
                "record_id"       : doc_id,
                "content"         : content[:10000],
                "university"      : university,
                "faculty"         : faculty,
                "file_type"       : "html",
                "storage_path"    : f"s3://raw-web-html/{obj_name}",
                "crawl_timestamp" : datetime.now().isoformat()
            }