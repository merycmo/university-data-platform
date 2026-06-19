# ingestion/ingest_file.py

import hashlib
import json
import logging
import time
from datetime import datetime
from io import BytesIO

import pdfplumber
from docx import Document
from minio import Minio

MINIO_HOST     = "localhost:9000"
MINIO_USER     = "admin"
MINIO_PASSWORD = "password123"

SOURCE_BUCKET  = "raw-documents"
TARGET_BUCKET  = "raw-json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD, 
        secure=False
    )

def extract_text_pdf(content):
    try:
        with pdfplumber.open(BytesIO(content)) as pdf:
            text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text.strip()
    except Exception as e:
        logger.error(f"❌ Erreur extraction PDF : {e}")
        return ""

def extract_text_docx(content):
    try:
        doc  = Document(BytesIO(content))
        text = "\n".join([para.text for para in doc.paragraphs if para.text])
        return text.strip()
    except Exception as e:
        logger.error(f"❌ Erreur extraction DOCX : {e}")
        return ""

def save_extracted(client, text, original_path, original_metadata):
    if not text:
        logger.warning(f"⚠️ Texte vide, ignoré : {original_path}")
        return None

    now      = datetime.now()
    checksum = hashlib.md5(text.encode()).hexdigest()

    object_path = original_path.replace(".pdf", ".json") \
                               .replace(".docx", ".json") \
                               .replace(".doc", ".json")

    payload = {
        "metadata": {
            "source_bucket"    : SOURCE_BUCKET,
            "source_path"      : original_path,
            "source_url"       : original_metadata.get("source_url", ""),
            "university"       : original_metadata.get("university", ""),
            "faculty"          : original_metadata.get("faculty", ""),
            "extraction_type"  : "text_extraction",
            "crawl_timestamp"  : original_metadata.get("crawl_timestamp", ""),
            "extract_timestamp": now.isoformat(),
            "content_checksum" : checksum,
            "storage_path"     : f"s3://{TARGET_BUCKET}/{object_path}"
        },
        "text": text
    }

    content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    client.put_object(
        bucket_name  = TARGET_BUCKET,
        object_name  = object_path,
        data         = BytesIO(content),
        length       = len(content),
        content_type = "application/json"
    )

    logger.info(f"✅ Texte extrait : {original_path} → {TARGET_BUCKET}/{object_path}")
    return object_path

def get_metadata(client, object_path):
    try:
        meta_path = object_path + ".meta.json"
        response  = client.get_object(SOURCE_BUCKET, meta_path)
        return json.loads(response.read().decode("utf-8"))
    except:
        return {}


def run_file_ingestion(university="hassan2", faculty="FLSH"):



    client = get_minio_client()
    logger.info(f"🚀 Début extraction texte — {faculty}")

    objects = client.list_objects(
        SOURCE_BUCKET,
        prefix    = f"university={university}/faculty={faculty}/",
        recursive = True
    )

    stats = {
        "pdf"    : 0,
        "docx"   : 0,
        "skipped": 0,
        "errors" : 0
    }

    for obj in objects:
        path = obj.object_name

        if path.endswith(".meta.json"):
            continue

        try:
            response = client.get_object(SOURCE_BUCKET, path)
            content  = response.read()

            if path.endswith(".pdf"):
                text      = extract_text_pdf(content)
                file_type = "pdf"

            elif path.endswith((".docx", ".doc")):
                text      = extract_text_docx(content)
                file_type = "docx"

            else:
                stats["skipped"] += 1
                continue

            metadata = get_metadata(client, path)

            result = save_extracted(client, text, path, metadata)

            if result:
                stats[file_type] += 1
            else:
                stats["skipped"] += 1

        except Exception as e:
            logger.error(f"❌ Erreur sur {path} : {e}")
            stats["errors"] += 1

        time.sleep(0.2)

    logger.info(f"""
    ✅ Extraction terminée pour {faculty}
    ─────────────────────────────────────
    PDFs extraits   : {stats['pdf']}
    DOCXs extraits  : {stats['docx']}
    Ignorés         : {stats['skipped']}
    Erreurs         : {stats['errors']}
    """)

    return stats

if __name__ == "__main__":

    run_file_ingestion(university="hassan2", faculty="FLSH")


