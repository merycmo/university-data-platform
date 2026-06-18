# ingestion/ingest_file.py

import hashlib
import json
import logging
from datetime import datetime
from io import BytesIO
from minio import Minio
import PyPDF2

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
MINIO_HOST     = "localhost:9000"
MINIO_USER     = "admin"
MINIO_PASSWORD = "password123"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Connexion MinIO
# ─────────────────────────────────────────
def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

# ─────────────────────────────────────────
# Extraire le texte d'un PDF
# ─────────────────────────────────────────
def extract_pdf_text(pdf_content):
    try:
        reader = PyPDF2.PdfReader(BytesIO(pdf_content))
        text   = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text.strip()
    except Exception as e:
        logger.error(f"❌ Erreur extraction PDF : {e}")
        return ""

# ─────────────────────────────────────────
# Traiter les PDFs depuis MinIO
# ─────────────────────────────────────────
def process_pdfs(university, faculty):

    logger.info(f"🚀 Traitement PDFs : {faculty} — {university}")

    client   = get_minio_client()
    prefix   = f"university={university}/faculty={faculty}/"
    objects  = client.list_objects(
        "raw-documents",
        prefix    = prefix,
        recursive = True
    )

    processed = 0
    errors    = 0

    for obj in objects:

        if not obj.object_name.endswith(".pdf"):
            continue

        try:
            # Lire le PDF depuis MinIO
            response    = client.get_object("raw-documents", obj.object_name)
            pdf_content = response.read()

            # Extraire le texte
            text = extract_pdf_text(pdf_content)

            if not text:
                logger.warning(f"⚠️ PDF vide : {obj.object_name}")
                continue

            # Créer le document enrichi
            now      = datetime.now()
            checksum = hashlib.md5(pdf_content).hexdigest()

            enriched = {
                "source_path"      : obj.object_name,
                "university"       : university,
                "faculty"          : faculty,
                "file_type"        : "pdf",
                "extracted_text"   : text[:50000],
                "text_length"      : len(text),
                "content_checksum" : checksum,
                "processed_at"     : now.isoformat()
            }

            # Sauvegarder le texte extrait dans raw-json
            enriched_content = json.dumps(
                enriched,
                ensure_ascii=False
            ).encode("utf-8")

            output_path = (
                f"university={university}/"
                f"faculty={faculty}/"
                f"year={now.year}/month={now.month:02d}/"
                f"pdf_extracted_{checksum[:8]}.json"
            )

            client.put_object(
                bucket_name  = "raw-json",
                object_name  = output_path,
                data         = BytesIO(enriched_content),
                length       = len(enriched_content),
                content_type = "application/json"
            )

            logger.info(f"✅ PDF traité : {obj.object_name}")
            processed += 1

        except Exception as e:
            logger.error(f"❌ Erreur PDF {obj.object_name} : {e}")
            errors += 1

    logger.info(f"""
    ✅ Traitement PDFs terminé pour {faculty}
    ─────────────────────────────────────────
    PDFs traités : {processed}
    Erreurs      : {errors}
    """)

    return {"processed": processed, "errors": errors}

# ─────────────────────────────────────────
# TEST DIRECT
# ─────────────────────────────────────────
if __name__ == "__main__":
    process_pdfs(
        university = "cadi_ayyad",
        faculty    = "FSSM"
    )