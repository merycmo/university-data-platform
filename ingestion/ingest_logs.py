import json
import hashlib
from datetime import datetime
from io import BytesIO
from minio import Minio
import logging

# ========================== CONFIGURATION ==========================
MINIO_HOST      = "localhost:9000"
MINIO_USER      = "admin"
MINIO_PASSWORD  = "password123"
RAW_LOGS_BUCKET = "raw-logs"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("UniversityPlatform")


def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )


def log_to_minio(level: str, message: str, university: str, faculty: str, source: str = "unknown"):
    """
    Sauvegarde un log structuré dans le bucket raw-logs de MinIO
    """
    client = get_minio_client()
    now = datetime.now()

    log_entry = {
        "timestamp": now.isoformat(),
        "level": level.upper(),
        "message": message,
        "university": university,
        "faculty": faculty,
        "source": source
    }

    content = json.dumps(log_entry, ensure_ascii=False).encode("utf-8")
    checksum = hashlib.md5(content).hexdigest()[:8]
    filename = f"log_{checksum}.json"

    object_path = (
        f"university={university}/"
        f"faculty={faculty}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{filename}"
    )

    # Sauvegarde du log
    client.put_object(
        bucket_name=RAW_LOGS_BUCKET,
        object_name=object_path,
        data=BytesIO(content),
        length=len(content),
        content_type="application/json"
    )

    # Fichier de métadonnées
    meta = {
        "source": source,
        "university": university,
        "faculty": faculty,
        "log_timestamp": now.isoformat(),
        "level": level.upper()
    }
    meta_content = json.dumps(meta, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name=RAW_LOGS_BUCKET,
        object_name=object_path + ".meta.json",
        data=BytesIO(meta_content),
        length=len(meta_content),
        content_type="application/json"
    )

    # Affichage console
    logger.log(getattr(logging, level.upper(), logging.INFO), f"[{source}] {message}")


# ========================== Fonctions à utiliser ==========================

def info(message, university, faculty, source="unknown"):
    log_to_minio("INFO", message, university, faculty, source)

def warning(message, university, faculty, source="unknown"):
    log_to_minio("WARNING", message, university, faculty, source)

def error(message, university, faculty, source="unknown"):
    log_to_minio("ERROR", message, university, faculty, source)

def debug(message, university, faculty, source="unknown"):
    log_to_minio("DEBUG", message, university, faculty, source)