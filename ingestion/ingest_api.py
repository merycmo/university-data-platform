# ingestion/ingest_api.py

import requests
import json
import logging
import hashlib
from datetime import datetime
from minio import Minio
from io import BytesIO

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
# Sauvegarder JSON dans MinIO
# ─────────────────────────────────────────
def save_json_to_minio(client, data, university, faculty, source):

    now         = datetime.now()
    content     = json.dumps(data, ensure_ascii=False).encode("utf-8")
    checksum    = hashlib.md5(content).hexdigest()
    filename    = f"{source}_{checksum[:8]}.json"
    object_path = (
        f"university={university}/"
        f"faculty={faculty}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{filename}"
    )

    client.put_object(
        bucket_name  = "raw-json",
        object_name  = object_path,
        data         = BytesIO(content),
        length       = len(content),
        content_type = "application/json"
    )

    metadata = {
        "source"           : source,
        "university"       : university,
        "faculty"          : faculty,
        "crawl_timestamp"  : now.isoformat(),
        "content_checksum" : checksum,
        "storage_path"     : f"s3://raw-json/{object_path}",
        "records_count"    : len(data) if isinstance(data, list) else 1
    }

    meta_content = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name  = "raw-json",
        object_name  = object_path + ".meta.json",
        data         = BytesIO(meta_content),
        length       = len(meta_content),
        content_type = "application/json"
    )

    logger.info(f"✅ Sauvegardé dans raw-json : {object_path}")
    return metadata

# ─────────────────────────────────────────
# Appel API OpenAlex — Auteurs
# ─────────────────────────────────────────
def fetch_openalex_authors(university_name, faculty_name, max_results=100):

    logger.info(f"🔍 OpenAlex auteurs : {faculty_name}")

    authors  = []
    page     = 1
    per_page = 25

    while len(authors) < max_results:
        try:
            response = requests.get(
                "https://api.openalex.org/authors",
                params={
                    "search"   : university_name,
                    "per_page" : per_page,
                    "page"     : page,
                    "mailto"   : "university@ma.ma"
                },
                timeout=15
            )

            if response.status_code != 200:
                logger.warning(f"⚠️ Erreur {response.status_code}")
                logger.warning(f"⚠️ Réponse : {response.text[:200]}")
                break

            data    = response.json()
            results = data.get("results", [])

            if not results:
                break

            for author in results:
                authors.append({
                    "openalex_id"        : author.get("id", ""),
                    "name"               : author.get("display_name", ""),
                    "orcid"              : author.get("orcid", ""),
                    "publications_count" : author.get("works_count", 0),
                    "citations_count"    : author.get("cited_by_count", 0),
                    "research_topics"    : [
                        t.get("display_name", "")
                        for t in author.get("topics", [])[:5]
                    ],
                    "university"         : university_name,
                    "faculty"            : faculty_name,
                    "source"             : "openalex",
                    "fetched_at"         : datetime.now().isoformat()
                })

            logger.info(f"📄 Page {page} → {len(results)} auteurs")
            page += 1

            if len(results) < per_page:
                break

        except Exception as e:
            logger.error(f"❌ Erreur : {e}")
            break

    logger.info(f"✅ {len(authors)} auteurs récupérés")
    return authors

# ─────────────────────────────────────────
# Appel API OpenAlex — Publications
# ─────────────────────────────────────────
def fetch_openalex_publications(university_name, faculty_name, max_results=200):

    logger.info(f"📚 OpenAlex publications : {faculty_name}")

    publications = []
    page         = 1
    per_page     = 25

    while len(publications) < max_results:
        try:
            response = requests.get(
                "https://api.openalex.org/works",
                params={
                    "search"   : f"{university_name} Casablanca",
                    "per_page" : per_page,
                    "page"     : page,
                    "mailto"   : "university@ma.ma"
                },
                timeout=15
            )

            if response.status_code != 200:
                logger.warning(f"⚠️ Erreur {response.status_code}")
                logger.warning(f"⚠️ Réponse : {response.text[:200]}")
                break

            data    = response.json()
            results = data.get("results", [])

            if not results:
                break

            for work in results:
                publications.append({
                    "openalex_id"     : work.get("id", ""),
                    "title"           : work.get("title", ""),
                    "publication_year": work.get("publication_year", ""),
                    "doi"             : work.get("doi", ""),
                    "citations_count" : work.get("cited_by_count", 0),
                    "type"            : work.get("type", ""),
                    "topics"          : [
                        t.get("display_name", "")
                        for t in work.get("topics", [])[:5]
                    ],
                    "authors"         : [
                        a.get("author", {}).get("display_name", "")
                        for a in work.get("authorships", [])[:5]
                    ],
                    "university"      : university_name,
                    "faculty"         : faculty_name,
                    "source"          : "openalex",
                    "fetched_at"      : datetime.now().isoformat()
                })

            logger.info(f"📄 Page {page} → {len(results)} publications")
            page += 1

            if len(results) < per_page:
                break

        except Exception as e:
            logger.error(f"❌ Erreur : {e}")
            break

    logger.info(f"✅ {len(publications)} publications récupérées")
    return publications

# ─────────────────────────────────────────
# FONCTION PRINCIPALE
# ─────────────────────────────────────────
def fetch_openalex(university_name, faculty_name):

    logger.info(f"🚀 Fetch API : {faculty_name} — {university_name}")

    client = get_minio_client()

    # Auteurs
    authors = fetch_openalex_authors(university_name, faculty_name)
    if authors:
        save_json_to_minio(
            client     = client,
            data       = authors,
            university = university_name.lower().replace(" ", "_"),
            faculty    = faculty_name,
            source     = "openalex_authors"
        )

    # Publications
    publications = fetch_openalex_publications(university_name, faculty_name)
    if publications:
        save_json_to_minio(
            client     = client,
            data       = publications,
            university = university_name.lower().replace(" ", "_"),
            faculty    = faculty_name,
            source     = "openalex_publications"
        )

    stats = {
        "authors"      : len(authors),
        "publications" : len(publications)
    }

    logger.info(f"""
    ✅ Fetch API terminé pour {faculty_name}
    ──────────────────────────────────────
    Auteurs        : {stats['authors']}
    Publications   : {stats['publications']}
    """)

    return stats

# ─────────────────────────────────────────
# TEST DIRECT
# ─────────────────────────────────────────
if __name__ == "__main__":
    fetch_openalex(
        university_name = "Hassan II",
        faculty_name    = "FSAC"
    )