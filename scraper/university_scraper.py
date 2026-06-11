# scraper/university_scraper.py

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import hashlib
import json
import time
import logging
from datetime import datetime
from minio import Minio
from io import BytesIO

# ─────────────────────────────────────────
# Configuration MinIO
# ─────────────────────────────────────────
MINIO_HOST     = "localhost:9000"
MINIO_USER     = "admin"
MINIO_PASSWORD = "password123"

# ─────────────────────────────────────────
# Logging
# ─────────────────────────────────────────
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
# Détecter le type de fichier
# ─────────────────────────────────────────
def get_file_type(url):
    url_lower = url.lower()
    if url_lower.endswith(".pdf"):
        return "pdf"
    elif url_lower.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")):
        return "image"
    elif url_lower.endswith((".json", ".csv")):
        return "json"
    else:
        return "html"

# ─────────────────────────────────────────
# Détecter le bucket selon le type
# ─────────────────────────────────────────
def get_bucket(file_type):
    buckets = {
        "html"  : "raw-web-html",
        "pdf"   : "raw-documents",
        "image" : "raw-images",
        "json"  : "raw-json"
    }
    return buckets.get(file_type, "raw-web-html")

# ─────────────────────────────────────────
# Sauvegarder dans MinIO
# ─────────────────────────────────────────
def save_to_minio(client, content, url, university, faculty, file_type, depth):

    now         = datetime.now()
    filename    = hashlib.md5(url.encode()).hexdigest() + "." + file_type
    object_path = (
        f"university={university}/"
        f"faculty={faculty}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{filename}"
    )

    bucket   = get_bucket(file_type)
    checksum = hashlib.md5(content).hexdigest()

    # Uploader le fichier
    client.put_object(
        bucket_name  = bucket,
        object_name  = object_path,
        data         = BytesIO(content),
        length       = len(content),
        content_type = f"application/{file_type}"
    )

    # Sauvegarder les métadonnées
    metadata = {
        "source_url"       : url,
        "university"       : university,
        "faculty"          : faculty,
        "depth_level"      : depth,
        "file_type"        : file_type,
        "crawl_timestamp"  : now.isoformat(),
        "content_checksum" : checksum,
        "storage_path"     : f"s3://{bucket}/{object_path}"
    }

    meta_path    = object_path + ".meta.json"
    meta_content = json.dumps(metadata, ensure_ascii=False).encode("utf-8")

    client.put_object(
        bucket_name  = bucket,
        object_name  = meta_path,
        data         = BytesIO(meta_content),
        length       = len(meta_content),
        content_type = "application/json"
    )

    logger.info(f"✅ Sauvegardé : {url} → {bucket}/{object_path}")
    return metadata

# ─────────────────────────────────────────
# Extraire les liens d'une page HTML
# ─────────────────────────────────────────
def extract_links(html_content, base_url, allowed_domain):
    soup  = BeautifulSoup(html_content, "html.parser")
    links = set()

    for tag in soup.find_all(["a", "link"]):
        href = tag.get("href")
        if not href:
            continue

        full_url = urljoin(base_url, href)

        if urlparse(full_url).netloc != allowed_domain:
            continue

        if href.startswith("#") or href.startswith("javascript"):
            continue

        links.add(full_url)

    return links

# ─────────────────────────────────────────
# SCRAPER PRINCIPAL — GÉNÉRIQUE
# ─────────────────────────────────────────
def scrape_university(start_url, university, faculty, max_depth=3):

    logger.info(f"🚀 Début scraping : {faculty} — {start_url}")

    client         = get_minio_client()
    visited        = set()
    allowed_domain = urlparse(start_url).netloc

    queue = [(start_url, 0)]

    stats = {
        "html"   : 0,
        "pdf"    : 0,
        "image"  : 0,
        "json"   : 0,
        "errors" : 0
    }

    while queue:
        url, depth = queue.pop(0)

        if url in visited:
            continue

        if depth > max_depth:
            continue

        visited.add(url)
        file_type = get_file_type(url)

        try:
            response = requests.get(
                url,
                timeout=10,
                headers={"User-Agent": "UniversityBot/1.0"}
            )

            if response.status_code != 200:
                logger.warning(f"⚠️ Erreur {response.status_code} : {url}")
                stats["errors"] += 1
                continue

            content = response.content

            save_to_minio(
                client     = client,
                content    = content,
                url        = url,
                university = university,
                faculty    = faculty,
                file_type  = file_type,
                depth      = depth
            )

            if file_type in stats:
                stats[file_type] += 1
            else:
                stats["html"] += 1

            # Si HTML → extraire liens et continuer
            if file_type == "html" and depth < max_depth:
                links = extract_links(
                    html_content   = content,
                    base_url       = url,
                    allowed_domain = allowed_domain
                )
                for link in links:
                    if link not in visited:
                        queue.append((link, depth + 1))

        except Exception as e:
            logger.error(f"❌ Erreur sur {url} : {e}")
            stats["errors"] += 1

        time.sleep(0.5)

    logger.info(f"""
    ✅ Scraping terminé pour {faculty}
    ─────────────────────────────
    HTML collectés  : {stats['html']}
    PDFs collectés  : {stats['pdf']}
    Images          : {stats['image']}
    Erreurs         : {stats['errors']}
    Total visités   : {len(visited)}
    """)

    return stats

# ─────────────────────────────────────────
# TEST DIRECT
# ─────────────────────────────────────────
if __name__ == "__main__":
    scrape_university(
        start_url  = "https://fsac.univh2c.ma/",
        university = "hassan2",
        faculty    = "FSAC",
        max_depth  = 3
    )