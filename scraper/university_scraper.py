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
    # Nettoyer les paramètres de l'URL
    url_clean = url_lower.split("?")[0].split("#")[0]

    if url_clean.endswith(".pdf"):
        return "pdf"
    elif url_clean.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg")):
        return "image"
    elif url_clean.endswith((".json", ".csv")):
        return "json"
    elif url_clean.endswith((".css", ".js", ".ico", ".xml", ".txt", ".map", ".woff", ".ttf", ".eot")):
        return "skip"
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

    for tag in soup.find_all(["a"]):
        href = tag.get("href")
        if not href:
            continue

        # Ignorer les ancres et javascript
        if href.startswith("#"):
            continue
        if href.startswith("javascript"):
            continue
        if href.startswith("mailto"):
            continue
        if href.startswith("tel"):
            continue

        # Construire l'URL absolue
        full_url = urljoin(base_url, href)

        # Rester sur le même domaine
        parsed = urlparse(full_url)
        if parsed.netloc != allowed_domain:
            continue

        # Nettoyer l'URL
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            clean_url += f"?{parsed.query}"

        links.add(clean_url)

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
        "skip"   : 0,
        "errors" : 0
    }

    while queue:
        url, depth = queue.pop(0)

        # STOP si déjà visité
        if url in visited:
            continue

        # STOP si niveau > max_depth
        if depth > max_depth:
            continue

        visited.add(url)
        file_type = get_file_type(url)

        # Ignorer CSS JS ICO etc
        if file_type == "skip":
            stats["skip"] += 1
            logger.info(f"⏭️ Ignoré : {url}")
            continue

        try:
            response = requests.get(
                url,
                timeout=10,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; UniversityBot/1.0)"
                }
            )

            if response.status_code != 200:
                logger.warning(f"⚠️ Erreur {response.status_code} : {url}")
                stats["errors"] += 1
                continue

            # Vérifier le content-type
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type and file_type == "html":
                logger.info(f"⏭️ Pas HTML ({content_type}) : {url}")
                stats["skip"] += 1
                continue

            content = response.content

            # Sauvegarder dans MinIO
            save_to_minio(
                client     = client,
                content    = content,
                url        = url,
                university = university,
                faculty    = faculty,
                file_type  = file_type,
                depth      = depth
            )

            stats[file_type] += 1

            # Si HTML → extraire les liens et continuer
            if file_type == "html" and depth < max_depth:
                links = extract_links(
                    html_content   = content,
                    base_url       = url,
                    allowed_domain = allowed_domain
                )
                for link in links:
                    if link not in visited:
                        queue.append((link, depth + 1))
                logger.info(f"🔗 {len(links)} liens trouvés sur {url}")

        except Exception as e:
            logger.error(f"❌ Erreur sur {url} : {e}")
            stats["errors"] += 1

        # Délai entre requêtes
        time.sleep(0.5)

    logger.info(f"""
    ✅ Scraping terminé pour {faculty}
    ─────────────────────────────────
    HTML collectés  : {stats['html']}
    PDFs collectés  : {stats['pdf']}
    Images          : {stats['image']}
    Ignorés         : {stats['skip']}
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