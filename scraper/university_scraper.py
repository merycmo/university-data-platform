# scraper/university_scraper.py

import hashlib
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse
from minio import Minio
from io import BytesIO

# ─────────────────────────────────────────
# Configuration MinIO
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
# Détecter le type de fichier
# ─────────────────────────────────────────
def get_file_type(url):
    url_clean = url.lower().split("?")[0].split("#")[0]
    if url_clean.endswith(".pdf"):
        return "pdf"
    elif url_clean.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg")):
        return "image"
    elif url_clean.endswith((".json", ".csv")):
        return "json"
    elif url_clean.endswith((".css", ".js", ".ico", ".xml", ".txt", ".map", ".woff", ".ttf")):
        return "skip"
    else:
        return "html"

# ─────────────────────────────────────────
# Détecter le bucket
# ─────────────────────────────────────────
def get_bucket(file_type):
    return {
        "html"  : "raw-web-html",
        "pdf"   : "raw-documents",
        "image" : "raw-images",
        "json"  : "raw-json"
    }.get(file_type, "raw-web-html")

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

    client.put_object(
        bucket_name  = bucket,
        object_name  = object_path,
        data         = BytesIO(content),
        length       = len(content),
        content_type = f"application/{file_type}"
    )

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

    meta_content = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name  = bucket,
        object_name  = object_path + ".meta.json",
        data         = BytesIO(meta_content),
        length       = len(meta_content),
        content_type = "application/json"
    )

    logger.info(f"✅ Sauvegardé : {url} → {bucket}/{object_path}")
    return metadata

# ─────────────────────────────────────────
# Extraire les liens depuis le HTML
# ─────────────────────────────────────────
def extract_links(html_content, base_url, allowed_domain):
    soup  = BeautifulSoup(html_content, "html.parser")
    links = set()

    allowed_domains = {
        allowed_domain,
        allowed_domain.replace("www.", ""),
        "www." + allowed_domain.replace("www.", "")
    }

    # ← Chercher <a href> ET <img src>
    for tag in soup.find_all(["a", "link"]):
        href = tag.get("href")
        if not href:
            continue
        if href.startswith(("#", "javascript", "mailto", "tel")):
            continue

        full_url = urljoin(base_url, href)
        parsed   = urlparse(full_url)

        if parsed.netloc not in allowed_domains:
            continue

        clean_url = f"{parsed.scheme}://{allowed_domain}{parsed.path}"
        if parsed.query:
            clean_url += f"?{parsed.query}"
        links.add(clean_url)

    # ← Ajouter les images <img src>
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue

        full_url = urljoin(base_url, src)
        parsed   = urlparse(full_url)

        if parsed.netloc not in allowed_domains and parsed.netloc != "":
            continue

        clean_url = f"{parsed.scheme}://{allowed_domain}{parsed.path}"
        links.add(clean_url)

    return links

# ─────────────────────────────────────────
# SCRAPER PRINCIPAL — GÉNÉRIQUE
# ─────────────────────────────────────────
def scrape_university(start_url, university, faculty, max_depth=2):
    logger.info(f"🚀 Début scraping récursif : {faculty} — {start_url}")

    from playwright.sync_api import sync_playwright

    client         = get_minio_client()
    visited        = set()
    allowed_domain = urlparse(start_url).netloc
    queue          = [(start_url, 0)]

    stats = {
        "html"   : 0,
        "pdf"    : 0,
        "image"  : 0,
        "skip"   : 0,
        "errors" : 0
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless = False,
            args     = [
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
            ]
        )
        context = browser.new_context(
            user_agent          = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport            = {"width": 1280, "height": 720},
            ignore_https_errors = True
        )

        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['fr-FR', 'fr'] });
        """)

        page = context.new_page()

        while queue:
            url, depth = queue.pop(0)

            if url in visited:
                continue
            if depth > max_depth:
                continue

            visited.add(url)
            file_type = get_file_type(url)

            if file_type == "skip":
                stats["skip"] += 1
                continue

            try:
                logger.info(f"🔄 Traitement ({depth}/{max_depth}) : {url}")

                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                content = page.content().encode("utf-8")

                if not content:
                    stats["errors"] += 1
                    continue

                save_to_minio(
                    client     = client,
                    content    = content,
                    url        = url,
                    university = university,
                    faculty    = faculty,
                    file_type  = file_type,
                    depth      = depth
                )

                stats[file_type if file_type in stats else "html"] += 1

                if file_type == "html" and depth < max_depth:
                    extracted_links = extract_links(
                        html_content   = content,
                        base_url       = url,
                        allowed_domain = allowed_domain
                    )
                    for link in extracted_links:
                        if link not in visited:
                            queue.append((link, depth + 1))

                    logger.info(f"🔗 {len(extracted_links)} liens découverts")

            except Exception as e:
                logger.error(f"❌ Erreur sur {url} : {e}")
                stats["errors"] += 1

            time.sleep(0.5)

        browser.close()

    logger.info(f"""
    ✅ Scraping Récursif Terminé pour {faculty}
    ─────────────────────────────────────────────
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
        start_url  = "https://www.fsbm.ma/",
        university = "hassan2",
        faculty    = "FSBM",
        max_depth  = 3
    )