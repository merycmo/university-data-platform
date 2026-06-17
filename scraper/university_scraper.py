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

MINIO_HOST     = "localhost:9000"
MINIO_USER     = "admin"
MINIO_PASSWORD = "password123"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UniversityBot/1.0)"}

def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

def get_file_type(url):
    url_clean = url.lower().split("?")[0].split("#")[0]
    if url_clean.endswith(".pdf"):
        return "pdf"
    elif url_clean.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg")):
        return "image"
    elif url_clean.endswith((".json", ".csv")):
        return "json"
    elif url_clean.endswith((".css", ".js", ".ico", ".xml", ".txt", ".map", ".woff", ".ttf", ".doc", ".docx")):
        return "skip"
    else:
        return "html"

def get_bucket(file_type):
    return {
        "html"  : "raw-web-html",
        "pdf"   : "raw-documents",
        "image" : "raw-images",
        "json"  : "raw-json"
    }.get(file_type, "raw-web-html")

def get_extension(file_type):
    return {
        "html"  : "html",
        "pdf"   : "pdf",
        "image" : "jpg",
        "json"  : "json"
    }.get(file_type, "html")

def save_to_minio(client, content, url, university, faculty, file_type, depth):
    now         = datetime.now()
    ext         = get_extension(file_type)
    filename    = hashlib.md5(url.encode()).hexdigest() + "." + ext
    object_path = (
        f"university={university}/"
        f"faculty={faculty}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{filename}"
    )
    bucket   = get_bucket(file_type)
    checksum = hashlib.md5(content).hexdigest()

    content_type_map = {
        "html"  : "text/html",
        "pdf"   : "application/pdf",
        "image" : "image/jpeg",
        "json"  : "application/json"
    }

    client.put_object(
        bucket_name  = bucket,
        object_name  = object_path,
        data         = BytesIO(content),
        length       = len(content),
        content_type = content_type_map.get(file_type, "application/octet-stream")
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

def extract_links(html_content, base_url, allowed_domain):
    soup  = BeautifulSoup(html_content, "html.parser")
    links = set()

    # ── liens <a> ──────────────────────────────────────────
    for tag in soup.find_all("a"):
        href = tag.get("href")
        if not href:
            continue
        if href.startswith(("#", "javascript", "mailto", "tel")):
            continue

        full_url = urljoin(base_url, href)
        parsed   = urlparse(full_url)

        if parsed.netloc != allowed_domain:
            continue

        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            clean_url += f"?{parsed.query}"
        links.add(clean_url)

    # ── images <img> et <source> ───────────────────────────
    for tag in soup.find_all(["img", "source"]):
        # supporte src, data-src (lazy loading), et srcset
        src = (
            tag.get("src")
            or tag.get("data-src")
            or tag.get("data-lazy-src")
            or (tag.get("srcset", "").split()[0] if tag.get("srcset") else None)
        )
        if not src or src.startswith("data:"):  # ignore base64 inline
            continue

        full_url = urljoin(base_url, src)
        parsed   = urlparse(full_url)

        if parsed.netloc != allowed_domain:
            continue

        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        links.add(clean_url)

    return links

def is_dynamic_site(html_content):
    dynamic_indicators = [
        "callWS", "getTabs", "getPage",
        "angular", "react", "vue",
        "$(document).ready"
    ]
    for indicator in dynamic_indicators:
        if indicator in html_content:
            return True
    return False

def fetch_with_requests(url):
    try:
        response = requests.get(url, timeout=15, headers=HEADERS)
        if response.status_code == 200:
            return response.content
        logger.warning(f"⚠️ Erreur {response.status_code} : {url}")
        return None
    except Exception as e:
        logger.error(f"❌ Erreur requests {url} : {e}")
        return None

def fetch_with_playwright(url):
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page    = browser.new_page()
            page.goto(url, timeout=30000, wait_until="networkidle")
            time.sleep(5)
            content = page.content().encode("utf-8")
            browser.close()
            return content
    except Exception as e:
        logger.error(f"❌ Erreur Playwright {url} : {e}")
        return None

def fetch_page(url, file_type):
    # PDF et images → requests direct, pas besoin de JS
    if file_type in ("pdf", "image"):
        return fetch_with_requests(url)

    content = fetch_with_requests(url)

    if content is None:
        return None

    html_text = content.decode("utf-8", errors="ignore")

    if is_dynamic_site(html_text):
        logger.info(f"🔄 Site dynamique détecté → Playwright : {url}")
        content = fetch_with_playwright(url)

    return content

def scrape_university(start_url, university, faculty, max_depth=3):

    logger.info(f"🚀 Début scraping : {faculty} — {start_url}")

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
            content = fetch_page(url, file_type)

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

            # On n'extrait des liens que depuis les pages HTML
            if file_type == "html" and depth < max_depth:
                html_text = content.decode("utf-8", errors="ignore")
                links     = extract_links(
                    html_content   = html_text,
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