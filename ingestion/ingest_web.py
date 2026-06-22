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
from .ingest_logs import info, error
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

    elif url_clean.endswith((".jpg", ".jpeg", ".png", ".gif",
                              ".webp", ".svg", ".bmp", ".tiff")):
        return "image"

    elif url_clean.endswith((".json", ".csv", ".xml", ".txt")):
        return "json"

    elif url_clean.endswith((".doc", ".docx", ".ppt", ".pptx",
                              ".xls", ".xlsx", ".odt", ".ods",
                              ".odp", ".zip", ".rar")):
        return "document"

    elif url_clean.endswith((".css", ".js", ".ico", ".map",
                              ".woff", ".ttf", ".eot", ".woff2")):
        return "skip"

    else:
        return "html"

def get_bucket(file_type):
    return {
        "html"     : "raw-web-html",
        "pdf"      : "raw-documents",
        "document" : "raw-documents",
        "image"    : "raw-images",
        "json"     : "raw-json"
    }.get(file_type, "raw-web-html")

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

    logger.info(f" Sauvegardé : {url} → {bucket}/{object_path}")
    return metadata

def extract_links(html_content, base_url, allowed_domain):
    soup  = BeautifulSoup(html_content, "html.parser")
    links = set()

    for tag in soup.find_all("a"):
        href = tag.get("href")
        if not href:
            continue
        if href.startswith(("#", "javascript", "mailto", "tel")):
            continue

        full_url  = urljoin(base_url, href)
        parsed    = urlparse(full_url)
        file_type = get_file_type(full_url)

        if parsed.netloc != allowed_domain and file_type != "image":
            continue

        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            clean_url += f"?{parsed.query}"
        links.add(clean_url)

    for tag in soup.find_all("img"):
        src = tag.get("src")
        if not src:
            continue
        full_url  = urljoin(base_url, src)
        file_type = get_file_type(full_url)
        if file_type == "image":
            links.add(full_url)

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

def fetch_with_requests(url, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=15, headers=HEADERS)
            if response.status_code == 200:
                return response.content
            logger.warning(f" Erreur {response.status_code} : {url}")
            return None
        except Exception as e:
            logger.warning(f" Tentative {attempt+1}/{retries} échouée : {url}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
            else:
                logger.error(f" Abandon après {retries} tentatives : {url}")
                return None

def fetch_with_playwright(url, retries=2):
    for attempt in range(retries):
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page    = browser.new_page()
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                time.sleep(3)
                content = page.content().encode("utf-8")
                browser.close()
                return content
        except Exception as e:
            logger.warning(f" Playwright tentative {attempt+1}/{retries} : {url}")
            if attempt < retries - 1:
                time.sleep(5)
            else:
                logger.error(f" Abandon Playwright : {url}")
                return None

def fetch_page(url, file_type):
    if file_type in ("pdf", "image", "document"):
        return fetch_with_requests(url)

    content = fetch_with_requests(url)

    if content is None:
        return None

    html_text = content.decode("utf-8", errors="ignore")

    if is_dynamic_site(html_text):
        logger.info(f" Site dynamique détecté → Playwright : {url}")
        content = fetch_with_playwright(url)

    return content

def scrape_university(start_url, university, faculty, max_depth=3):
    #log début de scraping
  try:
    info(
        message=f"Début du scraping - {faculty} | URL: {start_url}",
        university=university,
        faculty=faculty,
        source="ingest_web"
    )
    logger.info(f" Début scraping : {faculty} — {start_url}")

    client         = get_minio_client()
    visited        = set()
    allowed_domain = urlparse(start_url).netloc
    queue          = [(start_url, 0)]

    stats = {
        "html"     : 0,
        "pdf"      : 0,
        "image"    : 0,
        "document" : 0,
        "json"     : 0,
        "skip"     : 0,
        "errors"   : 0
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
            logger.error(f" Erreur sur {url} : {e}")
            stats["errors"] += 1
            #  LOG ERREUR 
            error(
                    message=f"Erreur sur {url} : {str(e)}",
                    university=university,
                    faculty=faculty,
                    source="ingest_web"
                )

            
        time.sleep(1.5)
    #log fin de scraping
    info(
        message=f"Scraping terminé - {faculty} | HTML: {stats['html']} | PDF: {stats['pdf']} | Erreurs: {stats['errors']}",
        university=university,
        faculty=faculty,
        source="ingest_web"
    )

    logger.info(f"""
     Scraping terminé pour {faculty}
    ─────────────────────────────────
    HTML collectés      : {stats['html']}
    PDFs collectés      : {stats['pdf']}
    Documents collectés : {stats['document']}
    JSON/CSV/XML/TXT    : {stats['json']}
    Images              : {stats['image']}
    Ignorés             : {stats['skip']}
    Erreurs             : {stats['errors']}
    Total visités       : {len(visited)}
    """)

    return stats
  except Exception as e:
        #  LOG ERREUR GÉNÉRALE 
        error(
            message=f"Erreur critique pendant le scraping : {str(e)}",
            university=university,
            faculty=faculty,
            source="ingest_web"
        )
        raise