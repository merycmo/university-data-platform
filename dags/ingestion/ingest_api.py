import requests
import json
import time
from datetime import datetime
from minio import Minio
from io import BytesIO
import hashlib
import logging
import schedule

# ============================================
# CONFIGURATION
# ============================================
BASE_URL = "https://api.openalex.org"
HEADERS = {"User-Agent": "mailto:salmaaouzale@gmail.com"}

MINIO_HOST     = "localhost:9000"
MINIO_USER     = "admin"
MINIO_PASSWORD = "password123"
MINIO_BUCKET   = "raw-json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CONNEXION MINIO
# ============================================
def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

# ============================================
# SAUVEGARDE JSON DANS MINIO
# ============================================
def save_to_minio(data, university, faculty, data_type):
    try:
        client = get_minio_client()
        now = datetime.now()
        content = json.dumps(data, ensure_ascii=False).encode("utf-8")
        checksum = hashlib.md5(content).hexdigest()[:8]

        object_path = (
            f"university={university.lower().replace(' ', '_')}/"
            f"faculty={faculty}/"
            f"type={data_type}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"{data_type}_{checksum}.json"
        )

        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_path,
            data=BytesIO(content),
            length=len(content),
            content_type="application/json"
        )

        metadata = {
            "source": "openalex",
            "university": university,
            "faculty": faculty,
            "data_type": data_type,
            "crawl_timestamp": now.isoformat(),
            "records_count": len(data)
        }

        meta_content = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_path + ".meta.json",
            data=BytesIO(meta_content),
            length=len(meta_content),
            content_type="application/json"
        )

        logger.info(f"Sauvegarde MinIO reussie : {object_path}")
        return True
    except Exception as e:
        logger.error(f"Erreur MinIO : {e}")
        return False

# ============================================
# RECUPERATION ID UNIVERSITE
# ============================================
def get_institution_id(university_name):
    url = f"https://api.openalex.org/institutions?search={university_name}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                inst_id = results[0]["id"].split("/")[-1]
                print(f"ID trouve pour {university_name} : {inst_id}")
                return inst_id
    except:
        pass
    print(f"ID non trouve pour {university_name}")
    return None

# ============================================
# RECUPERATION AUTEURS (max 500)
# ============================================
def get_authors_by_faculty(university_name, faculty_name, max_authors=500):
    inst_id = get_institution_id(university_name)
    if not inst_id:
        return []

    authors = []
    page = 1
    while len(authors) < max_authors:
        url = f"{BASE_URL}/authors?filter=last_known_institutions.id:{inst_id}&per_page=200&page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                break
            results = r.json().get("results", [])
            if not results:
                break
            authors.extend(results)
            if len(authors) >= max_authors:
                break
            page += 1
            time.sleep(0.5)
        except:
            break
    return authors[:max_authors]

# ============================================
# RECUPERATION PUBLICATIONS (max 5000)
# ============================================
def get_publications_by_faculty(university_name, faculty_name, max_works=5000):
    inst_id = get_institution_id(university_name)
    if not inst_id:
        return []

    publications = []
    page = 1
    while len(publications) < max_works:
        url = f"{BASE_URL}/works?filter=authorships.institutions.id:{inst_id}&per_page=200&page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                break
            results = r.json().get("results", [])
            if not results:
                break
            publications.extend(results)
            if len(publications) >= max_works:
                break
            page += 1
            time.sleep(0.5)
        except:
            break
    return publications[:max_works]

# ============================================
# ENRICHIR LES PUBLICATIONS
# ============================================
def enrich_publications(works, university_name, faculty_name):
    enriched = []
    for w in works:
        enriched.append({
            "work_id": w.get("id"),
            "title": w.get("display_name"),
            "university": university_name,
            "faculty": faculty_name,
            "publication_year": w.get("publication_year"),
            "cited_by_count": w.get("cited_by_count", 0),
            "type": w.get("type"),
            "doi": w.get("doi"),
            "open_access": w.get("open_access", {}).get("is_oa", False),
            "retrieved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    return enriched

# ============================================
# SAUVEGARDE JSON LOCALE
# ============================================
def save_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"JSON cree : {filename} ({len(data)} enregistrements)")

# ============================================
# FONCTION D'INGESTION
# ============================================
def ingest_faculty(university_name, faculty_name):
    print(f"\n{'='*70}")
    print(f"INGESTION PROGRAMMEE - {faculty_name} ({university_name})")
    print(f"{'='*70}\n")

    authors = get_authors_by_faculty(university_name, faculty_name)
    publications = get_publications_by_faculty(university_name, faculty_name)
    enriched_pub = enrich_publications(publications, university_name, faculty_name)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    save_json(authors, f"authors_{faculty_name}_{timestamp}.json")
    save_json(enriched_pub, f"publications_{faculty_name}_{timestamp}.json")

    save_to_minio(authors, university_name, faculty_name, "authors")
    save_to_minio(enriched_pub, university_name, faculty_name, "publications")

    print(f"Termine -> {len(authors)} auteurs | {len(enriched_pub)} publications\n")

# ============================================
# FONCTION PRINCIPALE AVEC SCHEDULER
# ============================================
def run_scheduled_ingestion():
    # Exécution immédiate
    ingest_faculty("Hassan II", "FSAC")
    ingest_faculty("Hassan II", "FLSH")          # ← Modifié
    ingest_faculty("Hassan II", "FST")
    ingest_faculty("Cadi Ayyad", "FSJES")
    ingest_faculty("Cadi Ayyad", "FSTG")
    ingest_faculty("Cadi Ayyad", "FSSM")

    # Planification horaire
    schedule.every().hour.do(lambda: ingest_faculty("Hassan II", "FSAC"))
    schedule.every().hour.do(lambda: ingest_faculty("Hassan II", "FLSH"))   # ← Modifié
    schedule.every().hour.do(lambda: ingest_faculty("Hassan II", "FST"))
    schedule.every().hour.do(lambda: ingest_faculty("Cadi Ayyad", "FSJES"))
    schedule.every().hour.do(lambda: ingest_faculty("Cadi Ayyad", "FSTG"))
    schedule.every().hour.do(lambda: ingest_faculty("Cadi Ayyad", "FSSM"))

    print("Lancement de la collecte horaire OpenAlex...")
    while True:
        schedule.run_pending()
        time.sleep(1)

# ============================================
# EXECUTION
# ============================================
if __name__ == "__main__":
    ingest_faculty("Cadi Ayyad", "FSSM")