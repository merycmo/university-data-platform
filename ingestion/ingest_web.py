import requests
import hashlib
import json
import datetime
import boto3

# Connexion à MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123"
)

def ingest_web():
    print("[INFO] Début ingestion Web...")
    
    # Page Wikipedia - stable et accessible
    url = "https://fr.wikipedia.org/wiki/Liste_des_universit%C3%A9s_au_Maroc"
    
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=10)
    
    if r.status_code != 200:
        print(f"[ERROR] Page retourne {r.status_code}")
        return
    
    raw = r.content
    checksum = hashlib.sha256(raw).hexdigest()
    
    meta = {
        "source_system": "wikipedia_universites_maroc",
        "source_url": url,
        "http_status": r.status_code,
        "extraction_timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "content_hash": checksum,
        "connector_version": "v1"
    }
    
    now = datetime.datetime.now(datetime.UTC)
    key = f"source=wikipedia/year={now.year}/month={now.month:02d}/day={now.day:02d}/{checksum}.html"
    
    s3.put_object(Bucket="raw-web-html", Key=key, Body=raw)
    s3.put_object(Bucket="raw-web-html", Key=key + ".meta.json", Body=json.dumps(meta).encode())
    
    print(f"[OK] Page HTML envoyée dans MinIO : {key}")
    print(f"[OK] Métadonnées : {key}.meta.json")

if __name__ == "__main__":
    ingest_web()