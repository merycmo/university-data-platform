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

def ingest_api():
    print("[INFO] Début ingestion API OpenAlex...")
    
    # Appel API OpenAlex - publications universitaires Maroc
    url = "https://api.openalex.org/works?filter=institutions.country_code:MA&per-page=50"
    r = requests.get(url)
    
    if r.status_code != 200:
        print(f"[ERROR] API retourne {r.status_code}")
        return
    
    raw = r.content
    checksum = hashlib.sha256(raw).hexdigest()
    
    # Métadonnées
    meta = {
        "source_system": "openalex_api",
        "source_url": url,
        "http_status": r.status_code,
        "extraction_timestamp": datetime.datetime.utcnow().isoformat(),
        "content_hash": checksum,
        "connector_version": "v1"
    }
    
    # Chemin daté dans MinIO
    now = datetime.datetime.utcnow()
    key = f"source=openalex/year={now.year}/month={now.month:02d}/day={now.day:02d}/{checksum}.json"
    
    # Envoi dans MinIO
    s3.put_object(Bucket="raw-json", Key=key, Body=raw)
    s3.put_object(Bucket="raw-json", Key=key + ".meta.json", Body=json.dumps(meta).encode())
    
    print(f"[OK] Données envoyées dans MinIO : {key}")
    print(f"[OK] Métadonnées : {key}.meta.json")

if __name__ == "__main__":
    ingest_api()