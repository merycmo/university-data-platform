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

def ingest_file():
    print("[INFO] Début ingestion Fichier CSV...")
    
    # Fichier CSV open data Maroc - établissements enseignement supérieur
    url = "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv"
    
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=10)
    
    if r.status_code != 200:
        print(f"[ERROR] Fichier retourne {r.status_code}")
        return
    
    raw = r.content
    checksum = hashlib.sha256(raw).hexdigest()
    
    meta = {
        "source_system": "open_data_csv",
        "source_url": url,
        "http_status": r.status_code,
        "extraction_timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "content_hash": checksum,
        "connector_version": "v1",
        "file_type": "csv"
    }
    
    now = datetime.datetime.now(datetime.UTC)
    key = f"source=opendata/year={now.year}/month={now.month:02d}/day={now.day:02d}/{checksum}.csv"
    
    s3.put_object(Bucket="raw-documents", Key=key, Body=raw)
    s3.put_object(Bucket="raw-documents", Key=key + ".meta.json", Body=json.dumps(meta).encode())
    
    print(f"[OK] Fichier CSV envoyé dans MinIO : {key}")
    print(f"[OK] Métadonnées : {key}.meta.json")

if __name__ == "__main__":
    ingest_file()