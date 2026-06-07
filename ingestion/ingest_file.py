import os
import time
import json
import requests
from datetime import datetime, UTC
from boto3 import session
from botocore.exceptions import ClientError

# 1. LIEN STABLE DE SECOURS (Contient les données éducatives officielles du Maroc)
DATA_URL = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
LOCAL_FILE_PATH = "ingestion/maroc_universities.csv" 
TARGET_FILE_NAME = f"maroc_education_{int(time.time())}.csv"

# 2. CONFIGURATION MINIO (Identique à ton docker-compose)
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"
BUCKET_NAME = "raw-documents"

# Connexion au client MinIO (Docker)
s3_client = session.Session().client(
    service_name="s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=MINIO_ENDPOINT
)

def run_file_ingestion():
    # Étape 1 : Téléchargement du fichier avec un User-Agent pour éviter d'être bloqué
    print("Téléchargement du jeu de données Éducation...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    try:
        response = requests.get(DATA_URL, headers=headers, timeout=15)
        if response.status_code == 200:
            os.makedirs(os.path.dirname(LOCAL_FILE_PATH), exist_ok=True)
            with open(LOCAL_FILE_PATH, "wb") as f:
                f.write(response.content)
            print(f"✅ Fichier enregistré localement dans : {LOCAL_FILE_PATH}")
        else:
            print(f"❌ Erreur de récupération. Statut : {response.status_code}")
            return
    except Exception as e:
        print(f"❌ Erreur de connexion au serveur : {e}")
        return

    # Étape 2 : Vérification et création du bucket raw-documents s'il n'existe pas
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except ClientError:
        print(f"Le bucket '{BUCKET_NAME}' n'existe pas. Création automatique...")
        s3_client.create_bucket(Bucket=BUCKET_NAME)

    # Étape 3 : Téléversement du fichier et des métadonnées vers MinIO
    try:
        print(f"Téléversement de {LOCAL_FILE_PATH} vers Docker MinIO...")
        s3_client.upload_file(
            Filename=LOCAL_FILE_PATH,
            Bucket=BUCKET_NAME,
            Key=TARGET_FILE_NAME
        )

        # Structure de métadonnées exigée par ton projet
        metadata = {
            "extracted_at": datetime.now(UTC).isoformat(),
            "source": "Portail National des Données Ouvertes (data.gov.ma) - Thème: Education",
            "original_file_name": os.path.basename(LOCAL_FILE_PATH),
            "file_size_bytes": os.path.getsize(LOCAL_FILE_PATH)
        }
        
        # Envoi du fichier JSON de métadonnées associé
        metadata_file_key = TARGET_FILE_NAME.replace(".csv", "_metadata.json")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=metadata_file_key,
            Body=json.dumps(metadata, indent=4),
            ContentType="application/json"
        )

        print("🎉 Ingestion du fichier et de ses métadonnées réussie avec succès dans MinIO !")

    except Exception as e:
        print(f"❌ Échec de l'envoi vers MinIO : {e}")

if __name__ == "__main__":
    run_file_ingestion()