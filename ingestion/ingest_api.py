import json
import time
import requests
from datetime import datetime, UTC
from boto3 import session
from botocore.exceptions import ClientError

# 1. PARAMÈTRES OPENALEX (MÉTHODE PUBLIC POOL GRATUITE)
ENDPOINT_URL = "https://api.openalex.org/authors?per_page=20"
USER_EMAIL = "hafsa@etud.univh2c.ma" 

# 2. CONFIGURATION MINIO COHÉRENTE AVEC VOTRE DOCKER-COMPOSE
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "admin"        # Mis à jour depuis votre docker-compose
SECRET_KEY = "password123"  # Mis à jour depuis votre docker-compose
BUCKET_NAME = "raw-json"

# Connexion au client MinIO (Passerelle avec Docker)
s3_client = session.Session().client(
    service_name="s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=MINIO_ENDPOINT
)

def run_api_ingestion():
    headers = {
        "User-Agent": f"mailto:{USER_EMAIL}"
    }
    
    print("Extraction des données depuis OpenAlex...")
    response = requests.get(ENDPOINT_URL, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        
        # Structure Semaine 1 : Données Brutes + Métadonnées
        payload = {
            "metadata": {
                "extracted_at": datetime.now(UTC).isoformat(), # Format moderne sans warning
                "source": "OpenAlex API (Authors)",
                "record_count": len(data.get("results", []))
            },
            "data": data.get("results", [])
        }
        
        # Vérification et création automatique du bucket s'il n'existe pas
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
        except ClientError:
            print(f"Le bucket '{BUCKET_NAME}' n'existe pas. Création en cours...")
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        
        # Génération du nom de fichier unique
        timestamp = int(time.time())
        file_name = f"openalex_authors_{timestamp}.json"
        
        # Envoi du fichier JSON vers MinIO dans Docker
        print(f"Envoi de {file_name} dans le bucket MinIO '{BUCKET_NAME}'...")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(payload, indent=4),
            ContentType="application/json"
        )
        print("🎉 Ingestion réussie ! Les données sont stockées en toute sécurité dans Docker MinIO.")
    else:
        print(f"❌ Erreur API OpenAlex : Statut {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    run_api_ingestion()