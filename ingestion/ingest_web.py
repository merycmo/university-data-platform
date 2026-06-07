import json
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, UTC
from boto3 import session
from botocore.exceptions import ClientError

# 1. PARAMÈTRES DE SCRAPING (Université Mohammed V - UM5)
TARGET_URL = "https://www.um5.ac.ma/um5r/"
BUCKET_NAME = "raw-web"  # Bucket dédié au Web Scraping
TARGET_FILE_NAME = f"um5_scraped_{int(time.time())}.json"

# 2. CONFIGURATION MINIO (Identique à ton docker-compose)
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"

# Connexion au client MinIO (Docker)
s3_client = session.Session().client(
    service_name="s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=MINIO_ENDPOINT
)

def run_web_scraping():
    print(f"Scraping en cours sur le site de l'UM5 : {TARGET_URL}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = requests.get(TARGET_URL, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"❌ Impossible d'accéder au site de l'UM5. Statut : {response.status_code}")
            return
            
        # Extraction du contenu HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        scraped_items = []
        
        # Extraction des blocs d'actualités/titres du site
        # On cible les liens et les titres principaux présents sur la page d'accueil
        for article in soup.find_all(['h2', 'h3', 'a'], limit=30):
            text = article.get_text(strip=True)
            href = article.get('href', '')
            
            # On ne garde que les textes pertinents (plus de 15 caractères) pour éviter le bruit
            if len(text) > 15 and (text not in [item['title'] for item in scraped_items]):
                # Reconstruire l'URL si elle est relative
                if href and not href.startswith('http'):
                    href = f"https://www.um5.ac.ma{href}"
                    
                scraped_items.append({
                    "title": text,
                    "url": href
                })

        # Construction du payload final (Données brutes + Métadonnées obligatoires)
        payload = {
            "metadata": {
                "extracted_at": datetime.now(UTC).isoformat(),
                "source": "Web Scraping - Université Mohammed V (UM5)",
                "url_scraped": TARGET_URL,
                "record_count": len(scraped_items)
            },
            "data": scraped_items
        }

        # Étape B : Vérification et création du bucket raw-web s'il n'existe pas
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
        except ClientError:
            print(f"Le bucket '{BUCKET_NAME}' n'existe pas. Création automatique...")
            s3_client.create_bucket(Bucket=BUCKET_NAME)

        # Étape C : Téléversement du résultat JSON vers Docker MinIO
        print(f"Envoi des données scrapées dans le bucket MinIO '{BUCKET_NAME}'...")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=TARGET_FILE_NAME,
            Body=json.dumps(payload, indent=4, ensure_ascii=False),
            ContentType="application/json"
        )
        print("🎉 Web Scraping et ingestion réussis avec succès dans MinIO !")

    except Exception as e:
        print(f"❌ Une erreur est survenue lors du scraping ou de l'envoi : {e}")

if __name__ == "__main__":
    run_web_scraping()