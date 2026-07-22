import sys
import os

# S'assure que le dossier courant est bien dans le path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ingest_file import run_file_ingestion

if __name__ == "__main__":
    print("🧪 Lancement du test d'ingestion des fichiers (PDF/DOCX)...")
    
    # Vous pouvez adapter l'université et la faculté selon ce que vous avez stocké dans MinIO
    stats = run_file_ingestion(university="Hassan II", faculty="FST")
    
    print(f"✅ Test terminé ! Statistiques : {stats}")