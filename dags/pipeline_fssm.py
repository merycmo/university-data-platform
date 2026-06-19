# dags/pipeline_fssm.py
# Membre 4 - Hafsa Masrour
# Faculté des Sciences Semlalia - Marrakech (FSSM)

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.university_scraper import scrape_university
from ingestion.ingest_api import ingest_faculty
from ingestion.ingest_file import run_file_ingestion
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("=" * 50)
    logger.info("PIPELINE FSSM - cadi_ayyad")
    logger.info("=" * 50)

    # Étape 1 - Scraping web
    logger.info("Étape 1 : Scraping web...")
    stats_web = scrape_university(
        start_url  = "https://www.uca.ma/fssm/fr",
        university = "cadi_ayyad",
        faculty    = "FSSM",
        max_depth  = 3
    )
    logger.info(f"Scraping terminé : {stats_web}")

    # Étape 2 - Ingestion API OpenAlex
    logger.info("Étape 2 : Ingestion API OpenAlex...")
    ingest_faculty("Cadi Ayyad", "FSSM")

    # Étape 3 - Ingestion fichiers PDF
    logger.info("Étape 3 : Ingestion fichiers PDF...")
    stats_file = run_file_ingestion(
        university = "cadi_ayyad",
        faculty    = "FSSM"
    )
    logger.info(f"Ingestion fichiers terminée : {stats_file}")

    logger.info("Pipeline FSSM terminé !")

if __name__ == "__main__":
    run_pipeline()