# dags/pipeline_fssm.py
# Membre 4 - Hafsa Masrour
# Faculté des Sciences Semlalia - Marrakech (FSSM)

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.university_scraper import scrape_university
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("=" * 50)
    logger.info("PIPELINE FSSM - cadi_ayyad")
    logger.info("=" * 50)

    stats = scrape_university(
        start_url  = "https://www.uca.ma/fssm/fr",
        university = "cadi_ayyad",
        faculty    = "FSSM",
        max_depth  = 3
    )

    logger.info(f"Pipeline FSSM terminé : {stats}")
    return stats

if __name__ == "__main__":
    run_pipeline()