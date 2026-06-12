# ingestion/ingest_web.py

import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper.university_scraper import scrape_university

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Configuration FSAC
# ─────────────────────────────────────────
FSAC_CONFIG = {
    "start_url"  : "https://fsac.univh2c.ma/",
    "university" : "hassan2",
    "faculty"    : "FSAC",
    "max_depth"  : 2
}

# ─────────────────────────────────────────
# Fonction principale
# ─────────────────────────────────────────
def ingest_web_fsac():
    logger.info("🚀 Démarrage ingestion Web — FSAC")

    stats = scrape_university(
        start_url  = FSAC_CONFIG["start_url"],
        university = FSAC_CONFIG["university"],
        faculty    = FSAC_CONFIG["faculty"],
        max_depth  = FSAC_CONFIG["max_depth"]
    )

    logger.info(f"""
    ✅ Ingestion Web FSAC terminée
    ──────────────────────────────
    HTML     : {stats['html']}
    PDFs     : {stats['pdf']}
    Images   : {stats['image']}
    Ignorés  : {stats['skip']}
    Erreurs  : {stats['errors']}
    """)

    return stats

# ─────────────────────────────────────────
# TEST DIRECT
# ─────────────────────────────────────────
if __name__ == "__main__":
    ingest_web_fsac()