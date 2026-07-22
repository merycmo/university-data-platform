import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.ingest_web import scrape_university
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("=" * 50)
    logger.info("PIPELINE FSTM - Hassan II")
    logger.info("=" * 50)

    stats = scrape_university(
        start_url  = "https://www.fstm.ac.ma",
        university = "Hassan II",
        faculty    = "FST",
        max_depth  = 3
    )

    logger.info(f"Pipeline FSTM terminé : {stats}")
    return stats

if __name__ == "__main__":
    run_pipeline()