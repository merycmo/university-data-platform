# ingestion/ingest_web.py
# Ce fichier appelle le scraper générique

from scraper.university_scraper import scrape_university

def ingest_web(start_url, university, faculty, max_depth=3):
    return scrape_university(
        start_url  = start_url,
        university = university,
        faculty    = faculty,
        max_depth  = max_depth
    )

if __name__ == "__main__":
    ingest_web(
        start_url  = "https://fsac.univh2c.ma/",
        university = "hassan2",
        faculty    = "FSAC"
    )