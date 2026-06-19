from university_scraper import scrape_university

print("🚀 Lancement du script de test d'ingestion unique...")

# Exécution du scraper avec la configuration fonctionnelle pour la FSTM
scrape_university(
    start_url  = "https://www.fstm.ac.ma/",
    university="hassan2",
    faculty="FST",
    max_depth=3
)