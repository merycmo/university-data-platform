from scraper.university_scraper import scrape_university

scrape_university(
    start_url  = "https://www.uca.ma/fssm/fr",
    university = "cadi_ayyad",
    faculty    = "FSSM",
    max_depth  = 3
)