from scraper.university_scraper import scrape_university

scrape_university(
    start_url  = "https://www.flsh-uh2c.ac.ma/",
    university = "hassan2",
    faculty    = "FLSH",
    max_depth  = 3
)