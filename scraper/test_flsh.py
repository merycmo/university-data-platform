# scraper/test_fsbm.py
import sys
sys.path.append('.')

from scraper.university_scraper import scrape_university


scrape_university(
    start_url  = "https://flsh-uh2c.ac.ma/",
    university = "hassan2",
    faculty    = "FLSHAC",
    max_depth  = 3
)