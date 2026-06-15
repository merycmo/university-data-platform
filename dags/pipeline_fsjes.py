import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scraper.university_scraper import scrape_university

scrape_university(
    start_url  = "https://fsjes.uca.ma/",
    university = "cadi_ayyad",
    faculty    = "fsjes",
    max_depth  = 3
)