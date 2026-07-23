from  ingest_api import run_scheduled_ingestion


UNIVERSITY = "Hassan II"
FACULTY    = "FLSH"

# Lancer l'ingestion
run_scheduled_ingestion(UNIVERSITY, FACULTY)