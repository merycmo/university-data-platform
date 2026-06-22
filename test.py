from ingestion.ingest_api import run_scheduled_ingestion

UNIVERSITY = "Cadi Ayyad"
FACULTY    = "FSSM"

# Lancer l'ingestion
run_scheduled_ingestion(UNIVERSITY, FACULTY)