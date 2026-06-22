from minio import Minio
client = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)
pdfs = [o.object_name for o in client.list_objects("raw-documents", prefix="university=cadi_ayyad/faculty=FSSM/", recursive=True) if o.object_name.endswith(".pdf")]
jsons = [o.object_name for o in client.list_objects("raw-json", prefix="university=cadi_ayyad/faculty=FSSM/", recursive=True) if o.object_name.endswith(".json")]
print(f"PDFs dans raw-documents : {len(pdfs)}")
print(f"JSONs dans raw-json     : {len(jsons)}")
print(f"Différence              : {len(pdfs) - len(jsons)}")