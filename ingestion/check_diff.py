from minio import Minio

client = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)

pdfs = set([o.object_name for o in client.list_objects("raw-documents", prefix="university=hassan2/faculty=FST/", recursive=True) if o.object_name.endswith(".pdf")])

jsons = set([o.object_name.replace(".json", ".pdf") for o in client.list_objects("raw-json", prefix="university=hassan2/faculty=FST/", recursive=True) if o.object_name.endswith(".json")])

manquants = pdfs - jsons
print(f"PDFs non extraits : {len(manquants)}")
for f in manquants:
    print(f" - {f}")