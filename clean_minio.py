from minio import Minio

client = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)

# Dates anciennes à supprimer
old_dates = ["day=13", "day=15"]

for bucket in ["raw-documents", "raw-web-html", "raw-images", "raw-json"]:
    try:
        objects = client.list_objects(
            bucket,
            prefix="university=cadi_ayyad/faculty=FSSM/",
            recursive=True
        )
        deleted = 0
        for obj in objects:
            for date in old_dates:
                if date in obj.object_name:
                    client.remove_object(bucket, obj.object_name)
                    deleted += 1
                    break
        print(f"{bucket} : {deleted} fichiers supprimés")
    except Exception as e:
        print(f"Erreur {bucket} : {e}")