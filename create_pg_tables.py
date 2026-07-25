import psycopg2
import json
from minio import Minio

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="metastore",
    user="hive",
    password="hive123"
)
cur = conn.cursor()

# Table faculty_profiles
cur.execute("""
    DROP TABLE IF EXISTS faculty_profiles;
    CREATE TABLE faculty_profiles (
        id SERIAL PRIMARY KEY,
        full_name VARCHAR(255),
        works_count INTEGER,
        cited_by_count INTEGER,
        university VARCHAR(100),
        faculty VARCHAR(50),
        ingested_at DATE DEFAULT CURRENT_DATE
    )
""")

# Table course_catalog
cur.execute("""
    DROP TABLE IF EXISTS course_catalog;
    CREATE TABLE course_catalog (
        id SERIAL PRIMARY KEY,
        course_name VARCHAR(500),
        raw_filename VARCHAR(500),
        university VARCHAR(100),
        faculty VARCHAR(50),
        ingested_at DATE DEFAULT CURRENT_DATE
    )
""")

minio = Minio("localhost:9000", access_key="admin", secret_key="password123", secure=False)

# Remplir faculty_profiles
objects = list(minio.list_objects("raw-json", prefix="university=cadi_ayyad/faculty=FSSM/type=authors/", recursive=True))
for obj in objects:
    if obj.object_name.endswith(".json") and not obj.object_name.endswith(".meta.json"):
        response = minio.get_object("raw-json", obj.object_name)
        data = json.loads(response.read().decode("utf-8"))
        if isinstance(data, list):
            for author in data:
                cur.execute("""
                    INSERT INTO faculty_profiles (full_name, works_count, cited_by_count, university, faculty)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    author.get("display_name", "Inconnu"),
                    author.get("works_count", 0),
                    author.get("cited_by_count", 0),
                    "cadi_ayyad",
                    "FSSM"
                ))

print("✅ faculty_profiles remplie !")

# Remplir course_catalog depuis raw-documents
objects = list(minio.list_objects("raw-documents", prefix="university=cadi_ayyad/faculty=FSSM/", recursive=True))
for obj in objects:
    if obj.object_name.endswith(".pdf"):
        filename = obj.object_name.split("/")[-1].replace(".pdf", "")
        cur.execute("""
            INSERT INTO course_catalog (course_name, raw_filename, university, faculty)
            VALUES (%s, %s, %s, %s)
        """, (filename, filename, "cadi_ayyad", "FSSM"))

print("✅ course_catalog remplie !")

conn.commit()
cur.close()
conn.close()
print("✅ Tout est prêt pour Metabase !")