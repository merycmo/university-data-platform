import os
import tempfile
import json
import requests

# --- CONTOURNEMENT WINDOWS HADOOP / WINUTILS ---
hadoop_dir = os.path.join(tempfile.gettempdir(), "hadoop")
bin_dir = os.path.join(hadoop_dir, "bin")
os.makedirs(bin_dir, exist_ok=True)
os.environ["HADOOP_HOME"] = hadoop_dir

winutils_path = os.path.join(bin_dir, "winutils.exe")
if not os.path.exists(winutils_path):
    with open(winutils_path, "w") as f:
        pass
# -----------------------------------------------

from pyspark.sql import SparkSession

ES_HOST = "http://localhost:9200"

# Chemin robuste vers mapping.json, peu importe le dossier depuis lequel on lance le script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_PATH = os.path.join(SCRIPT_DIR, "mapping.json")

# Tables Hudi à indexer -> index Elasticsearch cible
TABLES_TO_INDEX = {
    "course_catalog": {
        "hudi_path": "s3a://curated/course_catalog",
        "es_index": "course_catalog",
    },
    "faculty_profiles": {
        "hudi_path": "s3a://curated/faculty_profiles",
        "es_index": "faculty_profiles",
    },
}


def create_es_indices():
    """Supprime puis recrée les index Elasticsearch avec le mapping défini dans mapping.json."""
    print(f"\n{'='*70}")
    print("🏗️  Création des index Elasticsearch")
    print(f"{'='*70}")

    with open(MAPPING_PATH, encoding="utf-8") as f:
        mappings = json.load(f)

    for index_name, body in mappings.items():
        del_resp = requests.delete(f"{ES_HOST}/{index_name}")
        print(f"DELETE {index_name} -> {del_resp.status_code}")

        put_resp = requests.put(f"{ES_HOST}/{index_name}", json=body)
        print(f"PUT {index_name} -> {put_resp.status_code} {put_resp.text[:200]}")

        if put_resp.status_code != 200:
            raise RuntimeError(f"Échec de création de l'index '{index_name}': {put_resp.text}")

    print("✅ Index créés avec le mapping défini dans mapping.json.")


def get_spark_session():
    packages = (
        "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    return (
        SparkSession.builder
        .appName("IndexToElasticsearch")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.jars.packages", packages)
        .config("spark.es.nodes", "localhost")
        .config("spark.es.port", "9200")
        .config("spark.es.nodes.wan.only", "true")
        # Les index sont déjà créés avec le bon mapping via create_es_indices(),
        # on désactive l'auto-création pour ne pas laisser Spark deviner des types différents.
        .config("spark.es.index.auto.create", "false")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .enableHiveSupport()
        .getOrCreate()
    )


def index_table(spark, table_name, hudi_path, es_index):
    """Lit une table Hudi depuis MinIO et l'indexe dans Elasticsearch."""
    print(f"\n{'='*70}")
    print(f"📖 Lecture Hudi depuis : {hudi_path}")
    print(f"{'='*70}")

    try:
        df = spark.read.format("hudi").load(hudi_path)
        row_count = df.count()
        print(f"Nombre de lignes lues ({table_name}) : {row_count}")

        if row_count == 0:
            print(f"⚠️ Table '{table_name}' vide, indexation ignorée.")
            return

        df.show(5, truncate=False)

        # Exclut les soft-deletes (upsert Hudi) de l'index de recherche
        if "is_deleted" in df.columns:
            df = df.filter(df.is_deleted == False)  # noqa: E712

        # Ne garde que les colonnes définies dans le mapping ES
        cols_to_keep = [c for c in df.columns if not c.startswith("_hoodie_")]
        df = df.select(*cols_to_keep)

        print(f"🔎 Indexation vers Elasticsearch (index='{es_index}')...")
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", f"{es_index}/_doc") \
            .option("es.mapping.id", "record_id") \
            .mode("append") \
            .save()

        print(f"✅ Table '{table_name}' indexée avec succès dans '{es_index}' !")

    except Exception as e:
        print(f"❌ Erreur lors de l'indexation de '{table_name}' : {e}")
        raise


def main():
    # Étape 1 : (re)création des index avec le bon mapping
    create_es_indices()

    # Étape 2 : lecture Hudi + indexation vers Elasticsearch
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark connectée avec succès.")

    try:
        for table_name, cfg in TABLES_TO_INDEX.items():
            index_table(spark, table_name, cfg["hudi_path"], cfg["es_index"])

        print(f"\n{'='*70}")
        print("🎉 Indexation terminée pour toutes les tables.")
        print(f"{'='*70}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()