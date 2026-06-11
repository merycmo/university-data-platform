# spark/build_faculty.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Créer la session Spark
# ─────────────────────────────────────────
def get_spark_session():
    return SparkSession.builder \
        .appName("UniversityFacultyProfiles") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# ─────────────────────────────────────────
# Lire les données depuis MinIO
# ─────────────────────────────────────────
def read_from_minio(spark, university, faculty):
    path = (
        f"s3a://raw-json/"
        f"university={university}/"
        f"faculty={faculty}/**/*.json"
    )
    logger.info(f"📖 Lecture MinIO : {path}")
    try:
        df = spark.read.json(path)
        logger.info(f"✅ {df.count()} enregistrements lus")
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lecture MinIO : {e}")
        return None

# ─────────────────────────────────────────
# Construire la table faculty_profiles
# ─────────────────────────────────────────
def build_faculty_profiles(university, faculty):

    logger.info(f"🚀 Build faculty_profiles : {faculty} — {university}")

    spark = get_spark_session()

    # Lire les données OpenAlex
    df = read_from_minio(spark, university, faculty)

    if df is None or df.count() == 0:
        logger.warning(f"⚠️ Pas de données pour {faculty}")
        return

    # Sélectionner et normaliser les colonnes
    df_clean = df.select(
        col("openalex_id").alias("record_id"),
        col("name"),
        col("orcid"),
        col("publications_count"),
        col("citations_count"),
        col("research_topics")
    ) \
    .withColumn("university", lit(university)) \
    .withColumn("faculty", lit(faculty)) \
    .withColumn("source", lit("openalex")) \
    .withColumn("crawl_timestamp", current_timestamp()) \
    .dropDuplicates(["record_id"])

    logger.info(f"✅ {df_clean.count()} profils après nettoyage")

    # Écrire dans Hudi
    hudi_path = f"s3a://curated/faculty_profiles/"

    df_clean.write \
        .format("hudi") \
        .option("hoodie.table.name", "faculty_profiles") \
        .option("hoodie.datasource.write.recordkey.field", "record_id") \
        .option("hoodie.datasource.write.partitionpath.field", "university") \
        .option("hoodie.datasource.write.operation", "upsert") \
        .option("hoodie.datasource.write.precombine.field", "crawl_timestamp") \
        .mode("append") \
        .save(hudi_path)

    logger.info(f"✅ faculty_profiles écrit dans Hudi : {hudi_path}")

    spark.stop()

# ─────────────────────────────────────────
# TEST DIRECT
# ─────────────────────────────────────────
if __name__ == "__main__":
    build_faculty_profiles(
        university = "hassan2",
        faculty    = "FSAC"
    )