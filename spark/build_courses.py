# spark/build_courses.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Créer la session Spark
# ─────────────────────────────────────────
def get_spark_session():
    return SparkSession.builder \
        .appName("UniversityCourseCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# ─────────────────────────────────────────
# Lire les données HTML depuis MinIO
# ─────────────────────────────────────────
def read_html_from_minio(spark, university, faculty):
    path = (
        f"s3a://raw-web-html/"
        f"university={university}/"
        f"faculty={faculty}/**/*.html"
    )
    logger.info(f"📖 Lecture MinIO HTML : {path}")
    try:
        df = spark.read.text(path)
        logger.info(f"✅ {df.count()} fichiers HTML lus")
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lecture MinIO : {e}")
        return None

# ─────────────────────────────────────────
# Construire la table course_catalog
# ─────────────────────────────────────────
def build_course_catalog(university, faculty):

    logger.info(f"🚀 Build course_catalog : {faculty} — {university}")

    spark = get_spark_session()

    df = read_html_from_minio(spark, university, faculty)

    if df is None or df.count() == 0:
        logger.warning(f"⚠️ Pas de données pour {faculty}")
        return

    # Normaliser
    df_clean = df.select(
        col("value").alias("raw_content")
    ) \
    .withColumn("university", lit(university)) \
    .withColumn("faculty", lit(faculty)) \
    .withColumn("source", lit("web_scraping")) \
    .withColumn("crawl_timestamp", current_timestamp()) \
    .withColumn("record_id",
        col("university").cast("string")
    ) \
    .dropDuplicates(["record_id"])

    logger.info(f"✅ {df_clean.count()} cours après nettoyage")

    # Écrire dans Hudi
    hudi_path = f"s3a://curated/course_catalog/"

    df_clean.write \
        .format("hudi") \
        .option("hoodie.table.name", "course_catalog") \
        .option("hoodie.datasource.write.recordkey.field", "record_id") \
        .option("hoodie.datasource.write.partitionpath.field", "university") \
        .option("hoodie.datasource.write.operation", "upsert") \
        .option("hoodie.datasource.write.precombine.field", "crawl_timestamp") \
        .mode("append") \
        .save(hudi_path)

    logger.info(f"✅ course_catalog écrit dans Hudi : {hudi_path}")

    spark.stop()

# ─────────────────────────────────────────
# TEST DIRECT
# ─────────────────────────────────────────
if __name__ == "__main__":
    build_course_catalog(
        university = "hassan2",
        faculty    = "FSAC"
    )