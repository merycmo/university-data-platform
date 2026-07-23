# spark/build_faculty.py

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, md5, concat_ws

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT  = "http://minio:9000"
MINIO_USER      = "admin"
MINIO_PASSWORD  = "password123"

HUDI_TABLE_NAME = "faculty_profiles"
HUDI_TABLE_PATH = "s3a://curated/faculty_profiles"
HIVE_DATABASE   = "university"

UNIVERSITY_ALIASES = {
    "hassan2": ["hassan2", "hassan_ii", "Hassan II", "hassan_2"],
    "hassan_ii": ["hassan_ii", "hassan2", "Hassan II", "hassan_2"],
    "cadi_ayyad": ["cadi_ayyad", "Cadi Ayyad", "cadiayyad", "caddi_ayad", "kaddi_ayad"],
}


def get_spark_session():
    return (
        SparkSession.builder
        .appName("BuildFacultyProfiles")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .enableHiveSupport()
        .getOrCreate()
    )


def find_existing_university_path(spark, university_hint, faculty, bucket="raw-json", subpath="type=authors"):
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI(f"s3a://{bucket}"), hadoop_conf
    )

    candidates = UNIVERSITY_ALIASES.get(university_hint, [university_hint])
    candidates = list(dict.fromkeys(
        candidates + [
            university_hint,
            university_hint.lower(),
            university_hint.lower().replace(" ", "_"),
        ]
    ))

    for candidate in candidates:
        path_str = f"university={candidate}/faculty={faculty}/{subpath}/"
        hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{path_str}")
        try:
            if fs.exists(hadoop_path):
                logger.info(f"✅ Chemin trouvé : university={candidate}")
                return candidate, f"s3a://{bucket}/{path_str}"
        except Exception:
            continue

    logger.warning(f"⚠️ Aucune variante trouvée pour '{university_hint}'. Essayé : {candidates}")
    return university_hint, f"s3a://{bucket}/university={university_hint}/faculty={faculty}/{subpath}/"


def read_authors_raw(spark, university, faculty):
    resolved_university, path = find_existing_university_path(
        spark, university, faculty, bucket="raw-json", subpath="type=authors"
    )
    logger.info(f"📖 Lecture MinIO : {path}")
    return spark.read.option("multiLine", "true").json(path), resolved_university


def normalize_faculty_profiles(df, university, faculty):
    normalized = df.select(
        col("id").alias("author_id"),
        col("display_name").alias("full_name"),
        col("orcid"),
        col("works_count"),
        col("cited_by_count"),
        col("last_known_institutions").getItem(0).getField("display_name").alias("institution_name")
    )

    normalized = normalized.withColumn("university", lit(university)) \
                         .withColumn("faculty", lit(faculty)) \
                         .withColumn("source_system", lit("openalex_api")) \
                         .withColumn("record_id", md5(concat_ws("_", col("author_id"), lit(faculty)))) \
                         .withColumn("business_timestamp", current_timestamp()) \
                         .withColumn("is_deleted", lit(False)) \
                         .withColumn("language", lit("en"))

    normalized = normalized.dropDuplicates(["author_id"])
    normalized = normalized.filter(col("full_name").isNotNull())

    return normalized


def write_to_hudi(df, table_name, table_path):
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": "record_id",
        "hoodie.datasource.write.precombine.field": "business_timestamp",
        "hoodie.datasource.write.partitionpath.field": "university,faculty",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.hive_sync.enable": "false",
    }

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(table_path)


def register_table_in_hive(spark, table_name, table_path):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.{table_name}")
    spark.sql(f"""
        CREATE TABLE {HIVE_DATABASE}.{table_name}
        USING hudi
        OPTIONS (
            primaryKey 'record_id',
            preCombineField 'business_timestamp'
        )
        LOCATION '{table_path}'
    """)
    logger.info(f"✅ Table enregistrée dans Hive : {HIVE_DATABASE}.{table_name}")


def run_build_faculty(university="hassan2", faculty="FSAC"):
    logger.info(f"🚀 Build faculty_profiles : {faculty} — {university}")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw, resolved_university = read_authors_raw(spark, university, faculty)
        count_raw = df_raw.count()
        logger.info(f"📥 {count_raw} auteurs bruts lus depuis raw-json")

        if count_raw == 0:
            logger.warning("⚠️ Aucune donnée trouvée, arrêt.")
            return

        df_clean = normalize_faculty_profiles(df_raw, resolved_university, faculty)
        count_clean = df_clean.count()
        logger.info(f"✨ {count_clean} profils normalisés")

        write_to_hudi(df_clean, HUDI_TABLE_NAME, HUDI_TABLE_PATH)
        logger.info(f"✅ Table Hudi '{HUDI_TABLE_NAME}' mise à jour dans MinIO")

        register_table_in_hive(spark, HUDI_TABLE_NAME, HUDI_TABLE_PATH)

        df_clean.select("full_name", "works_count", "cited_by_count", "faculty").show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    university = sys.argv[1] if len(sys.argv) > 1 else "hassan2"
    faculty    = sys.argv[2] if len(sys.argv) > 2 else "FSAC"
    run_build_faculty(university=university, faculty=faculty)