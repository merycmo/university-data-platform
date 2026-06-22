# spark/build_courses.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, md5, concat_ws, regexp_extract, lower, when, length, trim

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT  = "http://minio:9000"
MINIO_USER      = "admin"
MINIO_PASSWORD  = "password123"

HUDI_TABLE_NAME = "course_catalog"
HUDI_TABLE_PATH = "s3a://curated/course_catalog"

UNIVERSITY_ALIASES = {
    "hassan2": ["hassan2", "hassan_ii", "Hassan II", "hassan_2"],
    "cadi_ayyad": ["cadi_ayyad", "Cadi Ayyad", "cadiayyad", "caddi_ayad", "kaddi_ayad"],
}

COURSE_KEYWORDS = ["formation", "filiere", "filière", "programme", "cursus", "licence", "master", "doctorat", "module", "cours", "syllabus", "guide", "convention", "partenariat"]


def get_spark_session():
    return (
        SparkSession.builder
        .appName("BuildCourseCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )


def find_existing_university_path(spark, university_hint, faculty, bucket="raw-json", subpath=""):
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
        path_str = f"university={candidate}/faculty={faculty}/{subpath}"
        hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{path_str}")
        try:
            if fs.exists(hadoop_path):
                logger.info(f"✅ Chemin trouvé : university={candidate}")
                return candidate, f"s3a://{bucket}/{path_str}"
        except Exception:
            continue

    logger.warning(f"⚠️ Aucune variante trouvée pour '{university_hint}'. Essayé : {candidates}")
    return university_hint, f"s3a://{bucket}/university={university_hint}/faculty={faculty}/{subpath}"


def read_extracted_text(spark, university, faculty):
    resolved_university, path = find_existing_university_path(
        spark, university, faculty, bucket="raw-json", subpath=""
    )
    logger.info(f"📖 Lecture MinIO : {path}")
    df = spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(path)
    return df, resolved_university


def normalize_course_catalog(df, university, faculty):
    # Détecte si c'est le format ingest_file (source_path + extracted_text)
    # ou le format ancien (metadata.source_url + text)
    columns = df.columns

    if "source_path" in columns:
        with_path = df.withColumn("source_path_col", col("source_path")) \
                      .withColumn("text_content", col("extracted_text"))
    else:
        with_path = df.withColumn("source_path_col", col("metadata.source_url")) \
                      .withColumn("text_content", col("text"))

    keyword_filter = None
    for kw in COURSE_KEYWORDS:
        cond = lower(col("text_content")).contains(kw)
        keyword_filter = cond if keyword_filter is None else (keyword_filter | cond)

    formations_only = with_path.filter(keyword_filter)

    normalized = formations_only.withColumn(
        "raw_filename",
        regexp_extract(col("source_path_col"), r"([^/]+)\.pdf$", 1)
    )

    normalized = normalized.withColumn(
        "course_name",
        regexp_extract(col("raw_filename"), r"(?:Fili[eè]re[s]?_?)(.*)", 1)
    )

    normalized = normalized.withColumn(
        "course_name",
        when(length(trim(col("course_name"))) > 0, col("course_name"))
        .otherwise(col("raw_filename"))
    )

    normalized = normalized.select(
        "course_name",
        "raw_filename",
        col("source_path_col").alias("source_path"),
        "text_content"
    )

    normalized = normalized.withColumn("university", lit(university)) \
                            .withColumn("faculty", lit(faculty)) \
                            .withColumn("source_system", lit("file_extraction")) \
                            .withColumn("record_id", md5(concat_ws("_", col("raw_filename"), lit(faculty)))) \
                            .withColumn("business_timestamp", current_timestamp()) \
                            .withColumn("is_deleted", lit(False)) \
                            .withColumn("language", lit("fr"))

    normalized = normalized.dropDuplicates(["record_id"])
    normalized = normalized.filter(col("course_name").isNotNull())

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
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        "hoodie.datasource.hive_sync.enable": "false",
    }

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(table_path)


def run_build_courses(university="hassan2", faculty="FSAC"):
    logger.info(f"🚀 Build course_catalog : {faculty} — {university}")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw, resolved_university = read_extracted_text(spark, university, faculty)
        count_raw = df_raw.count()
        logger.info(f"📥 {count_raw} documents bruts lus depuis raw-json")

        if count_raw == 0:
            logger.warning("⚠️ Aucune donnée trouvée, arrêt.")
            return

        df_clean = normalize_course_catalog(df_raw, resolved_university, faculty)
        count_clean = df_clean.count()
        logger.info(f"✨ {count_clean} cours/filières trouvés après filtrage")

        if count_clean == 0:
            logger.warning("⚠️ Aucun cours trouvé après filtrage. Vérifie les mots-clés COURSE_KEYWORDS.")
            return

        write_to_hudi(df_clean, HUDI_TABLE_NAME, HUDI_TABLE_PATH)
        logger.info(f"✅ Table Hudi '{HUDI_TABLE_NAME}' mise à jour avec succès")

        df_clean.select("course_name", "raw_filename", "faculty").show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_build_courses(university="cadi_ayyad", faculty="FSSM")