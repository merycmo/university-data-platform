# spark/create_tables.py

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT  = "http://minio:9000"
MINIO_USER      = "admin"
MINIO_PASSWORD  = "password123"

def get_spark_session():
    return (
        SparkSession.builder
        .appName("HiveCatalogSync")
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.hive.metastore.uris",
                "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir",
                "/tmp/spark-warehouse")
        .config("spark.hadoop.hive.metastore.warehouse.dir",
                "/tmp/hive-warehouse")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .enableHiveSupport()
        .getOrCreate()
    )

def register_tables(spark):
    logger.info("🚀 Création base de données university")
    spark.sql("CREATE DATABASE IF NOT EXISTS university")

    logger.info("🚀 Lecture faculty_profiles depuis MinIO")
    df_faculty = spark.read.format("hudi").load("s3a://curated/faculty_profiles/")
    df_faculty.createOrReplaceTempView("faculty_profiles_view")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS university.faculty_profiles
        USING hudi
        TBLPROPERTIES (
            'hoodie.table.name' = 'faculty_profiles'
        )
        AS SELECT * FROM faculty_profiles_view WHERE 1=0
    """)
    count = spark.sql("SELECT COUNT(*) FROM university.faculty_profiles").collect()[0][0]
    logger.info(f"✅ faculty_profiles enregistrée — {count} records")

    logger.info("🚀 Lecture course_catalog depuis MinIO")
    df_courses = spark.read.format("hudi").load("s3a://curated/course_catalog/")
    df_courses.createOrReplaceTempView("course_catalog_view")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS university.course_catalog
        USING hudi
        TBLPROPERTIES (
            'hoodie.table.name' = 'course_catalog'
        )
        AS SELECT * FROM course_catalog_view WHERE 1=0
    """)
    count = spark.sql("SELECT COUNT(*) FROM university.course_catalog").collect()[0][0]
    logger.info(f"✅ course_catalog enregistrée — {count} records")

def run_hive_sync():
    logger.info("🚀 Synchronisation tables Hudi → Hive")
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        register_tables(spark)
        logger.info("✅ Synchronisation Hive terminée")
        spark.sql("SELECT * FROM university.faculty_profiles LIMIT 5").show(truncate=False)
        spark.sql("SELECT * FROM university.course_catalog LIMIT 5").show(truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_hive_sync()