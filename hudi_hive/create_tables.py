# hudi_hive/create_tables.py

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

def register_faculty_profiles(spark):
    logger.info("🚀 Enregistrement table faculty_profiles dans Hive")
    spark.sql("CREATE DATABASE IF NOT EXISTS university")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS university.faculty_profiles
        USING hudi
        LOCATION 's3a://curated/faculty_profiles/'
        TBLPROPERTIES (
            'hoodie.table.name' = 'faculty_profiles',
            'hoodie.datasource.write.recordkey.field' = 'record_id',
            'hoodie.datasource.write.precombine.field' = 'crawl_timestamp'
        )
    """)
    count = spark.sql("SELECT COUNT(*) FROM university.faculty_profiles").collect()[0][0]
    logger.info(f"✅ faculty_profiles enregistrée — {count} records")

def register_course_catalog(spark):
    logger.info("🚀 Enregistrement table course_catalog dans Hive")
    spark.sql("CREATE DATABASE IF NOT EXISTS university")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS university.course_catalog
        USING hudi
        LOCATION 's3a://curated/course_catalog/'
        TBLPROPERTIES (
            'hoodie.table.name' = 'course_catalog',
            'hoodie.datasource.write.recordkey.field' = 'record_id',
            'hoodie.datasource.write.precombine.field' = 'crawl_timestamp'
        )
    """)
    count = spark.sql("SELECT COUNT(*) FROM university.course_catalog").collect()[0][0]
    logger.info(f"✅ course_catalog enregistrée — {count} records")

def run_hive_sync():
    logger.info("🚀 Synchronisation tables Hudi → Hive")
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        register_faculty_profiles(spark)
        register_course_catalog(spark)
        logger.info("✅ Synchronisation Hive terminée")
        logger.info("📊 Aperçu faculty_profiles :")
        spark.sql("SELECT * FROM university.faculty_profiles LIMIT 5").show(truncate=False)
        logger.info("📊 Aperçu course_catalog :")
        spark.sql("SELECT * FROM university.course_catalog LIMIT 5").show(truncate=False)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_hive_sync()