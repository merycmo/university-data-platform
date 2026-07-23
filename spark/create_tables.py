# hudi_hive/create_tables.py
from pyspark.sql import SparkSession
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session():
    return SparkSession.builder \
        .appName("UniversityHudiTables") \
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.hive.metastore.uris",
                "thrift://university_hive:9083") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .enableHiveSupport() \
        .getOrCreate()

def create_faculty_profiles_table(spark):
    logger.info("🚀 Création table faculty_profiles")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS faculty_profiles (
            record_id          STRING,
            name               STRING,
            orcid              STRING,
            publications_count BIGINT,
            citations_count    BIGINT,
            research_topics    ARRAY<STRING>,
            university         STRING,
            faculty            STRING,
            source             STRING,
            crawl_timestamp    TIMESTAMP
        )
        USING hudi
        PARTITIONED BY (university)
        LOCATION 's3a://curated/faculty_profiles/'
        TBLPROPERTIES (
            'hoodie.table.name' = 'faculty_profiles',
            'hoodie.datasource.write.recordkey.field' = 'record_id',
            'hoodie.datasource.write.precombine.field' = 'crawl_timestamp'
        )
    """)
    logger.info("✅ Table faculty_profiles créée")

def create_course_catalog_table(spark):
    logger.info("🚀 Création table course_catalog")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS course_catalog (
            record_id        STRING,
            title            STRING,
            level            STRING,
            department       STRING,
            language         STRING,
            raw_content      STRING,
            university       STRING,
            faculty          STRING,
            source           STRING,
            crawl_timestamp  TIMESTAMP
        )
        USING hudi
        PARTITIONED BY (university)
        LOCATION 's3a://curated/course_catalog/'
        TBLPROPERTIES (
            'hoodie.table.name' = 'course_catalog',
            'hoodie.datasource.write.recordkey.field' = 'record_id',
            'hoodie.datasource.write.precombine.field' = 'crawl_timestamp'
        )
    """)
    logger.info("✅ Table course_catalog créée")

def write_to_hudi(university, faculty):
    logger.info(f"🚀 Write Hudi : {faculty} — {university}")
    spark = get_spark_session()
    create_faculty_profiles_table(spark)
    create_course_catalog_table(spark)
    logger.info(f"✅ Tables Hudi créées pour {faculty}")
    spark.stop()

if __name__ == "__main__":
    write_to_hudi(
        university = "cadi_ayyad",
        faculty    = "FSSM"
    )