#!/usr/bin/env python3
"""
GENERAL Elasticsearch indexing from Hive
Reads faculty_profiles and course_catalog from Hive
Parameterized for all universities and faculties
"""

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
import json
import logging
import hashlib
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
ES_HOST = "localhost"
ES_PORT = 9200

FACULTY_INDEX = "faculty_profiles"
COURSE_INDEX = "course_catalog"

# ─────────────────────────────────────────
# Elasticsearch Client
# ─────────────────────────────────────────
def get_es_client():
    """Get Elasticsearch client"""
    return Elasticsearch(
        hosts=[{"host": ES_HOST, "port": ES_PORT, "scheme": "http"}]
    )

# ─────────────────────────────────────────
# Spark Session with Hive
# ─────────────────────────────────────────
def get_spark_session():
    """Get Spark session with Hive support"""
    return SparkSession.builder \
        .appName("ElasticsearchIndexing") \
        .enableHiveSupport() \
        .master("local[*]") \
        .getOrCreate()

# ─────────────────────────────────────────
# Create Indices with Mappings
# ─────────────────────────────────────────
def create_faculty_index(es):
    """Create faculty_profiles index"""
    
    if es.indices.exists(index=FACULTY_INDEX):
        logger.info(f"✅ Index {FACULTY_INDEX} exists")
        return
    
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "arabic_analyzer": {"type": "arabic"},
                    "french_analyzer": {"type": "french"}
                }
            }
        },
        "mappings": {
            "properties": {
                "prof_id": {"type": "keyword"},
                "prof_name": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword"},
                        "french": {"type": "text", "analyzer": "french_analyzer"},
                        "arabic": {"type": "text", "analyzer": "arabic_analyzer"}
                    }
                },
                "department": {
                    "type": "text",
                    "fields": {"raw": {"type": "keyword"}}
                },
                "faculty": {"type": "keyword"},
                "email": {"type": "keyword"},
                "university": {"type": "keyword"},
                "indexed_timestamp": {"type": "date"}
            }
        }
    }
    
    es.indices.create(index=FACULTY_INDEX, body=mapping)
    logger.info(f"✅ Index {FACULTY_INDEX} created")

def create_course_index(es):
    """Create course_catalog index"""
    
    if es.indices.exists(index=COURSE_INDEX):
        logger.info(f"✅ Index {COURSE_INDEX} exists")
        return
    
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "arabic_analyzer": {"type": "arabic"},
                    "french_analyzer": {"type": "french"}
                }
            }
        },
        "mappings": {
            "properties": {
                "course_id": {"type": "keyword"},
                "course_name": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword"},
                        "french": {"type": "text", "analyzer": "french_analyzer"},
                        "arabic": {"type": "text", "analyzer": "arabic_analyzer"}
                    }
                },
                "department": {
                    "type": "text",
                    "fields": {"raw": {"type": "keyword"}}
                },
                "semester": {"type": "keyword"},
                "prof_name": {
                    "type": "text",
                    "fields": {"raw": {"type": "keyword"}}
                },
                "faculty": {"type": "keyword"},
                "university": {"type": "keyword"},
                "indexed_timestamp": {"type": "date"}
            }
        }
    }
    
    es.indices.create(index=COURSE_INDEX, body=mapping)
    logger.info(f"✅ Index {COURSE_INDEX} created")

# ─────────────────────────────────────────
# Index Faculty Profiles from Hive
# ─────────────────────────────────────────
def index_faculty_profiles(spark, es, university=None, faculty=None):
    """Index faculty_profiles from Hive to Elasticsearch"""
    
    logger.info(f"🚀 Indexing faculty_profiles for {university}/{faculty}")
    
    # Read from Hive
    query = "SELECT * FROM university_db.faculty_profiles"
    if university:
        query += f" WHERE university = '{university}'"
    
    df = spark.sql(query)
    
    if df.count() == 0:
        logger.warning(f"⚠️ No faculty data found")
        return 0
    
    indexed = 0
    errors = 0
    
    for row in df.collect():
        try:
            doc_id = row.prof_id
            document = {
                "prof_id": row.prof_id,
                "prof_name": row.prof_name,
                "department": row.department,
                "faculty": row.faculty,
                "email": row.email,
                "university": row.university if hasattr(row, 'university') else "unknown",
                "indexed_timestamp": datetime.now().isoformat()
            }
            
            es.index(index=FACULTY_INDEX, id=doc_id, document=document)
            indexed += 1
            
        except Exception as e:
            logger.error(f"❌ Error indexing {row.prof_id}: {e}")
            errors += 1
    
    logger.info(f"✅ Indexed {indexed} faculty profiles ({errors} errors)")
    return indexed

# ─────────────────────────────────────────
# Index Course Catalog from Hive
# ─────────────────────────────────────────
def index_course_catalog(spark, es, university=None, faculty=None):
    """Index course_catalog from Hive to Elasticsearch"""
    
    logger.info(f"🚀 Indexing course_catalog for {university}/{faculty}")
    
    # Read from Hive
    query = "SELECT * FROM university_db.course_catalog"
    if university:
        query += f" WHERE university = '{university}'"
    
    df = spark.sql(query)
    
    if df.count() == 0:
        logger.warning(f"⚠️ No course data found")
        return 0
    
    indexed = 0
    errors = 0
    
    for row in df.collect():
        try:
            doc_id = row.course_id
            document = {
                "course_id": row.course_id,
                "course_name": row.course_name,
                "department": row.department,
                "semester": row.semester,
                "prof_name": row.prof_name,
                "faculty": row.faculty,
                "university": row.university if hasattr(row, 'university') else "unknown",
                "indexed_timestamp": datetime.now().isoformat()
            }
            
            es.index(index=COURSE_INDEX, id=doc_id, document=document)
            indexed += 1
            
        except Exception as e:
            logger.error(f"❌ Error indexing {row.course_id}: {e}")
            errors += 1
    
    logger.info(f"✅ Indexed {indexed} courses ({errors} errors)")
    return indexed

# ─────────────────────────────────────────
# MAIN FUNCTION (for Airflow)
# ─────────────────────────────────────────
def index_to_elasticsearch(university="hassan2", faculty="FLSH"):
    """
    GENERAL function to index data from Hive to Elasticsearch
    
    Parameters:
    - university: "hassan2", "cadi_ayyad", etc.
    - faculty: "FLSH", "FSAC", "FSJE", etc.
    """
    
    print("\n" + "="*70)
    print(f"📚 ELASTICSEARCH INDEXING: {university} / {faculty}")
    print("="*70 + "\n")
    
    spark = get_spark_session()
    es = get_es_client()
    
    try:
        # Check Elasticsearch connection
        print("1️⃣ Checking Elasticsearch...")
        if es.ping():
            print("   ✅ Connected to Elasticsearch\n")
        else:
            logger.error("❌ Cannot connect to Elasticsearch")
            return False
        
        # Create indices
        print("2️⃣ Creating indices...")
        create_faculty_index(es)
        create_course_index(es)
        print()
        
        # Index faculty profiles
        print("3️⃣ Indexing faculty profiles...")
        faculty_count = index_faculty_profiles(spark, es, university, faculty)
        print()
        
        # Index courses
        print("4️⃣ Indexing course catalog...")
        course_count = index_course_catalog(spark, es, university, faculty)
        print()
        
        # Verify
        print("5️⃣ Verification:")
        faculty_stats = es.indices.stats(index=FACULTY_INDEX)
        course_stats = es.indices.stats(index=COURSE_INDEX)
        
        print(f"   • Faculty profiles: {faculty_stats['indices'][FACULTY_INDEX]['primaries']['docs']['count']} docs")
        print(f"   • Courses: {course_stats['indices'][COURSE_INDEX]['primaries']['docs']['count']} docs\n")
        
        print("="*70)
        print("✅ Elasticsearch indexing completed successfully")
        print("="*70)
        print("\nYou can search now:")
        print(f"  GET /{FACULTY_INDEX}/_search?q=Ahmed")
        print(f"  GET /{COURSE_INDEX}/_search?q=Géographie\n")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        spark.stop()

# ─────────────────────────────────────────
# TEST
# ─────────────────────────────────────────
if __name__ == "__main__":
    success = index_to_elasticsearch(
        university="hassan2",
        faculty="FLSH"
    )
    exit(0 if success else 1)