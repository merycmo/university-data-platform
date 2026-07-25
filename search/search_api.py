# search/search_api.py

import sys
import json
import logging
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ES_HOST    = "localhost"
ES_PORT    = 9200
INDEX_NAME = "university_content"


def get_es_client():
    return Elasticsearch(host=ES_HOST, port=ES_PORT, scheme="http")


def search(query, university=None, faculty=None, file_type=None, size=10):
    es = get_es_client()

    must    = [{"multi_match": {"query": query, "fields": ["content", "title"]}}]
    filters = []

    if university:
        filters.append({"term": {"university": university}})
    if faculty:
        filters.append({"term": {"faculty": faculty}})
    if file_type:
        filters.append({"term": {"file_type": file_type}})

    body = {
        "query": {
            "bool": {
                "must"   : must,
                "filter" : filters
            }
        },
        "size": size
    }

    response = es.search(index=INDEX_NAME, body=body)
    hits     = response["hits"]["hits"]

    results = []
    for hit in hits:
        results.append({
            "score"       : hit["_score"],
            "record_id"   : hit["_source"].get("record_id", ""),
            "file_type"   : hit["_source"].get("file_type", ""),
            "university"  : hit["_source"].get("university", ""),
            "faculty"     : hit["_source"].get("faculty", ""),
            "source_url"  : hit["_source"].get("source_url", ""),
            "storage_path": hit["_source"].get("storage_path", ""),
            "content"     : hit["_source"].get("content", "")[:200]
        })

    logger.info(f"🔍 '{query}' → {len(results)} résultats")
    return results


if __name__ == "__main__":
    query      = sys.argv[1] if len(sys.argv) > 1 else "formation"
    university = sys.argv[2] if len(sys.argv) > 2 else None
    faculty    = sys.argv[3] if len(sys.argv) > 3 else None

    results = search(query=query, university=university, faculty=faculty)
    print(json.dumps(results, ensure_ascii=False, indent=2))