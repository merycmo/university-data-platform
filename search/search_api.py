from fastapi import FastAPI, HTTPException
from elasticsearch import Elasticsearch
from typing import Optional

app = FastAPI(title="University Search API", version="1.0")

es = Elasticsearch("http://localhost:9200")


@app.get("/search/courses")
def search_courses(q: Optional[str] = None, university: Optional[str] = None, faculty: Optional[str] = None, size: int = 10):
    if q:
        must = [{"multi_match": {"query": q, "fields": ["course_name^2", "text_content"]}}]
    else:
        must = [{"match_all": {}}]

    query_body = {
        "query": {
            "bool": {
                "must": must,
                "filter": [{"term": {"is_deleted": False}}]
            }
        },
        "size": size
    }
    if university:
        query_body["query"]["bool"]["filter"].append({"term": {"university": university}})
    if faculty:
        query_body["query"]["bool"]["filter"].append({"term": {"faculty": faculty}})

    try:
        response = es.search(index="course_catalog", body=query_body)
        hits = [hit["_source"] for hit in response["hits"]["hits"]]
        return {"total": response["hits"]["total"]["value"], "results": hits}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search/faculty")
def search_faculty(q: Optional[str] = None, university: Optional[str] = None, sort: Optional[str] = None, size: int = 10):
    if q:
        must = [{"multi_match": {"query": q, "fields": ["full_name^2", "institution_name"]}}]
    else:
        must = [{"match_all": {}}]

    query_body = {
        "query": {
            "bool": {
                "must": must,
                "filter": [{"term": {"is_deleted": False}}]
            }
        },
        "size": size
    }
    if university:
        query_body["query"]["bool"]["filter"].append({"term": {"university": university}})
    if sort in ("cited_by_count", "works_count"):
        query_body["sort"] = [{sort: {"order": "desc"}}]

    try:
        response = es.search(index="faculty_profiles", body=query_body)
        hits = [hit["_source"] for hit in response["hits"]["hits"]]
        return {"total": response["hits"]["total"]["value"], "results": hits}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))