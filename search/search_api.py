#!/usr/bin/env python3
"""
Search API endpoint
Provides REST API for searching faculty and courses
"""

from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ─────────────────────────────────────────
# Elasticsearch Configuration
# ─────────────────────────────────────────
ES_HOST = "localhost"
ES_PORT = 9200

FACULTY_INDEX = "faculty_profiles"
COURSE_INDEX = "course_catalog"

es = Elasticsearch(hosts=[{"host": ES_HOST, "port": ES_PORT, "scheme": "http"}])

# ─────────────────────────────────────────
# Health Check
# ─────────────────────────────────────────
@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    try:
        if es.ping():
            return jsonify({"status": "healthy", "elasticsearch": "connected"}), 200
        else:
            return jsonify({"status": "unhealthy", "elasticsearch": "disconnected"}), 503
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ─────────────────────────────────────────
# Search Faculty
# ─────────────────────────────────────────
@app.route('/search/faculty', methods=['GET'])
def search_faculty():
    """
    Search faculty profiles
    
    Query parameters:
    - q: search query (prof_name, department, email)
    - department: filter by department
    - university: filter by university
    - limit: number of results (default: 10, max: 100)
    """
    
    try:
        query = request.args.get('q', '')
        department = request.args.get('department')
        university = request.args.get('university')
        limit = min(int(request.args.get('limit', 10)), 100)
        
        # Build Elasticsearch query
        es_query = {
            "size": limit,
            "query": {
                "bool": {
                    "must": [],
                    "filter": []
                }
            }
        }
        
        # Full-text search
        if query:
            es_query["query"]["bool"]["must"].append({
                "multi_match": {
                    "query": query,
                    "fields": ["prof_name", "department", "email"]
                }
            })
        
        # Filters
        if department:
            es_query["query"]["bool"]["filter"].append({
                "term": {"department.raw": department}
            })
        
        if university:
            es_query["query"]["bool"]["filter"].append({
                "term": {"university": university}
            })
        
        # Execute search
        results = es.search(index=FACULTY_INDEX, body=es_query)
        
        # Format results
        hits = []
        for hit in results['hits']['hits']:
            hits.append({
                "id": hit['_id'],
                "score": hit['_score'],
                "data": hit['_source']
            })
        
        return jsonify({
            "total": results['hits']['total']['value'],
            "results": hits
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Search error: {e}")
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────
# Search Courses
# ─────────────────────────────────────────
@app.route('/search/courses', methods=['GET'])
def search_courses():
    """
    Search course catalog
    
    Query parameters:
    - q: search query (course_name, department, prof_name)
    - department: filter by department
    - semester: filter by semester (S1, S2, etc.)
    - prof_name: filter by professor
    - university: filter by university
    - limit: number of results (default: 10, max: 100)
    """
    
    try:
        query = request.args.get('q', '')
        department = request.args.get('department')
        semester = request.args.get('semester')
        prof_name = request.args.get('prof_name')
        university = request.args.get('university')
        limit = min(int(request.args.get('limit', 10)), 100)
        
        # Build Elasticsearch query
        es_query = {
            "size": limit,
            "query": {
                "bool": {
                    "must": [],
                    "filter": []
                }
            }
        }
        
        # Full-text search
        if query:
            es_query["query"]["bool"]["must"].append({
                "multi_match": {
                    "query": query,
                    "fields": ["course_name", "department", "prof_name"]
                }
            })
        
        # Filters
        if department:
            es_query["query"]["bool"]["filter"].append({
                "term": {"department.raw": department}
            })
        
        if semester:
            es_query["query"]["bool"]["filter"].append({
                "term": {"semester": semester}
            })
        
        if prof_name:
            es_query["query"]["bool"]["filter"].append({
                "match": {"prof_name.raw": prof_name}
            })
        
        if university:
            es_query["query"]["bool"]["filter"].append({
                "term": {"university": university}
            })
        
        # Execute search
        results = es.search(index=COURSE_INDEX, body=es_query)
        
        # Format results
        hits = []
        for hit in results['hits']['hits']:
            hits.append({
                "id": hit['_id'],
                "score": hit['_score'],
                "data": hit['_source']
            })
        
        return jsonify({
            "total": results['hits']['total']['value'],
            "results": hits
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Search error: {e}")
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────
# Search All
# ─────────────────────────────────────────
@app.route('/search', methods=['GET'])
def search_all():
    """
    Combined search across faculty and courses
    """
    
    try:
        query = request.args.get('q', '')
        university = request.args.get('university')
        limit = min(int(request.args.get('limit', 10)), 100)
        
        if not query:
            return jsonify({"error": "Query parameter 'q' is required"}), 400
        
        # Search faculty
        faculty_query = {
            "size": limit,
            "query": {
                "bool": {
                    "must": [{
                        "multi_match": {
                            "query": query,
                            "fields": ["prof_name", "department"]
                        }
                    }],
                    "filter": []
                }
            }
        }
        
        if university:
            faculty_query["query"]["bool"]["filter"].append({
                "term": {"university": university}
            })
        
        # Search courses
        course_query = {
            "size": limit,
            "query": {
                "bool": {
                    "must": [{
                        "multi_match": {
                            "query": query,
                            "fields": ["course_name", "department", "prof_name"]
                        }
                    }],
                    "filter": []
                }
            }
        }
        
        if university:
            course_query["query"]["bool"]["filter"].append({
                "term": {"university": university}
            })
        
        faculty_results = es.search(index=FACULTY_INDEX, body=faculty_query)
        course_results = es.search(index=COURSE_INDEX, body=course_query)
        
        return jsonify({
            "faculty": {
                "total": faculty_results['hits']['total']['value'],
                "results": [
                    {
                        "id": hit['_id'],
                        "score": hit['_score'],
                        "data": hit['_source']
                    }
                    for hit in faculty_results['hits']['hits']
                ]
            },
            "courses": {
                "total": course_results['hits']['total']['value'],
                "results": [
                    {
                        "id": hit['_id'],
                        "score": hit['_score'],
                        "data": hit['_source']
                    }
                    for hit in course_results['hits']['hits']
                ]
            }
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Search error: {e}")
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────
# RUN
# ─────────────────────────────────────────
if __name__ == '__main__':
    print("\n" + "="*70)
    print("🔍 SEARCH API STARTED")
    print("="*70)
    print("\nAvailable endpoints:")
    print("  GET /health")
    print("  GET /search?q=Ahmed")
    print("  GET /search/faculty?q=Ahmed&department=Histoire")
    print("  GET /search/courses?q=Géographie&semester=S2")
    print("\nServer running on http://localhost:5000\n")
    
    app.run(host='0.0.0.0', port=5000, debug=False)