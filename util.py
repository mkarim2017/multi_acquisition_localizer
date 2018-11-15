#!/usr/bin/env python 
import os, sys, time, json, requests, logging
from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
import datetime
import dateutil.parser
from datetime import datetime, timedelta

GRQ_URL = app.conf.GRQ_ES_URL


def get_dataset(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = GRQ_URL
    es_index = "grq_*_{}".format(index_suffix.lower())
    #es_index = "grq"

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result

def get_dataset(id):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = GRQ_URL
    #es_index = "grq_*_{}".format(index_suffix.lower())
    es_index = "grq"

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result


def get_partial_grq_data(id):
    es_url = GRQ_URL
    es_index = "grq"

    query = {
        "query": {
            "term": {
                "_id": id,
            },
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits'][0]


def get_query_data(query):
    es_url = GRQ_URL
    es_index = "grq"

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']


def get_acquisition_data(id):
    es_url = GRQ_URL
    es_index = "grq_*_*acquisition*"
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_id": id
              }
            }
          ]
        }
      },
      "partial_fields": {
        "partial": {
          "include": [
            "id",
            "dataset_type",
            "dataset",
            "metadata",
            "city",
            "continent"
          ]
        }
      }
    }


    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']


