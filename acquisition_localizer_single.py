#!/usr/bin/env python 
import os, sys, time, json, requests, logging

from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


BASE_PATH = os.path.dirname(__file__)


def dataset_exists(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = app.conf.GRQ_ES_URL
    es_index = "grq_*_{}".format(index_suffix.lower())
    
    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } },
                ]
            }
        },
        "fields": [],
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        total = result['hits']['total']
    else:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        if r.status_code == 404: total = 0
        else: r.raise_for_status()
    return False if total == 0 else True


def query_es(query, es_index):
    """Query ES."""

    es_url = app.conf.GRQ_ES_URL
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #logger.info("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    #logger.info("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits


def query_aois(starttime, endtime):
    """Query ES for active AOIs that intersect starttime and endtime."""

    es_index = "grq_*_area_of_interest"
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "filtered": {
                            "query": {
                                "range": {
                                    "starttime": {
                                        "lte": endtime
                                    }
                                }
                            },
                            "filter": {
                                "missing": {
                                    "field": "endtime"
                                }
                            }
                        }
                    },
                    {
                        "filtered": {
                            "query": {
                                "range": {
                                    "endtime": {
                                        "gte": starttime
                                    }
                                }
                            },
                            "filter": {
                                "missing": {
                                    "field": "starttime"
                                }
                            }
                        }
                    }
                ]
            }
        },
        "partial_fields" : {
            "partial" : {
                "include" : [ "id", "starttime", "endtime", "location", 
                              "metadata.user_tags", "metadata.priority" ]
            }
        }
    }

    # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query, es_index) 
            if 'inactive' not in i['fields']['partial'][0].get('metadata', {}).get('user_tags', [])]
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits


def query_aoi_acquisitions(starttime, endtime, platform):
    """Query ES for active AOIs that intersect starttime and endtime and 
       find acquisitions that intersect the AOI polygon for the platform."""

    acq_info = {}
    es_index = "grq_*_*acquisition*"
    for aoi in query_aois(starttime, endtime):
        logger.info("aoi: {}".format(aoi['id']))
        query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset_type.raw": "acquisition"
                                    }
                                },
                                {
                                    "term": {
                                        "metadata.platform.raw": platform
                                    }
                                },
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "filter": {
                        "geo_shape": {  
                            "location": {
                                "shape": aoi['location']
                            }
                        }
                    }
                }
            },
            "partial_fields" : {
                "partial" : {
                    "include" : [ "id", "dataset_type", "dataset", "metadata" ]
                }
            }
        }
        acqs = [i['fields']['partial'][0] for i in query_es(query, es_index)]
        logger.info("Found {} acqs for {}: {}".format(len(acqs), aoi['id'],
                    json.dumps([i['id'] for i in acqs], indent=2)))
        for acq in acqs:
            aoi_priority = aoi.get('metadata', {}).get('priority', 0)
            # ensure highest priority is assigned if multiple AOIs resolve the acquisition
            if acq['id'] in acq_info and acq_info[acq['id']].get('priority', 0) > aoi_priority:
                continue
            acq['aoi'] = aoi['id']
            acq['priority'] = aoi_priority
            acq_info[acq['id']] = acq
    logger.info("Acquistions to localize: {}".format(json.dumps(acq_info, indent=2)))
    return acq_info
    

def resolve_s1_slc(identifier, download_url, asf_queue, esa_queue):
    """Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA."""

    # determine best url and corresponding queue
    vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(identifier)
    r = requests.head(vertex_url, allow_redirects=True)
    if r.status_code == 403:
        url = r.url
        queue = asf_queue
    elif r.status_code == 404:
        url = download_url
        queue = esa_queue
    else:
        url = download_url
        queue = esa_queue
        #raise RuntimeError("Got status code {} from {}: {}".format(r.status_code, vertex_url, r.url))
    return url, queue


class DatasetExists(Exception):
    """Exception class for existing dataset."""
    pass


def resolve_source_from_ctx(ctx):
    """Resolve best URL from acquisition."""

    dataset_type = ctx['dataset_type']
    identifier = ctx['identifier']
    dataset = ctx['dataset']
    download_url = ctx['download_url']
    asf_ngap_download_queue = ctx['asf_ngap_download_queue']
    esa_download_queue = ctx['esa_download_queue']
    job_priority = ctx.get('job_priority', 0)
    aoi = ctx.get('aoi', 'no_aoi')
    spyddder_extract_version = ctx['spyddder_extract_version']
    archive_filename = ctx['archive_filename']

    return resolve_source(dataset_type, identifier, dataset, download_url, asf_ngap_download_queue, esa_download_queue, spyddder_extract_version,archive_filename, job_priority, aoi)


def resolve_source(dataset_type, identifier, dataset, download_url, asf_ngap_download_queue, esa_download_queue, spyddder_extract_version, archive_filename, job_priority, aoi):
   
    # get settings
    '''
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
    with open(settings_file) as f:
        settings = json.load(f)
    '''
    settings = {}
    settings['ACQ_TO_DSET_MAP'] = {"acquisition-S1-IW_SLC": "S1-IW_SLC"}
   

    # ensure acquisition
    if dataset_type != "acquisition":
        raise RuntimeError("Invalid dataset type: {}".format(dataset_type))

    # route resolver and return url and queue
    if dataset == "acquisition-S1-IW_SLC":
        '''
        if dataset_exists(identifier, settings['ACQ_TO_DSET_MAP'][dataset]):
            raise DatasetExists("Dataset {} already exists.".format(identifier))
        '''
        url, queue = resolve_s1_slc(identifier, download_url, asf_ngap_download_queue, esa_download_queue)
    else:
        raise NotImplementedError("Unknown acquisition dataset: {}".format(dataset))

    return extract_job(spyddder_extract_version, queue, url, archive_filename, identifier, time.strftime('%Y-%m-%d' ), job_priority, aoi)

def resolve_source_from_ctx_file(ctx_file):
    """Resolve best URL from acquisition."""

    with open(ctx_file) as f:
        return resolve_source(json.load(f))


def extract_job(spyddder_extract_version, queue, localize_url, file, prod_name,
                prod_date, priority, aoi, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    '''
    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")
    '''

    # set job type and disk space reqs
    job_type = "job-spyddder-extract:{}".format(spyddder_extract_version)

    # resolve hysds job
    params = {
        "localize_url": localize_url,
        "file": file,
        "prod_name": prod_name,
        "prod_date": prod_date,
        "aoi": aoi,
    }
    job = resolve_hysds_job(job_type, queue, priority=priority, params=params, 
                            job_name="%s-%s-%s" % (job_type, aoi, prod_name))

    # save to archive_filename if it doesn't match url basename
    if os.path.basename(localize_url) != file:
        job['payload']['localize_urls'][0]['local_path'] = file

    # add workflow info
    if wuid is not None and job_num is not None:
        job['payload']['_sciflo_wuid'] = wuid
        job['payload']['_sciflo_job_num'] = job_num
    print("job: {}".format(json.dumps(job, indent=2)))

    return submit_hysds_job(job)

def submit_sling(ctx_file):
    """Submit sling for S1 SLC from acquisition."""

    # get context
    with open(ctx_file) as f:
        ctx = json.load(f)
    logger.info("ctx: {}".format(json.dumps(ctx, indent=2)))

    # filter non acquisitions
    if ctx.get('source_dataset', None) != "acquisition-S1-IW_SLC":
        raise RuntimeError("Skipping invalid acquisition dataset.")

    # build payload items for job submission
    qtype = "scihub"
    archive_fname = ctx['archive_filename']
    title, ext = archive_fname.split('.')
    start_dt = get_date(ctx['starttime'])
    yr = start_dt.year
    mo = start_dt.month
    dy = start_dt.day
    logger.info("starttime: {}".format(start_dt))
    md5 = hashlib.md5("{}\n".format(archive_fname)).hexdigest()
    repo_url = "{}/{}/{}/{}/{}/{}".format(ctx['repo_url'], md5[0:8], md5[8:16],
                                          md5[16:24], md5[24:32], archive_fname)
    logger.info("repo_url: {}".format(repo_url))
    prod_met = {}
    prod_met['source'] = qtype
    prod_met['dataset_type'] = title[0:3]
    prod_met['spatial_extent'] = {
        'type': 'polygon',
        'aoi': None,
        'coordinates': ctx['prod_met']['location']['coordinates'],
    }
    prod_met['tag'] = []

    #required params for job submission
    job_type = "job:spyddder-sling_%s" % qtype
    oauth_url = None
    queue = "factotum-job_worker-%s_throttled" % qtype # job submission queue

    #set sling job spec release/branch
    #job_spec = "job-sling:release-20170619"
    job_spec = "job-sling:{}".format(ctx['sling_release'])
    rtime = datetime.utcnow()
    job_name = "%s-%s-%s-%s" % (job_spec, queue, archive_fname, rtime.strftime("%d_%b_%Y_%H:%M:%S"))
    job_name = job_name.lstrip('job-')

    #Setup input arguments here
    rule = {
        "rule_name": job_spec,
        "queue": queue,
        "priority": ctx.get('job_priority', 0),
        "kwargs":'{}'
    }
    params = [
        { "name": "download_url",
          "from": "value",
          "value": ctx['download_url'],
        },
        { "name": "repo_url",
          "from": "value",
          "value": repo_url,
        },
        { "name": "prod_name",
          "from": "value",
          "value": title,
        },
        { "name": "file_type",
          "from": "value",
          "value": ext,
        },
        { "name": "prod_date",
          "from": "value",
          "value": "{}".format("%04d-%02d-%02d" % (yr, mo, dy)),
        },
        { "name": "prod_met",
          "from": "value",
          "value": prod_met,
        },
        { "name": "options",
          "from": "value",
          "value": "--force_extract"
        }
    ]

    logger.info("rule: {}".format(json.dumps(rule, indent=2)))
    logger.info("params: {}".format(json.dumps(params, indent=2)))

    submit_mozart_job({}, rule,
        hysdsio={"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": job_spec},
        job_name=job_name)

