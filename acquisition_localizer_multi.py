#!/usr/bin/env python 
import os, sys, time, json, requests, logging
import hashlib
from datetime import datetime
#from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
import util
import uuid  # only need this import to simulate returned mozart job id
from hysds.celery import app
from hysds_commons.job_utils import submit_mozart_job
import traceback

import acquisition_localizer_single

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


IFG_CFG_ID_TMPL = "ifg-cfg_R{}_M{:d}S{:d}_TN{:03d}_{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}-{}-{}"

BASE_PATH = os.path.dirname(__file__)
MOZART_URL = app.conf['MOZART_URL']
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"
sleep_seconds = 120
slc_check_max_sec = 300
sling_completion_max_sec = 10800


class ACQ:
    def __init__(self, acq_id, acq_data, localized=False, job_id=None, job_status = None):
        self.acq_id=acq_id
        self.acq_data = acq_data
        self.localized = localized
        self.job_id = job_id
        self.job_status = job_status

def get_acq_object(acq_id, acq_data, localized=False, job_id=None, job_status = None):
    return {
        "acq_id": acq_id,
        "acq_data" :acq_data,
        "localized" : localized,
        "job_id": job_id,
        "job_status": job_status


    }


def query_es(endpoint, doc_id):
    """
    This function queries ES
    :param endpoint: the value specifies which ES endpoint to send query
     can be MOZART or GRQ
    :param doc_id: id of product or job
    :return: result from elasticsearch
    """
    es_url, es_index = None, None
    if endpoint == GRQ_ES_ENDPOINT:
        es_url = app.conf["GRQ_ES_URL"]
        es_index = "grq"
    if endpoint == MOZART_ES_ENDPOINT:
        es_url = app.conf['JOBS_ES_URL']
        es_index = "job_status-current"

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}} # add job status:
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)

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

    if len(result["hits"]["hits"]) == 0:
        raise ValueError("Couldn't find record with ID: %s, at ES: %s"%(doc_id, es_url))
        return

    #LOGGER.debug("Got: {0}".format(json.dumps(result)))
    return result



def get_job_status(job_id):
    """
    This function gets the staged products and context of previous PGE job
    :param job_id: this is the id of the job on mozart
    :return: tuple(products_staged, prev_context, message)
    the message refects the
    """
    endpoint = MOZART_ES_ENDPOINT
    return_job_id = None
    return_job_status = None

    #check if Jobs ES has updated job status
    if check_ES_status(job_id):
        response = query_es(endpoint, job_id)

    result = response["hits"]["hits"][0]
    message = None  #using this to store information regarding deduped jobs, used later to as error message unless it's value is "success"

    #print ("Job INFO retrieved from ES: %s"%json.dumps(result))
    #print ("Type of status from ES: %s"%type(result["_source"]["status"]))
    status = str(result["_source"]["status"])
    if status == "job-deduped":
        #query ES for the original job's status
        orig_job_id = result["_source"]["dedup_job"]
        return_job_id = orig_job_id
        orig_job_info = query_es(endpoint, orig_job_id)
        """check if original job failed -> this would happen when at the moment of deduplication, the original job
         was in 'running state', but soon afterwards failed. So, by the time the status is checked in this function,
         it may be shown as failed."""
        #print ("Original JOB info: \n%s"%json.dumps(orig_job_info))
        orig_job_info = orig_job_info["hits"]["hits"][0]
        orig_job_status = str(orig_job_info["_source"]["status"])
        logger.info("Check Job Status : Job %s was Deduped. The new/origianl job id is %s whose status is : %s" %(job_id, return_job_id, return_job_status)) 
        return_job_status = orig_job_status

        if  orig_job_status == "job-failed":
            message = "Job was deduped against a failed job with id: %s, please retry job."%orig_job_id
            logger.info(message) 
        elif orig_job_status == "job-started" or orig_job_status == "job-queued":
            logger.info ("Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id)
            message = "Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id
        elif orig_job_status == "job-completed":
            # return products staged and context of original job
            message = "success"
    else:
        return_job_id = job_id
        return_job_status = result["_source"]["status"]

    return return_job_status, return_job_id

def check_ES_status(doc_id):
    """
    There is a latency in the update of ES job status after
    celery signals job completion.
    To handle that case, we much poll ES (after sciflo returns status after blocking)
    until the job status is correctly reflected.
    :param doc_id: ID of the Job ES doc
    :return: True  if the ES has updated job status within 5 minutes
            otherwise raise a run time error
    """
    es_url = app.conf['JOBS_ES_URL']
    es_index = "job_status-current"
    query = {
        "_source": [
                   "status"
               ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}}
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)
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


    sleep_seconds = 2
    timeout_seconds = 300
    # poll ES until job status changes from "job-started" or for the job doc to show up. The poll will timeout soon after 5 mins.

    while len(result["hits"]["hits"]) == 0: #or str(result["hits"]["hits"][0]["_source"]["status"]) == "job-started":
        if sleep_seconds >= timeout_seconds:
            if len(result["hits"]["hits"]) == 0:
                raise RuntimeError("ES taking too long to index job with id %s."%doc_id)
            else:
                raise RuntimeError("ES taking too long to update status of job with id %s."%doc_id)
        time.sleep(sleep_seconds)
        #result = ES.search(index=es_index, body=query)

        r = requests.post(search_url, data=json.dumps(query))

        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        result = r.json()
        sleep_seconds = sleep_seconds * 2

    logging.info("Job status updated on ES to %s"%str(result["hits"]["hits"][0]["_source"]["status"]))
    return True

def check_slc_status(slc_id, index_suffix):

    result = util.get_dataset(slc_id, index_suffix)
    total = result['hits']['total']

    if total > 0:
        return True

    return False

def check_slc_status(slc_id):

    result = util.get_dataset(slc_id)
    total = result['hits']['total']

    if total > 0:
        return True

    return False
def get_acq_data_from_list(acq_list):
    logger.info("get_acq_data_from_list")
    acq_info = {}
    # Find out status of all Master ACQs, create a ACQ object with that and update acq_info dictionary 
    for acq in acq_list: 
        #logger.info(acq) 
        #acq_data = util.get_acquisition_data(acq)[0]['fields']['partial'][0] 
        acq_data = util.get_partial_grq_data(acq)['fields']['partial'][0] 
        status = check_slc_status(acq_data['metadata']['identifier']) 
        if status: 
            # status=1 
            logger.info("%s exists" %acq_data['metadata']['identifier']) 
            acq_info[acq]=get_acq_object(acq, acq_data, 1) 
        else: 
            #status = 0 
            logger.info("%s does NOT exist"%acq_data['metadata']['identifier']) 
            acq_info[acq]=get_acq_object(acq, acq_data, 0)
    return acq_info

def get_acq_data_from_query(query):
    logger.info("get_acq_data_from_query")
    acq_info = {}

    hits = util.get_query_data(query)
    if hits["total"] == 0:
        raise RuntimeError("No Acquisition Found that Matched the Criteria.")
    else:
        logger.info("get_acq_data_from_query : Found %s data" %hits["total"])
 
    for i in range (len(hits["hits"])):
        acq_data = hits["hits"][i]["_source"]
        #logger.info("\n%s" %acq_data)
        acq = hits["hits"][i]["_id"]
        status = check_slc_status(acq_data['metadata']['identifier'])
        if status:
            # status=1
            logger.info("%s exists" %acq_data['metadata']['identifier'])
            acq_info[acq]=get_acq_object(acq, acq_data, 1)
        else:
            #status = 0
            logger.info("%s does NOT exist"%acq_data['metadata']['identifier'])
            acq_info[acq]=get_acq_object(acq, acq_data, 0)
    
    return acq_info 

def resolve_source(ctx_file):
    """Resolve best URL from acquisition."""


    # get settings
    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)
    
    spyddder_extract_version = "develop"
    acq_info = {}

    acq_list = ctx['products'] if isinstance(ctx['products'], list) else [ctx['products']]
    logger.info("Acq List Type : %s" %type(acq_list))
 
    if "spyddder_extract_version" in ctx:
        spyddder_extract_version = ctx["spyddder_extract_version"]

    asf_ngap_download_queue = ctx['asf_ngap_download_queue']
    esa_download_queue = ctx['esa_download_queue']

    job_priority = ctx["job_priority"]
    job_type, job_version = ctx['job_specification']['id'].split(':') 
    acquisition_localizer_version = job_version

    queues = []  # where should we get the queue value
    identifiers = []
    prod_dates = []
   

    
    index_suffix = "S1-IW_ACQ"


    return sling(acq_list, spyddder_extract_version, acquisition_localizer_version, esa_download_queue, asf_ngap_download_queue, job_priority, job_type, job_version)



def sling(acq_list, spyddder_extract_version, acquisition_localizer_version, esa_download_queue, asf_ngap_download_queue, job_priority, job_type, job_version):
    '''
	This function checks if any ACQ that has not been ingested yet and sling them.
    '''

    acq_info = get_acq_data_from_list(acq_list)

    #logger.info("acq_info type: %s : %s" %(type(acq_info), len(acq_info) ))
    #logger.info(acq_info)
    logger.info("%s : %s" %(type(spyddder_extract_version), spyddder_extract_version))
    # acq_info has now all the ACQ's status. Now submit the Sling job for the one's whose status = 0 and update the slc_info with job id
    for acq_id in acq_info.keys():
        acq_info[acq_id]['localized'] = False
        if not acq_info[acq_id]['localized']:
            acq_data = acq_info[acq_id]['acq_data']
            job_id = submit_sling_job(spyddder_extract_version, acquisition_localizer_version, esa_download_queue, asf_ngap_download_queue, acq_data, job_priority)
 
            acq_info[acq_id]['job_id'] = job_id
            job_status, new_job_id  = get_job_status(job_id)
            acq_info[acq_id]['job_id'] = new_job_id
            acq_info[acq_id]['job_status'] = job_status


    # Now loop in until all the jobs are completed 
    all_done = False
    sling_check_start_time = datetime.utcnow()
    while not all_done:

        for acq_id in acq_info.keys():
            acq_data = acq_info[acq_id]['acq_data']
            if not acq_info[acq_id]['localized']: 
                job_status, job_id  = get_job_status(acq_info[acq_id]['job_id'])  
                if job_status == "job-completed":
                    logger.info("Success! sling job for slc : %s  with job id : %s COMPLETED!!" %(acq_data['metadata']['identifier'], job_id))
                    acq_info[acq_id]['job_id'] = job_id
                    acq_info[acq_id]['job_status'] = job_status
                    acq_info[acq_id]['localized'] = check_slc_status(acq_data['metadata']['identifier'])

                elif job_status == "job-failed":
                    err_msg = "Error : Sling job %s FAILED. So existing out of the sciflo!!....." %job_id
                    logger.info(err_msg)
                    raise RuntimeError(err_msg)

                else:
                    acq_info[acq_id]['job_id'] = job_id
                    acq_info[acq_id]['job_status'] = job_status
                    logger.info("Sling job for %s  : Job id : %s. Job Status : %s" %(acq_info[acq_id], acq_info[acq_id]['job_id'], acq_info[acq_id]['job_status']))

        logger.info("Checking if all job completed")
        all_done = check_all_job_completed(acq_info)
        if not all_done:
            now = datetime.utcnow()
            delta = (now - sling_check_start_time).total_seconds()
            if delta >= sling_completion_max_sec:
                raise RuntimeError("Error : Sling jobs NOT completed after %.2f hours!!" %(delta/3600))
            logger.info("All job not completed. So sleeping for %s seconds" %sleep_seconds)
            time.sleep(sleep_seconds)



    #At this point, all sling jobs have been completed. Now lets recheck the slc status


    logger.info("\nAll sling jobs have been completed. Now lets recheck the slc status")
    all_exists = False
    index_suffix = "S1-IW_ACQ"
    slc_check_start_time = datetime.utcnow()
    slcs_not_exist = []
    while not all_exists:
        all_exists = True
        slcs_not_exist = []
        for acq_id in acq_info.keys():
            if not acq_info[acq_id]['localized']:
                acq_data = acq_info[acq_id]['acq_data']
                acq_info[acq_id]['localized'] = check_slc_status(acq_data['metadata']['identifier'])
		
                if not acq_info[acq_id]['localized']:
                    logger.info("%s NOT localized!!" %acq_data['metadata']['identifier'])
                    all_exists = False
                    slcs_not_exist.append(acq_id)
                    break
        if not all_exists:
            now = datetime.utcnow()
            delta = (now-slc_check_start_time).total_seconds()
            if delta >= slc_check_max_sec:
                raise RuntimeError("Error : SLC not available %.2f min after sling jobs completed!!" %(delta/60))
            time.sleep(60)
    
    #At this point we have all the slcs localized
    if all_exists:
        localized_data = get_output_data(acq_info)
        return True, localized_data
    else:
        return False, slcs_not_exist
        
    
    

    '''
    logger.info("\n\nLocalized Acquisitions:")
    if localized_data:
        for acq in localized_data.keys():
            for acq_data in acq:
                logger.info("%s : %s" %(acq_data, acq[acq_data]))
            logger.info("\n")

    ''' 


def get_output_data(acq_info):
    localized_data = {}
    for acq_id in acq_info.keys():
        if not acq_info[acq_id]['localized']:
            return None
        acq_data = acq_info[acq_id]['acq_data']
        slc_data = util.get_partial_grq_data(acq_data['metadata']['identifier'])['fields']['partial'][0] 
        localize_url = ""
        urls = slc_data['urls']
        for url in urls:
            localize_url = url
            if localize_url.startswith('s3://'):
                break
        
        acq_localized_data = {}
        acq_localized_data['acquisition'] = acq_id
        acq_localized_data['identifier'] = acq_data['metadata']['identifier']
        acq_localized_data['localized'] = acq_info[acq_id]['localized']
        acq_localized_data['urls'] = urls
        acq_localized_data['localize_url'] = localize_url
        localized_data[acq_id] = acq_localized_data
    return localized_data


        

def check_all_job_completed(acq_info):
    all_done = True
    for acq_id in acq_info.keys():
        if not acq_info[acq_id]['localized']:  
            job_status = acq_info[acq_id]['job_status']
            if not job_status == "job-completed":
                logger.info("check_all_job_completed : %s NOT completed!!" %acq_info[acq_id]['job_id'])	
                all_done = False
                break
    return all_done




def submit_sling_job(spyddder_extract_version, acquisition_localizer_version, esa_download_queue, asf_ngap_download_queue, acq_data, priority):
    identifier = acq_data["metadata"]["identifier"]
    dataset_type = acq_data["dataset_type"]
    dataset = acq_data["dataset"]
    download_url = acq_data["metadata"]["download_url"]
    archive_filename = acq_data["metadata"]["archive_filename"]
    aoi = "no_aoi"

    return acquisition_localizer_single.resolve_source(dataset_type, identifier, dataset, download_url, asf_ngap_download_queue, esa_download_queue, spyddder_extract_version,archive_filename, priority, aoi)


def submit_sling_job2(spyddder_extract_version, acquisition_localizer_version, esa_download_queue, asf_ngap_download_queue, acq_data, priority):

    """Map function for spyddder-man extract job."""

    #acquisition_localizer_version = "master"
    #spyddder_extract_version = "develop"
    job_submit_url = '%s/mozart/api/v0.1/job/submit' % MOZART_URL

    # set job type and disk space reqs
    job_type = "job-acquisition_localizer_single:{}".format(acquisition_localizer_version)
    logger.info("\nSubmitting job of type : %s" %job_type)
     # set job type and disk space reqs
    disk_usage = "10GB"
    #logger.info(acq_data)
    #acq_id = acq_data['acq_id']

    # set job queue
    job_queue = "system-jobs-queue" 
    rule = {
        "rule_name": "acquisition_localizer_multi-sling",
        "queue": job_queue,
        "priority": priority,
        "kwargs":'{}'
    }

    sling_job_name = "sling-%s-%s" %(job_type, acq_data["metadata"]["identifier"])


    params = [
	{
            "name": "workflow",
            "from": "value",
            "value": "acquisition_localizer.sf.xml"
        },
        {
            "name": "asf_ngap_download_queue",
            "from": "value",
            "value": asf_ngap_download_queue
        },
        {
            "name": "esa_download_queue",
            "from": "value",
            "value": esa_download_queue
        },
        {
            "name": "spyddder_extract_version",
            "from": "value",
            "value": spyddder_extract_version
        },
        {
            "name": "dataset_type",
            "from": "value",
            "value": acq_data["dataset_type"]
        },
        {
            "name": "dataset",
            "from": "value",
            "value": acq_data["dataset"]
        },
        {
            "name": "identifier",
            "from": "value",
            "value": acq_data["metadata"]["identifier"]
        },
        {
            "name": "download_url",
            "from": "value",
            "value": acq_data["metadata"]["download_url"]
        },
        {
            "name": "archive_filename",
            "from": "value",
            "value": acq_data["metadata"]["archive_filename"]
        },
        {
            "name": "prod_met",
            "from": "value",
            "value": acq_data["metadata"]
        }
    ]
    

    logger.info("PARAMS : %s" %params)
    logger.info("RULE : %s"%rule)
    logger.info(job_type)
    logger.info(sling_job_name)

    mozart_job_id = submit_mozart_job({}, rule,hysdsio={"id": "internal-temporary-wiring", "params": params, "job-specification": job_type}, job_name=sling_job_name)
    logger.info("\nSubmitted sling job with id %s for  %s" %(acq_data["metadata"]["identifier"], mozart_job_id))

    return mozart_job_id

def check_ES_status(doc_id):
    """
    There is a latency in the update of ES job status after
    celery signals job completion.
    To handle that case, we much poll ES (after sciflo returns status after blocking)
    until the job status is correctly reflected.
    :param doc_id: ID of the Job ES doc
    :return: True  if the ES has updated job status within 5 minutes
            otherwise raise a run time error
    """
    es_url = app.conf['JOBS_ES_URL']
    es_index = "job_status-current"
    query = {
        "_source": [
                   "status"
               ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}}
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)
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


    sleep_seconds = 2
    timeout_seconds = 300
    # poll ES until job status changes from "job-started" or for the job doc to show up. The poll will timeout soon after 5 mins.

    while len(result["hits"]["hits"]) == 0: #or str(result["hits"]["hits"][0]["_source"]["status"]) == "job-started":
        if sleep_seconds >= timeout_seconds:
            if len(result["hits"]["hits"]) == 0:
                raise RuntimeError("ES taking too long to index job with id %s."%doc_id)
            else:
                raise RuntimeError("ES taking too long to update status of job with id %s."%doc_id)
        time.sleep(sleep_seconds)
        #result = ES.search(index=es_index, body=query)

        r = requests.post(search_url, data=json.dumps(query))

        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        result = r.json()
        sleep_seconds = sleep_seconds * 2

    logging.info("Job status updated on ES to %s"%str(result["hits"]["hits"][0]["_source"]["status"]))
    return True
    

def main():

    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    resolve_source(context_file)

if __name__ == "__main__":
    main()


