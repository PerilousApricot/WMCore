import logging
### use request manager funtion directly
### TODO: remove this when GlobalMonitor spins out as a separate application
try:
    from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
          import getGlobalQueues
except:
    logging.warning("Not part of ReqMgr")
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.WMBS.WMBS import WMBS
from WMCore.Lexicon import splitCouchServiceURL

def getSiteOverview(serviceURL, serviceLevel):

    if serviceLevel == "RequestManager":
        return getSiteInfoFromReqMgr(serviceURL)
    elif serviceLevel == "GlobalQueue":
        return getSiteInfoFromGlobalQueue(serviceURL)
    elif serviceLevel == "LocalQueue":
        return getSiteInfoFromLocalQueue(serviceURL)
    else:
        raise

def getSiteInfoFromReqMgr(serviceURL):
    """ get agent info from request mgr """

    reqMgr = RequestManager({'endpoint':serviceURL})
    #get information from global queue.
    try:
        if serviceURL.lower() == "local":
            queues = getGlobalQueues()
        ###TODO: add back when GlobalMonitor spins out as a separate application
        else:
            queues = reqMgr.getWorkQueue()
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
        errorInfo = {}
        errorInfo['site_name'] = serviceURL
        return [errorInfo]

    siteInfo = []
    for queueURL in queues:
        _combineSites(siteInfo, getSiteInfoFromGlobalQueue(queueURL))
    return siteInfo

def getSiteInfoFromGlobalQueue(serviceURL):

    url, dbName = splitCouchServiceURL(serviceURL)
    globalQ = WorkQueue(url, dbName)
    try:
        queues = globalQ.getChildQueues()
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
        errorInfo = {}
        errorInfo['site_name'] = serviceURL
        return [errorInfo]

    siteInfo = []
    for queueURL in queues:
        _combineSites(siteInfo, getSiteInfoFromLocalQueue(queueURL))
    return siteInfo

def getSiteInfoFromLocalQueue(serviceURL):
    """ get agent status from local agent """

    url, dbName = splitCouchServiceURL(serviceURL)
    wqService = WorkQueue(url, dbName)
    try:
        wmbsUrls = wqService.getWMBSUrl()
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
        errorInfo = {}
        errorInfo['site_name'] = serviceURL
        return [errorInfo]

    siteInfo = []
    for url in wmbsUrls:
        _combineSites(siteInfo, getSiteInfoFromWMBSService(url))
    return siteInfo

def getSiteInfoFromWMBSService(serviceURL):
    wmbsSvc = WMBS({'endpoint': serviceURL})
    try:
        batchJobs = wmbsSvc.getBatchJobStatusBySite()
        completeJobs = wmbsSvc.getSiteSummaryFromCouchDB()
        _combineSites(completeJobs, batchJobs)
        return completeJobs
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
        return []

    return batchJobs

def _combineSites(results, batchJobs):
    """ get site information from each agent """
    for batchJob in batchJobs:
        newSite = True
        for item in results:
            if item['site_name'] == batchJob['site_name']:
                newSite = False
                for status in ['Pending', 'Running', 'Complete','Error',
                               "success", "failure", "cooloff"]:
                    item.setdefault(status, 0)
                    batchJob.setdefault(status, 0)
                    item[status] += batchJob[status]
                item.setdefault('pending_slots', 0)
                batchJob.setdefault('pending_slots', 0)
                item['pending_slots'] += batchJob['pending_slots']
        if newSite:
            results.append(batchJob)
