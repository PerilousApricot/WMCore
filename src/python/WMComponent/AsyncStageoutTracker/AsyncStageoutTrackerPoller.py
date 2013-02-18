#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
# W6501: It doesn't like string formatting in logging messages
"""
The actual ASO tracker algorithm

This is pretty straightforward. Get a list of jobs we know are pendingaso, get their files,
query the central couchdb for their status. propagate it to the agent. boom boom.

"""
__all__ = []

import time
import os.path
import threading
import logging
import traceback
import collections

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, CouchError

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ACDC.DataCollectionService  import DataCollectionService
from WMCore.WMSpec.WMWorkload           import WMWorkload, WMWorkloadHelper
from WMCore.WMException                 import WMException
from WMCore.FwkJobReport.Report         import Report
from WMCore.Services.PhEDEx.PhEDEx      import PhEDEx
class AsyncStageoutTrackerException(WMException):
    """
    The Exception class for the AsyncStageoutTrackerPoller

    """
    pass


class AsyncStageoutTrackerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        # Things needed to do this
        self.getJobsAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        self.getNameAction = self.daoFactory(classname = "Jobs.LoadFromIDWithWorkflow")

        # initialize the alert framework (if available - config.Alert present)
        #    self.sendAlert will be then be available    
        self.initAlerts(compName = "AsyncStageoutTracker") 
        
        # set up JSM
        self.stateChanger = ChangeState(config)
               
        # store the last timestamp we looked at in DB
        self.lastMonitoringTimestamp = 0
        
        # connect to the user_monitoring DB
        self.asoCouch = CouchServer(self.config.AsyncStageoutTracker.couchurl)
        self.asoMonDB = self.asoCouch.connectDatabase(\
                                        self.config.AsyncStageoutTracker.couchDBName,\
                                        False)

        # how long to wait before failing a job
        self.asoTimeout = 3 * 24 * 60 * 60
        
        # Connect to phedex for sitedb lookups
        self.phedexApi = PhEDEx(responseType='json')
        
        # load up a few caches
        self.jobWorkflowCache = {}
        self.pfn_to_lfn_mapping = {}
        self.jobLfnMapping = {}
        self.jobTimeoutMapping = {}
        return
    
    def setup(self, parameters = None):
        """
        Load DB objects required for queries
        """

        # For now, does nothing

        return

    def terminate(self, params):
        """
        _terminate_
        
        Do one pass, then commit suicide
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)

    def getWorkflowNameFromJobId(self, target):
        if id in self.jobWorkflowCache:
            self.jobWorkflowCache[1] = time.time()
            return self.jobWorkflowCache[target][0]
        else:
            self.jobWorkflowCache[target] = \
                  [self.getNameAction.execute( jobID = target )['workflow'],
                   time.time()]
            return self.jobWorkflowCache[target][0]
    
    def trimWorkflowBaneCache(self):
        cutoffTime = time.time() - 600
        for k in self.jobWorkflowCache:
            if self.jobWorkflowCache[k][1] < cutoffTime:
                del self.jobWorkflowCache[k]
    def algorithm(self, parameters = None):
        """
        Wrap exceptions here .. we want to at least retry later
        """
        try:
            return self.algorithm_real(parameters)
        except CouchError, e:
            logging.exception("We got the exception %s. Ignorning" % e)
        except Exception, e:
            logging.exception("We got an unrecognized exception %s. Bad!" % e)
            
    def algorithm_real(self, parameters = None):
        """
    	Performs the handleErrors method, looking for each type of failure
    	And deal with it as desired.
        """
        logging.debug("Beginning ASOTracker loop")
        filesCache = {}
        currentTime = time.time()
        pendingASOJobs = self.getJobsAction.execute(state = "asopending")
        logging.debug("Have %s jobs to examine" % len(pendingASOJobs))
        logging.debug("We have %s jobs cached" % len(self.jobLfnMapping))

        for job_id in pendingASOJobs:
            workflow = self.getWorkflowNameFromJobId(job_id)
            #logging.debug("Processing %s from %s" % \
            #                    (job_id, workflow) )


            if not job_id in self.jobLfnMapping:
                job = Job( id = job_id )
                job.load()
                jobReport = Report()
                jobReportPath = job['fwjr_path']
                try:
                    jobReportPath = jobReportPath.replace("file://","")
                    jobReport.load( jobReportPath )
                except Exception, ex:
                    # if we got here, we must've used to have had a FWJR, knock it back
                    # to the JobAccountant, they can deal with it
                    logging.debug( "ASOTracker: %s has no FWJR, but it should if we got here" % job['id'])
                    logging.debug( "ASOTracker: Tried to open %s" % jobReportPath )
                    logging.debug( "ASOTracker: Got exception %s" % ex )
                    # FIXME Should find out how to store errors so the outside will see
                    self.stateChanger.propagate(job, "complete", "asopending")
                    continue
                allFiles = jobReport.getAllFileRefs()
                lfnsNeedingASO = []
                for fwjrFile in allFiles:
                    if (getattr(fwjrFile, "preserveLFN", False) == False) and\
                       (getattr(fwjrFile, "preserve_lfn", False) == False):
                        lfn = fwjrFile.lfn.replace('store/temp', 'store', 1)
                    else:
                        lfn = fwjrFile.lfn
                    
                    # if we wanted ASO, ASO is complete and the LFN is there
                    if getattr(fwjrFile, "async_dest", None) and \
                        not getattr(fwjrFile, "asyncStatus", None):
                        lfnsNeedingASO.append(lfn)
                self.jobLfnMapping[job_id] = lfnsNeedingASO
                self.jobTimeoutMapping[job_id] = job['state_time']
                allLfns = self.jobLfnMapping[job_id]
            else:
                allLfns = self.jobLfnMapping[job_id]
            
            if self.jobTimeoutMapping[job_id] + self.asoTimeout < time.time():
                logging.debug("Failing a job for being too old")
                job = Job( id = job_id )
                job.load()
                jobReport = Report()
                jobReportPath = job['fwjr_path']
                jobReportPath = jobReportPath.replace("file://","")
                jobReport.load( jobReportPath )
                msg = "ASO timed out after %s seconds" % self.asoTimeout
                jobReport.addError("ASOTimeout", 60317, "ASOTimeout", msg)
                self.stateChanger.propagate(job, "asofailed", "asopending")
                jobReport.save()
                continue


            # retrieve all the files for this workflow, if it exists
            if not workflow in filesCache:
                logging.debug("Pulling document from couch: %s" % workflow)
                query = { 'startkey' : [workflow],
                          'endkey' : [workflow, {}],
                          'reduce' : False,
                          'stale' : 'update_after'}
                monFiles = self.asoMonDB.loadView('UserMonitoring',\
                                                  'FilesByWorkflow',\
                                                  query)
                oneCache = {}
                logging.debug("Have %s files in the view" % \
                                    len(monFiles['rows']))
                for oneFile in monFiles['rows']:
                    lfn = oneFile['value']['dest_lfn']
                    oneCache[lfn] = \
                                { 'state' : oneFile['value']['state'],
                                  'lfn'   : lfn,
                                  'dest_lfn'   : oneFile['value']['dest_lfn'],
                                  'location' : self.phedexApi.getNodeSE( oneFile['value']['location'] )}
                      
                filesCache[workflow] = oneCache
                asoFiles = oneCache
            else:
                asoFiles = filesCache[workflow]
                       
            # Look through each job state and update it
            filesFailed = False
            asoComplete = False
            for lfn in allLfns:
                if not lfn in asoFiles:
                    continue
                if asoFiles[lfn]['state'] in ['done','failed']:
                    asoComplete = True
                    # TODO: We should clean the user_monitoring when we've seen
                    job = Job( id = job_id )
                    job.load()
                    jobReport = Report()
                    jobReportPath = job['fwjr_path']
                    jobReportPath = jobReportPath.replace("file://","")
                    jobReport.load( jobReportPath )
                    allFiles = jobReport.getAllFileRefs()
                    for fwjrFile in allFiles:
                        if (getattr(fwjrFile, "preserveLFN", False) == False) and\
                           (getattr(fwjrFile, "preserve_lfn", False) == False):
                            newLfn = fwjrFile.lfn.replace('store/temp', 'store', 1)
                        else:
                            newLfn = fwjrFile.lfn
                        if newLfn == lfn:
                            break
                    if newLfn != lfn:
                        # didn't get a match somehow?
                        logging.error("Somehow couldn't match lfn %s to fwjr" % lfn)
                        continue
                 
                    if asoFiles[lfn]['state'] == 'done':
                        fwjrFile.asyncStatus = 'Success'
                        fwjrFile.lfn = asoFiles[lfn]['dest_lfn']
                        fwjrFile.location    = asoFiles[lfn]['location']
                        logging.debug("Saving a file success to %s" % jobReportPath)
                        jobReport.save( jobReportPath )
                    elif asoFiles[lfn]['state'] == 'failed':
                        # TODO: need to propagate diagnostic info here
                        fwjrFile.asyncStatus = 'Failed'
                        jobReport.save( jobReportPath )
                        filesFailed = True
                else:
                    logging.debug('the file had the state %s' % \
                                        asoFiles[lfn]['state'] )
                    asoComplete = False
            
            # Obviously need to change this to query the info from ASO
            #   if a job failed, send it to asofailed instead
            if asoComplete:
                if not filesFailed:
                    self.stateChanger.propagate(job, "complete", "asopending")
                else:
                    logging.debug("Failing a job for being too old")
                    wmbsJob = Job( id = job_id )
                    wmbsJob.load()
                    jobReport = Report()
                    jobReportPath = wmbsJob['fwjr_path']
                    jobReportPath = jobReportPath.replace("file://","")
                    jobReport.load( jobReportPath )
                    msg = "ASO Reported a failure"
                    jobReport.addError("ASOFailure", 60307, "ASOFailure", msg)
                    self.stateChanger.propagate(job, "asofailed", "asopending")
                    jobReport.save(jobReportPath)
        
            # FIXME the above code doesn't change the LFN or check the file state
            # FIXME FIXME
            
        self.lastMonitoringTimestamp = currentTime
        
    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        Stolen from ASO
        """
        site, lfn = tuple(file.split(':'))
        pfn = self.tfc_map[site].matchLFN('srmv2', lfn)

        #TODO: improve fix for wrong tfc on sites
        try:
            if pfn.split(':')[0] != 'srm':
                logging.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
        except IndexError:
            logging.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None
        except AttributeError:
            logging.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None

        # Add the pfn key into pfn-to-lfn mapping
        if not self.pfn_to_lfn_mapping.has_key(pfn):
            self.pfn_to_lfn_mapping[pfn] = lfn

        return pfn
