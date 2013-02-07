#!/usr/bin/env python
"""
_DataProcessing_

Standard DataProcessing workflow: a processing task and a merge for all outputs.
"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required
    by the standard DataProcessing workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """
    arguments = {
        "AcquisitionEra": "WMAgentCommissioning10",
        "Requestor": "sfoulkes@fnal.gov",
        "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
        "CMSSWVersion": "CMSSW_3_5_8",
        "ScramArch": "slc5_ia32_gcc434",
        "ProcessingVersion": 2,
        "SkimInput": "output",
        "GlobalTag": "GR10_P_v4::All",

        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "scf_wmagent_configcache",

        "ProcScenario": "cosmics",
        "Multicore" : None,
        "DashboardHost" : "127.0.0.1",
        "DashboardPort" : 8884,
        }

    return arguments

class MeloProcessingWorkloadFactory(StdBase):
    """
    _MeloProcessingWorkloadFactory_

    A workflow to process data. I didn't want to futz with incompatible changes
    to specs people were using, so I just made my own.
    
    For these workflows, we want to stick files in these places
    
    Unmerged-
    gets written to the local site at /store/temp/user/
    gets transferred to the target site at /store/temp/user/
    NOTE: don't munge the LFN, it needs to stay in /store/temp
    
    Merged-
    merged files end up in /store/user
    
    NOTE: UNLESS the target site is FNAL. In that case, there's no
          grid interface, so you want to merge at the individual sites
          and ASO the outputs to FNAL
    
    """
    def __init__(self):
        StdBase.__init__(self)

        return

    def configureLFNs(self, asoMerged):
        """
        _makes some needed LFNs_
        """

        # leave the unmerged files in the per-user space
        # NOTE: this should be the LFN on both sides of the transfer
        self.unmergedLFNBase = '/store/temp/user/%s' % self.userName
        
        # send the merged files to the per-group/user space
        # NOTE, this is munged to /store/temp if ASO is in use
        #      then ASO will drop the temp if it's getting moved
        tempSpot = ""
        if asoMerged:
            tempSpot = "temp/"
        if self.groupStorageName:
            self.mergedLFNBase = '/store/%suser/%s/%s' % \
                            (tempSpot, self.groupStorageName, self.userName)
        else:
            self.mergedLFNBase = '/store/%suser/%s' % \
                            (tempSpot, self.userName)
                            
        # keep the log files in the user's personal temp space        
        self.logBase = '/store/temp/user/%s/logs/' % self.userName
        
        # but send the collected tarballs to the right ASO place
        self.logCollBase = '/store/user/%s/YAAT-logs/' % self.userName
        
    
    def buildWorkload(self, jobType):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.  At the very least
        this will create a processing task and merge tasks for all the outputs
        of the processing task.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.
        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        if not jobType in ['MeloMC', 'MeloProcessing']:
            raise RuntimeError, "Invalid parameter to buildWorkload"

        # make the workload
        workload = self.createWorkload()
        workload.setDashboardActivity(jobType)
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo, self.procJobSplitArgs)
        procTask = workload.newTask(jobType)

        # set ACDC variables
#        if self.ACDCID:
#            procTask.addInputACDC(self.ACDCURL, self.ACDCDBName, self.origRequest, self.ACDCID)
#            self.inputDataset = None
#            workload.setWorkQueueSplitPolicy("ResubmitBlock", self.procJobSplitAlgo, self.procJobSplitArgs)
#        else:
#            workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo, self.procJobSplitArgs)


        # Do we want to transfer the files via ASO before or after they are
        # merged? If the files are going to FNAL, merge them before. Otherwise
        # they end up in a black hole because merge jobs can't land there
        if not self.asyncDest:
            asoMerged   = False
            asoUnmerged = False
            procTaskDest = None
        elif self.asyncDest.upper().find("US_FNAL") != -1:
            # the files will end up at FNAL, transfer only merged products there
            asoMerged    = True
            asoUnmerged  = False
            procTaskDest = None
        else:
            asoMerged    = False
            asoUnmerged  = True
            procTaskDest = self.asyncDest

        self.configureLFNs( asoMerged = asoMerged )
        outputMods = self.setupProcessingTask(procTask, "Analysis", self.inputDataset,
                                              scenarioName = self.procScenario,
                                              couchURL = self.couchURL,
                                              couchDBName = self.couchDBName,
                                              configDoc = self.configCacheID,
                                              splitAlgo = self.procJobSplitAlgo,
                                              splitArgs = self.procJobSplitArgs,
                                              stepType = "CMSSW",
                                              userSandbox = self.userSandbox,
                                              userFiles = self.userFiles,
                                              userDN = self.owner_dn,
                                              asyncDest = procTaskDest,
                                              seeding=self.seeding,
                                              totalEvents=self.totalEvents,
                                              owner_vogroup = self.owner_vogroup,
                                              owner_vorole = self.owner_vorole)

        # set merging policy and LFNs for the main task
        for stepName in procTask.listAllStepNames():
            step = procTask.getStep( stepName )

            
            # TODO make log stuff also be async
            if step.stepType() in ['StageOut']:
                # Disable direct to merge 
                step.data.output.doNotDirectMerge = True
                if asoUnmerged:
                    step.setUserDN(self.owner_dn)
                    step.setAsyncDest(self.asyncDest)
                    step.setUserRoleAndGroup(self.owner_vogroup,
                                             self.owner_vorole)
                    

                    # tells ASO to not munge the filename since this is
                    # intended to remain a temporary file
                    step.setPreserveLFN( True )

        # TODO logCollect should definitely be ASO
        tasksForLogCollect = [procTask]
        
        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            mergeTask = self.addMergeTask(procTask, self.procJobSplitAlgo,
                                          outputModuleName)
            tasksForLogCollect.append(mergeTask)
            for stepName in mergeTask.listAllStepNames():
                step = mergeTask.getStep( stepName )
                if step.stepType() == 'StageOut' and asoMerged:
                    step.setUserDN(self.owner_dn)
                    step.setAsyncDest(self.asyncDest)
                    step.setUserRoleAndGroup(self.owner_vogroup,
                                               self.owner_vorole)
            procMergeTasks[outputModuleName] = mergeTask
        
        tasksNeedingLogCollect = []
        for tasksForLogCollect in tasksNeedingLogCollect:
            logCollectTask = self.addLogCollectTask(taskForLogCollect)
            logCollectStep = logCollectTask.getStep('logCollect1')
            logCollectStep.addOverride('userLogs',  True)
            logCollectStep.setNewStageoutOverride(True)
            logCollectStep.addOverride('seName', "se1.accre.vanderbilt.edu")
            logCollectStep.addOverride('lfnBase', self.logCollBase)
            logCollectStep.addOverride('lfnPrefix', self.lcPrefix)
            logCollectStep.addOverride('dontStage', False)


        # do a final munge of the lfn bases
        workload.setLFNBase( self.mergedLFNBase, self.unmergedLFNBase )
        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a DataProcessing workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        meloJobType = arguments.get('RequestType', "MeloProcessing")

        # File Parameters
        self.userName = arguments.get("Username",None)
        self.groupStorageName = arguments.get("GroupStorageDir", None)
        self.owner_vogroup = arguments.get("VoGroup", '')
        self.owner_vorole = arguments.get("VoRole", '')
        self.asyncDest = arguments.get("AsyncDest", None)
        
        
        self.userSandbox = arguments.get("userSandbox", None)

        self.emulation = arguments.get("Emulation", False)

        # ACDC and job splitting
        self.ACDCURL = arguments.get("ACDCUrl", "")
        self.ACDCDBName = arguments.get("ACDCDBName", "wmagent_acdc")
        self.ACDCID = arguments.get("ACDCDoc", None)
        timePerEvent     = int(arguments.get("TimePerEvent", 60))
        filterEfficiency = float(arguments.get("FilterEfficiency", 1.0))
        totalTime        = int(arguments.get("TotalTime", 9 * 3600))
        reqEvents        = int(arguments.get("RequestNumEvents", 1))
        self.totalEvents = int(int(reqEvents / filterEfficiency))
        self.firstEvent  = int(arguments.get("FirstEvent",1))
        self.firstLumi   = int(arguments.get("FirstLumi",1))

        # Required parameters that must be specified by the Requestor.
        self.inputDataset = arguments["InputDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments["GlobalTag"]

        # Monte Carlo arguments
        self.inputPrimaryDataset = arguments.get("PrimaryDataset", None)
        self.seeding = arguments.get("Seeding", "AutomaticSeeding")
        self.firstEvent = arguments.get("FirstEvent", 1)
        self.firstLumi = arguments.get("FirstLumi", 1)

        # Pileup configuration for the first generation task
        self.pileupConfig = arguments.get("PileupConfig", None)

        # The CouchURL and name of the ConfigCache database must be passed in
        # by the ReqMgr or whatever is creating this workflow.
        self.couchURL = arguments["CouchURL"]
        self.couchDBName = arguments["CouchDBName"]

        # Get the ConfigCacheID
        self.configCacheID = arguments.get("ConfigCacheID", None)

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo  = arguments.get("StdJobSplitAlgo", "LumiBased")
        self.procJobSplitArgs  = arguments.get("StdJobSplitArgs",
                                               {"lumis_per_job": 8,
                                                "include_parents": self.includeParents})
        return self.buildWorkload(meloJobType)

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        requiredFields = ["CMSSWVersion", "GlobalTag",
                          "InputDataset", "ScramArch",
                          "ConfigCacheID","CouchURL"]

        if schema['RequestType'] == 'MeloProcessing':
            requiredFields.extend([])
        elif schema['RequestType'] == 'MeloMC':
            requiredFields.extend(["PrimaryDataset", "RequestNumEvents"])
        else:
            self.raiseValidationException("Invalid RequestType.. how'd we get here?")

        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)

        self.validateConfigCacheExists(configID = schema['ConfigCacheID'],
                                                    couchURL = schema["CouchURL"],
                                                    couchDBName = schema["CouchDBName"],
                                                    getOutputModules = True)

        #if schema.get("ProdJobSplitAlgo", "EventBased") == "EventBased":
        #    self.validateEventBasedParameters(schema = schema)

        return


def meloProcessingWorkload(workloadName, arguments):
    """
    _dataProcessingWorkload_

    Instantiate the DataProcessingWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myDataProcessingFactory = MeloProcessingWorkloadFactory()
    return myDataProcessingFactory(workloadName, arguments)
