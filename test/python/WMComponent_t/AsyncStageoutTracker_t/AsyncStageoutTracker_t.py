#!/usr/bin/env python
"""
AsyncTrackerPoller test TestAsyncTrackerPoller module and the harness
"""

import os
import os.path
import threading
import time
import unittest
import logging
import shutil


from WMComponent.AsyncStageoutTracker.AsyncStageoutTrackerPoller import AsyncStageoutTrackerPoller
from WMComponent.JobAccountant.JobAccountantPoller import JobAccountantPoller
import WMCore.WMBase
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.DAOFactory          import DAOFactory
from WMCore.Services.UUID       import makeUUID


from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.File         import File
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Job          import Job
from WMCore.WMBS.JobGroup     import JobGroup
from WMCore.ResourceControl.ResourceControl  import ResourceControl

from WMCore.DataStructs.Run             import Run
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.Agent.Configuration         import Configuration
from WMCore.WMSpec.Makers.TaskMaker     import TaskMaker
from WMCore.ACDC.DataCollectionService  import DataCollectionService
from WMCore.FwkJobReport.Report         import Report
from WMCore.Database.CMSCouch import CouchServer
from WMQuality.TestInitCouchApp import CouchAppTestHarness

from WMCore_t.WMSpec_t.TestSpec         import testWorkload
from WMCore.Storage.TrivialFileCatalog import readTFC

# do some monkeypatching. I'd be more robust with it, but this should be
# the only ASO-touching code
hasASO = True
try:
    import AsyncStageOut, AsyncStageOut_t
    import AsyncStageOut.TransferDaemon
    import AsyncStageOut.AnalyticsDaemon
    import AsyncStageOut.LFNSourceDuplicator
    import AsyncStageOut_t.FakeTransferWorker
    from AsyncStageOut_t.FakeTransferWorker import FakeTransferWorker \
        as FakeTransferWorker
except ImportError:
    hasASO = False

if not hasASO:
    import nose
    class AsyncTrackerLifecycleTest(unittest.TestCase):
        def testASOTracker(self):
            raise nose.SkipTest, "ASO Libraries are missing, can't run test!"
else:
    AsyncStageOut.TransferDaemon.TransferWorker = FakeTransferWorker
    
    class AsyncTrackerLifecycleTest(unittest.TestCase):
        """
        Test round-tripping data from a FJR in WMStats, to ASO and back
        
        An example from the inputAsyncStageout db on WMStats:
        {workflow: "meloam_MeloMCGenTestv6_120719_204741_1342", 
        jobid: "a9a04888-d239-11e1-b31b-001d7d020436-0", 
        task: "/meloam_MeloMCGenTestv6_120719_204741_1342/Production/ProductionMergeAODSIMoutput", 
        _id: "/store/user/meloam/Backfill/melooutput1/AODSIM/IntegrationTest_120719/00000/741028C8-39D2-E111-B356-00A0D1E952B8.root", 
        checksums: {adler32: "5759a594", cksum: "468777326"}, 
        size: 2432988, 
        source: "T2_US_Vanderbilt", 
        job_end_time: 1342768166}
        
        whigh is generated from here:
        {
           "_id": "a9a04888-d239-11e1-b31b-001d7d020436-0",
           "_rev": "1-041a7906195d6b55249b6397e1349a5e",
           "workflow": "meloam_MeloMCGenTestv6_120719_204741_1342",
           "timestamp": 1342768166,
           "site": "T2_US_Vanderbilt",
           "lumis": [
               {
                   "1": [
                       1,
                       2
                   ]
               }
           ],
           "errors": {
               "logArch1": [
               ],
               "cmsRun1": [
               ],
               "stageOut1": [
               ]
           },
           "outputdataset": {
               "applicationName": "cmsRun",
               "applicationVersion": "CMSSW_5_2_6",
               "processedDataset": "Backfill-IntegrationTest_120719",
               "dataTier": "AODSIM",
               "primaryDataset": "melooutput1"
           },
           "retrycount": 0,
           "task": "/meloam_MeloMCGenTestv6_120719_204741_1342/Production/ProductionMergeAODSIMoutput",
           "state": "success",
           "output": [
               {
                   "lfn": "/store/user/meloam/Backfill/melooutput1/AODSIM/IntegrationTest_120719/00000/741028C8-39D2-E111-B356-00A0D1E952B8.root",
                   "location": "T2_US_Vanderbilt",
                   "checksums": {
                       "adler32": "5759a594",
                       "cksum": "468777326"
                   },
                   "type": "output",
                   "size": 2432988
               },
               {
                   "lfn": "/store/temp/meloam/logs/prod/2012/7/20/meloam_MeloMCGenTestv6_120719_204741_1342/Production/ProductionMergeAODSIMoutput/0000/0/a9a04888-d239-11e1-b31b-001d7d020436-0-0-logArchive.tar.gz",
                   "location": "T2_US_Vanderbilt",
                   "checksums": {
                       "adler32": "edab9170",
                       "cksum": "4056743812",
                       "md5": "318861899292dde3d4a269ad3fb0af5a"
                   },
                   "type": "logArchive",
                   "size": 0
               }
           ],
           "type": "jobsummary",
           "exitcode": 0
        }
        
        And the workflow document looks like this
        {
           "_id": "meloam_MeloMCGenTestv6_120719_204741_1342",
           "inputdataset": "",
           "vo_group": "",
           "group": "testing",
           "campaign": "",
           "workflow": "meloam_MeloMCGenTestv6_120719_204741_1342",
           "user_dn": "None",
           "vo_role": "",
           "priority": 100,
           "requestor": "meloam",
           "request_type": "LHEStepZero",
           "publish_dbs_url": "",
           "dbs_url": null,
           "async_dest": "",
           "type": "reqmgr_request",
           "request_status": [
               {
                   "status": "new",
                   "update_time": 1342748862
               },
               {
                   "status": "running",
                   "update_time": 1342749497
               },
               {
                   "status": "completed",
                   "update_time": 1342797628
               }
           ],
        }
        
        """
        
        def xxtestPassWorkflow(self):
            pass
        
        def setUp(self):
            """
            setup for test.
            """
            myThread = threading.currentThread()
            
            self.testInit = TestInitCouchApp(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection(destroyAllDatabase = True)
            self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                     "WMCore.ResourceControl"],
                                    useDefault = False)
            self.testDir = self.testInit.generateWorkDir()
            self.testInit.setupCouch("asynctrackerpoller_t_wmstats", "WMStats")
            self.testInit.setupCouch("jobtracker_t/jobs", "JobDump")
            self.testInit.setupCouch("jobtracker_t/fwjrs", "FWJRDump")
            couchapps = os.path.join(os.path.dirname(AsyncStageOut.__file__), "../../couchapp")
            self.couchServer = CouchServer(dburl = os.getenv("COUCHURL"))
    
            self.async_couchapp = "%s/AsyncTransfer" % couchapps
            self.monitor_couchapp = "%s/monitor" % couchapps
            self.user_monitoring_couchapp = "%s/UserMonitoring" % couchapps
            self.stat_couchapp = "%s/stat" % couchapps
            self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                         logger = myThread.logger,
                                         dbinterface = myThread.dbi)
            harness = CouchAppTestHarness("asynctransfer2_t")
            harness.create()
            harness.pushCouchapps(self.async_couchapp)
            harness.pushCouchapps(self.monitor_couchapp)
            harness_user_mon = CouchAppTestHarness("user_monitoring_asynctransfer_t")
            harness_user_mon.create()
            harness_user_mon.pushCouchapps(self.user_monitoring_couchapp)
            harness_stat = CouchAppTestHarness("asynctransfer_stat_t")
            harness_stat.create()
            harness_stat.pushCouchapps(self.stat_couchapp)
            
            self.harness = harness
            self.harness_user_mon = harness_user_mon
            self.harness_stat = harness_stat
            
        def tearDown(self):
            """
            Database deletion
            """
            self.harness.drop()
            self.harness_user_mon.drop()
            self.harness_stat.drop()
            self.testInit.tearDownCouch()
            
            self.testInit.clearDatabase()
            self.testInit.delWorkDir()
    
            
        
        def _mock_get_tfc_rules(self, site):
            return readTFC(os.path.join(os.path.dirname(AsyncStageOut_t.__file__),\
                                         'fake-tfc.xml'))
        
        def testASOSuccess(self):
            AsyncStageOut_t.FakeTransferWorker.setFailProbability(0)
            testJob = self.roundtripHelper(preserveLFN = False)
            stepReport = Report('cmsRun1')
            stepReport.unpersist(testJob['fwjr_path'])
            files = stepReport.getAllFileRefsFromStep(step = 'cmsRun1')
            for file in files:
                self.assertEqual( file.lfn.find('store/temp'),
                                     -1,
                                     "The lfn should not have store/temp: %s" % file.lfn)
        
        def testASOFail(self):
            AsyncStageOut_t.FakeTransferWorker.setFailProbability(1)
            self.roundtripHelper(wantsASOWin=False)
            
        def testASONoNameChange(self):
            AsyncStageOut_t.FakeTransferWorker.setFailProbability(0)
            testJob = self.roundtripHelper(preserveLFN = True)
            stepReport = Report('cmsRun1')
            stepReport.unpersist(testJob['fwjr_path'])
            files = stepReport.getAllFileRefsFromStep(step = 'cmsRun1')
            for file in files:
                self.assertNotEqual( file.lfn.find('store/temp'),
                                     -1,
                                     "The lfn should still have store/temp: %s" % file.lfn)
                
        def roundtripHelper(self, wantsASO = True, wantsASOWin = True,
                                  preserveLFN = False):
            wmstatDB = self.couchServer.connectDatabase('asynctrackerpoller_t_wmstats')
            
            # first, add some documents telling ASO we have files ready to MOVE
            wmstatDB.queue( self.getWorkloadDoc( preserveLFN = preserveLFN ) ) 
            wmstatDB.queue( self.getJobDoc() )
            wmstatDB.commit()
            
            # make some test jobs
            testJob = self.createTestJob()        
            self.setJobWantsASO(os.path.join(self.testDir, "oneaso.pkl"),
                                    preserveLFN = preserveLFN)
            self.assertEqual( testJob.getState().lower(), 'asopending')
            
            # now, make our ASOTrackerPoller and make sure that things are
            # still in pending
            asoPoller = AsyncStageoutTrackerPoller( self.getWMAgentConfig() )
            self.assertEqual( testJob.getState().lower(), 'asopending')
            asoPoller.algorithm()
            self.assertEqual( testJob.getState().lower(), 'asopending')
    
            
            
            # okay, we have the right stuff in WMStats for ASO to pick it up.
            # let's run the algorithm...
            config = self.getASOConfig()
            
            # pull data from WMStats to ASO's database
            print "Pushing lfnDaemon"
            lfnDaemon = AsyncStageOut.LFNSourceDuplicator.LFNSourceDuplicator(config)
            lfnDaemon.algorithm()
            
            # tell ASO to "transfer" a file
            print "Done.. pushing transferDaemon"
            # trigger ASO's transfer logic (which we've noop'd)
            transferDaemon = AsyncStageOut.TransferDaemon.TransferDaemon(config)
            transferDaemon.get_tfc_rules = self._mock_get_tfc_rules
            print "Calling transferDaemon.algorithm()"
            transferDaemon.algorithm()
            transferDaemon.algorithm()
            transferDaemon.algorithm()
            print "Done...calling monitorDaemon"
            
            # ASO has allegedly transferred things, check the status of monitoring
            monitorDaemon = AsyncStageOut.AnalyticsDaemon.AnalyticsDaemon(config)
            monitorDaemon.algorithm()
            monitorDaemon.algorithm()
            
            # Now we have a user_monitoring document we can look at
            # see if the poller finds it
            print "Done...calling ASOTrackerPoller"
            # needs to call it twice since end_key isn't inclusive
            asoPoller.algorithm()
            asoPoller.algorithm()
            if wantsASOWin:
                self.assertEqual( testJob.getState().lower(), 'complete')
            else:
                self.assertEqual( testJob.getState().lower(), 'asofailed')
                
            # now that the file has been transferred, make sure thac the accountant
            # sends it to jobsuccess
            jobAccountant = JobAccountantPoller(self.getWMAgentConfig())
            jobAccountant.setup()
            jobAccountant.algorithm()
            if wantsASOWin:
                self.assertEqual( testJob.getState().lower(), 'success')
            else:
                self.assertEqual( testJob.getState().lower(), 'asofailed')
                
            return testJob
    
            
        def setJobWantsASO(self, filename, preserveLFN = True):
            stepReport = Report('cmsRun1')
            stepReport.unpersist(filename)
            files = stepReport.getAllFileRefsFromStep(step = 'cmsRun1')
            for file in files:
    
                if not hasattr(file, 'lfn') or not hasattr(file, 'location') or \
                       not hasattr(file, 'guid'):
                    continue
    
                file.user_dn = "/CN=dummy-name/O=melopartydotcom"
                file.async_dest = "T2_US_Vanderbilt"
                file.user_vogroup = ''
                file.user_vorole = ''
                file.preserve_lfn = preserveLFN
                
            stepReport.persist(filename)
                    
            
        def createTestJob(self):
            """
            _createTestJobs_
    
            Create several jobs
            """
            #Create sites in resourceControl
            resourceControl = ResourceControl()
            resourceControl.insertSite(siteName = 'malpaquet', seName = 'se.malpaquet',
                                       ceName = 'malpaquet', plugin = "CondorPlugin")
            resourceControl.insertThreshold(siteName = 'malpaquet', taskType = 'Processing', \
                                            maxSlots = 10000, pendingSlots = 10000)
    
            locationAction = self.daoFactory(classname = "Locations.New")
            locationAction.execute(siteName = "malpaquet", seName = "malpaquet",
                                   ceName = "malpaquet", plugin = "CondorPlugin")
            jobAction = self.daoFactory(classname = "Jobs.New")
            jobAction.execute()
            # Create user
            newuser = self.daoFactory(classname = "Users.New")
            newuser.execute(dn = "jchurchill")
    
            testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                    name = "meloam_ASYNCTEST1_120810_170823_8981", task="Test")
            testWorkflow.create()
            
            testWMBSFileset = Fileset(name = "TestFileset")
            testWMBSFileset.create()
            
            testSubscription = Subscription(fileset = testWMBSFileset,
                                            workflow = testWorkflow,
                                            type = "Processing",
                                            split_algo = "FileBased")
            testSubscription.create()
    
            testJobGroup = JobGroup(subscription = testSubscription)
            testJobGroup.create()
    
            # Create a file
            testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 10)
            testFileA.addRun(Run(10, *[12312]))
            testFileA.setLocation('malpaquet')
            testFileA.create()
    
            baseName = makeUUID()
            
            # make a copy of the FWJR since it will be modified
            shutil.copyfile(os.path.join(WMCore.WMBase.getTestBase(),
                                                'WMComponent_t',
                                                'AsyncStageoutTracker_t',
                                                'oneaso.pkl'),
                            os.path.join(self.testDir, "oneaso.pkl") )
            
    
            # Now create a job
    
            testJob = Job(name = '%s-%i' % (baseName, 1))
            testJob.addFile(testFileA)
            testJob['location'] = 'malpaquet'
            testJob['retry_count'] = 1
            testJob['retry_max'] = 10
            testJob['fwjr_path'] = os.path.join(self.testDir, "oneaso.pkl")
            testJob.create(testJobGroup)
            testJob.save()
            testJobGroup.add(testJob)
    
            testJobGroup.commit()
            
            stateAction = self.daoFactory(classname = "Jobs.ChangeState")
            stateAction.execute( [{'id' : testJob['id'],
                                    'state' : 'asopending',
                                    'couch_record' : testJob['couch_record']}])
            return testJob
    
    
            return testJobGroup       
        def getWMAgentConfig(self):
            """
            _getWMAgentConfig_
    
            """
            config = Configuration()
    
            # First the general stuff
            config.section_("General")
            config.General.workDir = os.getenv("TESTDIR", self.testDir)
            config.section_("CoreDatabase")
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            config.CoreDatabase.socket     = os.getenv("DBSOCK")
    
            config.component_("AsyncStageoutTracker")
            # The log level of the component. 
            config.AsyncStageoutTracker.logLevel = 'DEBUG'
            # The namespace of the component
            config.AsyncStageoutTracker.namespace = 'WMComponent.AsyncStageoutTracker.AsyncStageoutTracker'
            # maximum number of threads we want to deal
            # with messages per pool.
            config.AsyncStageoutTracker.maxThreads = 1
            # maximum number of retries we want for job
            config.AsyncStageoutTracker.maxRetries = 5
            # The poll interval at which to look for failed jobs
            config.AsyncStageoutTracker.pollInterval = 60
            config.AsyncStageoutTracker.couchurl     = "http://127.0.0.1:5984"
            config.AsyncStageoutTracker.couchDBName  = "user_monitoring_asynctransfer_t" 
    
            # JobStateMachine
            config.component_('JobStateMachine')
            config.JobStateMachine.couchurl        = os.getenv('COUCHURL', None)
            config.JobStateMachine.couchDBName     = "asynctrackerpoller_t_jd"
            config.JobStateMachine.jobSummaryDBName= 'asynctrackerpoller_t_wmstats'
    
            config.component_('JobAccountant')
            
            config.section_('ACDC')
            config.ACDC.couchurl = self.testInit.couchUrl
            config.ACDC.database = "asynctrackerpoller_t"
    
            return config  
        
        def getASOConfig(self):
            """
            _getWMAgentConfig_
    
            """
            config = Configuration()
    
            # First the general stuff
            config.section_("General")
            config.General.workDir = os.getenv("TESTDIR", self.testDir)
            config.section_("CoreDatabase")
            config.CoreDatabase.connectUrl = os.getenv("DATABASE")
            config.CoreDatabase.socket     = os.getenv("DBSOCK")
    
            config.section_("Agent")
            config.Agent.contact = "testingmail@fnal.gov"
            config.Agent.agentName = "testingagent"
            config.Agent.hostName = "localhost"
            config.Agent.teamName = "testingteam"
            
            config.component_("AsyncTransfer")
            config.AsyncTransfer.log_level = logging.DEBUG
            config.AsyncTransfer.namespace = "AsyncStageOut.AsyncTransfer"
            config.AsyncTransfer.componentDir  = config.General.workDir
            config.AsyncTransfer.pollInterval = 10
            config.AsyncTransfer.pollViewsInterval = 10
            config.AsyncTransfer.couch_instance = os.getenv('COUCHURL')
            config.AsyncTransfer.files_database = 'asynctransfer2_t'
            config.AsyncTransfer.statitics_database = 'asynctransfer_stat_t'
            config.AsyncTransfer.requests_database = 'aso_request_database_t'
            config.AsyncTransfer.data_source = os.getenv('COUCHURL')
            config.AsyncTransfer.db_source = 'asynctrackerpoller_t_wmstats'
            config.AsyncTransfer.pluginName = "CentralMonitoring"
            config.AsyncTransfer.pluginDir = "AsyncStageOut.Plugins"
            config.AsyncTransfer.max_files_per_transfer = 1000
            config.AsyncTransfer.pool_size = 1
            config.AsyncTransfer.max_retry = 0
    #        config.AsyncTransfer.credentialDir = credentialDir
    #        config.AsyncTransfer.UISetupScript = ui_script
    #        config.AsyncTransfer.transfer_script = 'ftscp'
    #        config.AsyncTransfer.serverDN = hostDN
    #        config.AsyncTransfer.pollStatInterval = 86400
    #        config.AsyncTransfer.expiration_days = 30
    #        config.AsyncTransfer.couch_statinstance = statCouchUrl
    #        config.AsyncTransfer.serviceCert = serviceCert
    #        config.AsyncTransfer.serviceKey = "/path/to/valid/host-key"
    #        config.AsyncTransfer.cleanEnvironment = True
            config.AsyncTransfer.user_monitoring_db = 'user_monitoring_asynctransfer_t'
            config.AsyncTransfer.couch_user_monitoring_instance = os.getenv('COUCHURL')
            config.AsyncTransfer.analyticsPollingInterval = 60
    #        config.AsyncTransfer.filesCleaningPollingInterval = 14400
            config.AsyncTransfer.summaries_expiration_days = 30
            return config  
        def getWorkloadDoc(self, preserveLFN = False):
            retval = {
                       "_id": "meloam_ASYNCTEST1_120810_170823_8981",
                       "inputdataset": "",
                       "vo_group": "",
                       "group": "testing",
                       "campaign": "",
                       "workflow": "meloam_ASYNCTEST1_120810_170823_8981",
                       "user_dn": "None",
                       "vo_role": "",
                       "priority": 100,
                       "requestor": "meloam",
                       "request_type": "LHEStepZero",
                       "publish_dbs_url": "",
                       "dbs_url": None,
                       "async_dest": "T1_US_FNAL_Buffer",
                       "type": "reqmgr_request",
                       "request_status": [
                           {
                               "status": "new",
                               "update_time": 1342748862
                           },
                           {
                               "status": "running",
                               "update_time": 1342749497
                           },
                           {
                               "status": "completed",
                               "update_time": 1342797628
                           }
                       ],
                    }
            retval['preserve_lfn'] = preserveLFN
            return retval
            
        def getJobDoc(self):
            return {
                       "_id": "a9a04888-d239-11e1-b31b-001d7d020436-0",
                       "workflow": "meloam_ASYNCTEST1_120810_170823_8981",
                       "timestamp": 1342768166,
                       "site": "T2_US_Vanderbilt",
                       "lumis": [
                           {
                               "1": [
                                   1,
                                   2
                               ]
                           }
                       ],
                       "errors": {
                           "logArch1": [
                           ],
                           "cmsRun1": [
                           ],
                           "stageOut1": [
                           ]
                       },
                       "outputdataset": {
                           "applicationName": "cmsRun",
                           "applicationVersion": "CMSSW_5_2_6",
                           "processedDataset": "Backfill-IntegrationTest_120719",
                           "dataTier": "AODSIM",
                           "primaryDataset": "melooutput1"
                       },
                       "retrycount": 0,
                       "task": "/meloam_MeloMCGenTestv6_120719_204741_1342/Production",
                       "state": "success",
                       "output": [
                           {
                               "lfn": "/store/temp/user/meloam/T2stop_600_50_0_5test/MeloAcquisitionEra/IntegrationTest_120810/00000/B4749FB5-38E3-E111-ADC3-782BCB4FBD6F.root",
                               "location": "T2_US_Vanderbilt",
                               "checksums": {
                                   "adler32": "5759a594",
                                   "cksum": "468777326"
                               },
                               "type": "output",
                               "size": 2432988
                           },
                           {
                               "lfn": "/store/user/meloam/logs/prod/2012/7/20/meloam_MeloMCGenTestv6_120719_204741_1342/Production/ProductionMergeAODSIMoutput/0000/0/a9a04888-d239-11e1-b31b-001d7d020436-0-0-logArchive.tar.gz",
                               "location": "T2_US_Vanderbilt",
                               "checksums": {
                                   "adler32": "edab9170",
                                   "cksum": "4056743812",
                                   "md5": "318861899292dde3d4a269ad3fb0af5a"
                               },
                               "type": "logArchive",
                               "size": 0
                           }
                       ],
                       "type": "jobsummary",
                       "exitcode": 0
                    }
    
if __name__ == '__main__':
    unittest.main()
    
