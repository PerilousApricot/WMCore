#/usr/bin/env python
"""
_ChangeState_t_

"""

import unittest
import sys
import os
import logging
import threading
import time
import urllib
import types
import pprint

from WMQuality.TestInitCouchApp import TestInitCouchApp

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer

from WMCore.JobStateMachine.ChangeState import ChangeState, Transitions

from WMCore.WMBS.Job import Job
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup

from WMCore.DataStructs.Run import Run

from WMCore.FwkJobReport.Report import Report
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.WMBase import getTestBase
from WMCore.Configuration import Configuration

class TestChangeState(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        self.transitions = Transitions()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.generateWorkDir()
        self.testInit.setupCouch("changestate_t/jobs", "JobDump")
        self.testInit.setupCouch("changestate_t/fwjrs", "FWJRDump")
        self.testInit.setupCouch("job_summary_t", "WMStats")

        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        self.couchServer = CouchServer(dburl = os.getenv("COUCHURL"))
        self.config = Configuration()
        self.config.component_("JobStateMachine")
        self.config.JobStateMachine.couchurl = os.getenv("COUCHURL")
        self.config.JobStateMachine.jobSummaryDBName = "job_summary_t"
        return

    def tearDown(self):
        """
        _tearDown_

        Cleanup the databases.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        return

    def atestCheck(self):
        """
        This is the test class for function Check from module ChangeState
        """
        change = ChangeState(self.config, "changestate_t")

        # Run through all good state transitions and assert that they work
        for state in self.transitions.keys():
            for dest in self.transitions[state]:
                change.check(dest, state)
        dummystates = ['dummy1', 'dummy2', 'dummy3', 'dummy4']

        # Then run through some bad state transistions and assertRaises(AssertionError)
        for state in self.transitions.keys():
            for dest in dummystates:
                self.assertRaises(AssertionError, change.check, dest, state)
        return

    def atestRecordInCouch(self):
        """
        _testRecordInCouch_

        Verify that jobs, state transitions and fwjrs are recorded correctly.
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()

        testFileA = File(lfn = "SomeLFNA", events = 1024, size = 2048,
                         locations = set(["somese.cern.ch"]))
        testFileB = File(lfn = "SomeLFNB", events = 1025, size = 2049,
                         locations = set(["somese.cern.ch"]))
        testFileA.create()
        testFileB.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 2, \
               "Error: Splitting should have created two jobs."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Merge"
        testJobB = jobGroup.jobs[1]
        testJobB["user"] = "sfoulkes"
        testJobB["group"] = "DMWM"
        testJobB["taskType"] = "Processing"

        change.propagate([testJobA, testJobB], "new", "none")
        change.propagate([testJobA, testJobB], "created", "new")
        change.propagate([testJobA, testJobB], "executing", "created")

        testJobADoc = change.jobsdatabase.document(testJobA["couch_record"])

        for transition in testJobADoc["states"].itervalues():
            self.assertTrue(type(transition["timestamp"]) in (types.IntType,
                                                             types.LongType))

        assert testJobADoc["jobid"] == testJobA["id"], \
               "Error: ID parameter is incorrect."
        assert testJobADoc["name"] == testJobA["name"], \
               "Error: Name parameter is incorrect."
        assert testJobADoc["jobgroup"] == testJobA["jobgroup"], \
               "Error: Jobgroup parameter is incorrect."
        assert testJobADoc["workflow"] == testJobA["workflow"], \
               "Error: Workflow parameter is incorrect."
        assert testJobADoc["task"] == testJobA["task"], \
               "Error: Task parameter is incorrect."
        assert testJobADoc["owner"] == testJobA["owner"], \
               "Error: Owner parameter is incorrect."

        assert testJobADoc["mask"]["FirstEvent"] == testJobA["mask"]["FirstEvent"], \
               "Error: First event in mask is incorrect."
        assert testJobADoc["mask"]["LastEvent"] == testJobA["mask"]["LastEvent"], \
               "Error: Last event in mask is incorrect."
        assert testJobADoc["mask"]["FirstLumi"] == testJobA["mask"]["FirstLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobADoc["mask"]["LastLumi"] == testJobA["mask"]["LastLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobADoc["mask"]["FirstRun"] == testJobA["mask"]["FirstRun"], \
               "Error: First run in mask is incorrect."
        assert testJobADoc["mask"]["LastEvent"] == testJobA["mask"]["LastRun"], \
               "Error: First event in mask is incorrect."

        assert len(testJobADoc["inputfiles"]) == 1, \
               "Error: Input files parameter is incorrect."

        testJobBDoc = change.jobsdatabase.document(testJobB["couch_record"])

        assert testJobBDoc["jobid"] == testJobB["id"], \
               "Error: ID parameter is incorrect."
        assert testJobBDoc["name"] == testJobB["name"], \
               "Error: Name parameter is incorrect."
        assert testJobBDoc["jobgroup"] == testJobB["jobgroup"], \
               "Error: Jobgroup parameter is incorrect."

        assert testJobBDoc["mask"]["FirstEvent"] == testJobB["mask"]["FirstEvent"], \
               "Error: First event in mask is incorrect."
        assert testJobBDoc["mask"]["LastEvent"] == testJobB["mask"]["LastEvent"], \
               "Error: Last event in mask is incorrect."
        assert testJobBDoc["mask"]["FirstLumi"] == testJobB["mask"]["FirstLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobBDoc["mask"]["LastLumi"] == testJobB["mask"]["LastLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobBDoc["mask"]["FirstRun"] == testJobB["mask"]["FirstRun"], \
               "Error: First run in mask is incorrect."
        assert testJobBDoc["mask"]["LastEvent"] == testJobB["mask"]["LastRun"], \
               "Error: First event in mask is incorrect."

        assert len(testJobBDoc["inputfiles"]) == 1, \
               "Error: Input files parameter is incorrect."

        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t/jobs")
        allDocs = changeStateDB.document("_all_docs")

        self.assertEqual(len(allDocs["rows"]), 3,
                         "Error: Wrong number of documents.")

        couchJobDoc = changeStateDB.document("1")

        assert couchJobDoc["name"] == testJobA["name"], \
               "Error: Name is wrong"
        assert len(couchJobDoc["inputfiles"]) == 1, \
               "Error: Wrong number of input files."

        result = changeStateDB.loadView("JobDump", "jobsByWorkflowName")

        self.assertEqual(len(result["rows"]), 2,
                         "Error: Wrong number of rows.")
        for row in result["rows"]:
            couchJobDoc = changeStateDB.document(row["value"]["id"])
            self.assertEqual(couchJobDoc["_rev"], row["value"]["rev"],
                             "Error: Rev is wrong.")
        change.propagate([testJobA, testJobB], "complete", "executing")
        change.propagate([testJobA, testJobB], "asopending", "complete")
        change.propagate([testJobA, testJobB], "complete", "asopending")
           
        return

    def atestUpdateFailedDoc(self):
        """
        _testUpdateFailedDoc_

        Verify that the update function will work correctly and not throw a 500
        error if the doc didn't make it into the database for some reason.
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()

        testFileA = File(lfn = "SomeLFNA", events = 1024, size = 2048,
                         locations = set(["somese.cern.ch"]))
        testFileA.create()
        testFileset.addFile(testFileA)
        testFileset.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Merge"
        testJobA["couch_record"] = str(testJobA["id"])

        change.propagate([testJobA], "new", "none")
        testJobADoc = change.jobsdatabase.document(testJobA["couch_record"])

        self.assertTrue(testJobADoc.has_key("states"))
        self.assertTrue(testJobADoc["states"].has_key("1"))
        return

    def atestPersist(self):
        """
        _testPersist_

        This is the test class for function Propagate from module ChangeState
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        for i in range(4):
            newFile = File(lfn = "File%s" % i, locations = set(["somese.cern.ch"]))
            newFile.create()
            testFileset.addFile(newFile)

        testFileset.commit()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 4, \
               "Error: Splitting should have created four jobs."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Processing"
        testJobB = jobGroup.jobs[1]
        testJobB["user"] = "sfoulkes"
        testJobB["group"] = "DMWM"
        testJobB["taskType"] = "Processing"
        testJobC = jobGroup.jobs[2]
        testJobC["user"] = "sfoulkes"
        testJobC["group"] = "DMWM"
        testJobC["taskType"] = "Processing"
        testJobD = jobGroup.jobs[3]
        testJobD["user"] = "sfoulkes"
        testJobD["group"] = "DMWM"
        testJobD["taskType"] = "Processing"

        change.persist([testJobA, testJobB], "created", "new")
        change.persist([testJobC, testJobD], "new", "none")

        stateDAO = self.daoFactory(classname = "Jobs.GetState")

        jobAState = stateDAO.execute(id = testJobA["id"])
        jobBState = stateDAO.execute(id = testJobB["id"])
        jobCState = stateDAO.execute(id = testJobC["id"])
        jobDState = stateDAO.execute(id = testJobD["id"])

        assert jobAState == "created" and jobBState =="created" and \
               jobCState == "new" and jobDState == "new", \
               "Error: Jobs didn't change state correctly."

        return

    def atestRetryCount(self):
        """
        _testRetryCount_

        Verify that the retry count is incremented when we move out of the
        submitcooloff or jobcooloff state.
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        for i in range(4):
            newFile = File(lfn = "File%s" % i, locations = set(["somese.cern.ch"]))
            newFile.create()
            testFileset.addFile(newFile)

        testFileset.commit()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 4, \
               "Error: Splitting should have created four jobs."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Processing"
        testJobB = jobGroup.jobs[1]
        testJobB["user"] = "sfoulkes"
        testJobB["group"] = "DMWM"
        testJobB["taskType"] = "Processing"
        testJobC = jobGroup.jobs[2]
        testJobC["user"] = "sfoulkes"
        testJobC["group"] = "DMWM"
        testJobC["taskType"] = "Processing"
        testJobD = jobGroup.jobs[3]
        testJobD["user"] = "sfoulkes"
        testJobD["group"] = "DMWM"
        testJobD["taskType"] = "Processing"

        change.persist([testJobA], "created", "submitcooloff")
        change.persist([testJobB], "created", "jobcooloff")
        change.persist([testJobC, testJobD], "new", "none")

        testJobA.load()
        testJobB.load()
        testJobC.load()
        testJobD.load()

        assert testJobA["retry_count"] == 1, \
               "Error: Retry count is wrong."
        assert testJobB["retry_count"] == 1, \
               "Error: Retry count is wrong."
        assert testJobC["retry_count"] == 0, \
               "Error: Retry count is wrong."
        assert testJobD["retry_count"] == 0, \
               "Error: Retry count is wrong."

        return

    def atestJobSerialization(self):
        """
        _testJobSerialization_

        Verify that serialization of a job works when adding a FWJR.
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testFile = File(lfn = "SomeLFNC", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 1, \
               "Error: Splitting should have created one job."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Processing"

        change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        reportPath = os.path.join(getTestBase(),
                                  "WMCore_t/JobStateMachine_t/Report.pkl")
        myReport.unpersist(reportPath)
        testJobA["fwjr"] = myReport

        change.propagate([testJobA], 'executing', 'created')

        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t/fwjrs")
        allDocs = changeStateDB.document("_all_docs")

        self.assertEqual(len(allDocs["rows"]), 2,
                         "Error: Wrong number of documents")

        result = changeStateDB.loadView("FWJRDump", "fwjrsByWorkflowName")
        self.assertEqual(len(result["rows"]), 1,
                         "Error: Wrong number of rows.")
        for row in result["rows"]:
            couchJobDoc = changeStateDB.document(row["value"]["id"])
            self.assertEqual(couchJobDoc["_rev"], row["value"]["rev"],
                             "Error: Rev is wrong.")

        for resultRow in allDocs["rows"]:
            if resultRow["id"] != "_design/FWJRDump":
                fwjrDoc = changeStateDB.document(resultRow["id"])
                break

        assert fwjrDoc["retrycount"] == 0, \
               "Error: Retry count is wrong."

        assert len(fwjrDoc["fwjr"]["steps"].keys()) == 2, \
               "Error: Wrong number of steps in FWJR."
        assert "cmsRun1" in fwjrDoc["fwjr"]["steps"].keys(), \
               "Error: cmsRun1 step is missing from FWJR."
        assert "stageOut1" in fwjrDoc["fwjr"]["steps"].keys(), \
               "Error: stageOut1 step is missing from FWJR."

        return

    def atestDuplicateJobReports(self):
        """
        _testDuplicateJobReports_

        Verify that everything works correctly if a job report is added to the
        database more than once.
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testFile = File(lfn = "SomeLFNC", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 1, \
               "Error: Splitting should have created one job."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Processing"

        change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        reportPath = os.path.join(getTestBase(),
                                  "WMCore_t/JobStateMachine_t/Report.pkl")
        myReport.unpersist(reportPath)
        testJobA["fwjr"] = myReport

        change.propagate([testJobA], 'executing', 'created')
        change.propagate([testJobA], 'executing', 'created')

        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t/fwjrs")
        allDocs = changeStateDB.document("_all_docs")

        self.assertEqual(len(allDocs["rows"]), 2,
                         "Error: Wrong number of documents")

        for resultRow in allDocs["rows"]:
            if resultRow["id"] != "_design/FWJRDump":
                fwjrDoc = changeStateDB.document(resultRow["id"])
                break

        return


    def atestJobKilling(self):
        """
        _testJobKilling_

        Test that we can successfully set jobs to the killed state
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        for i in range(4):
            newFile = File(lfn = "File%s" % i, locations = set(["somese.cern.ch"]))
            newFile.create()
            testFileset.addFile(newFile)

        testFileset.commit()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 4, \
               "Error: Splitting should have created four jobs."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Processing"
        testJobB = jobGroup.jobs[1]
        testJobB["user"] = "sfoulkes"
        testJobB["group"] = "DMWM"
        testJobB["taskType"] = "Processing"
        testJobC = jobGroup.jobs[2]
        testJobC["user"] = "sfoulkes"
        testJobC["group"] = "DMWM"
        testJobC["taskType"] = "Processing"
        testJobD = jobGroup.jobs[3]
        testJobD["user"] = "sfoulkes"
        testJobD["group"] = "DMWM"
        testJobD["taskType"] = "Processing"

        change.persist([testJobA], "created", "new")
        change.persist([testJobB], "jobfailed", "executing")
        change.persist([testJobC, testJobD], "executing", "created")

        change.persist([testJobA], "killed", "created")
        change.persist([testJobB], "killed", "jobfailed")
        change.persist([testJobC, testJobD], "killed", "executing")

        for job in [testJobA, testJobB, testJobC, testJobD]:
            job.load()
            self.assertEqual(job['retry_count'], 99999)
            self.assertEqual(job['state'], 'killed')

        return

    def atestFWJRInputFileTruncation(self):
        """
        _testFWJRInputFileTruncation_

        Test and see whether the ChangeState code can
        be used to automatically truncate the number of input files
        in a FWJR

        Code stolen from the serialization test
        """

        self.config.JobStateMachine.maxFWJRInputFiles = 0
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testFile = File(lfn = "SomeLFNC", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        self.assertEqual(len(jobGroup.jobs), 1,
                         "Error: Splitting should have created one job.")

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Processing"

        change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        reportPath = os.path.join(getTestBase(),
                                  "WMCore_t/JobStateMachine_t/Report.pkl")
        myReport.unpersist(reportPath)

        testJobA["fwjr"] = myReport

        change.propagate([testJobA], 'executing', 'created')

        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t/fwjrs")
        allDocs = changeStateDB.document("_all_docs")

        self.assertEqual(len(allDocs["rows"]), 2,
                         "Error: Wrong number of documents")

        result = changeStateDB.loadView("FWJRDump", "fwjrsByWorkflowName")
        self.assertEqual(len(result["rows"]), 1,
                         "Error: Wrong number of rows.")
        for row in result["rows"]:
            couchJobDoc = changeStateDB.document(row["value"]["id"])
            self.assertEqual(couchJobDoc["_rev"], row["value"]["rev"],
                             "Error: Rev is wrong.")

        for resultRow in allDocs["rows"]:
            if resultRow["id"] != "_design/FWJRDump":
                fwjrDoc = changeStateDB.document(resultRow["id"])
                break

        self.assertEqual(fwjrDoc["fwjr"]["steps"]['cmsRun1']['input']['source'], [])

        return

    def testFWJRSetAsyncOptions(self):
        """
        _testFWJRSetAsyncOptions_

        Test and see whether the ChangeState code can
        be used to propagate whether or not a file needs to be transferred

        Code stolen from the inputFileTruncation test
            who stole from the serialization test
        """

        self.config.JobStateMachine.maxFWJRInputFiles = 0
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testFile = File(lfn = "SomeLFNC", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        self.assertEqual(len(jobGroup.jobs), 1,
                         "Error: Splitting should have created one job.")

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "sfoulkes"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Processing"

        change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        reportPath = os.path.join(getTestBase(),
                                  "WMCore_t/JobStateMachine_t/Report.pkl")
        newPath = os.path.join( self.testInit.testDir, "newreport.pkl" )
        myReport.unpersist(reportPath)
        files = myReport.getAllFileRefsFromStep('cmsRun1')
        recoSize = 5588698
        alcaSize = 5588749
        for onefile in files:
            onefile.branches = ['emptyyyy branch']
            if onefile.module_label == 'outputRECORECO' and onefile.size == recoSize:
                onefile.async_dest = 'T9_US_Mordor'
                onefile.preserve_lfn = True
            elif onefile.module_label == 'outputALCARECOALCARECO' and onefile.size == alcaSize:
                onefile.preserve_lfn = True
                onefile.async_dest = None
            else:
                raise RuntimeError, "Unknown output module"
        myReport.persist(newPath)
        newReport = Report()
        newReport.unpersist(newPath)
        testJobA["fwjr"] = newReport

        change.propagate([testJobA], 'executing', 'created')

        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t/fwjrs")
        allDocs = changeStateDB.document("_all_docs")

        self.assertEqual(len(allDocs["rows"]), 2,
                         "Error: Wrong number of documents")

        result = changeStateDB.loadView("FWJRDump", "fwjrsByWorkflowName")
        self.assertEqual(len(result["rows"]), 1,
                         "Error: Wrong number of rows.")
        for row in result["rows"]:
            couchJobDoc = changeStateDB.document(row["value"]["id"])
            self.assertEqual(couchJobDoc["_rev"], row["value"]["rev"],
                             "Error: Rev is wrong.")

        for resultRow in allDocs["rows"]:
            if resultRow["id"] != "_design/FWJRDump":
                fwjrDoc = changeStateDB.document(resultRow["id"])
                break
        changeStateDB = self.couchServer.connectDatabase(dbname = "job_summary_t")
        jobDumpJobsDB = self.couchServer.connectDatabase(dbname = "changestate_t/jobs")
        allSummaryDocs = changeStateDB.document("_all_docs")
        for resultRow in allSummaryDocs["rows"]:
            if resultRow["id"].find("_design") != -1:
                continue
            doc = changeStateDB.document(resultRow['id'])
            for onefile in doc['output']:
                if onefile['size'] == alcaSize:
                    self.assertFalse( 'asyncDest' in onefile )
                    self.assertFalse( 'preserveLFN' in onefile )
                elif onefile['size'] == recoSize:
                    self.assertEqual( onefile['asyncDest'], 'T9_US_Mordor' )
                    self.assertEqual( onefile['preserveLFN'], True )
                else:
                    self.fail('Found an unknown file, this test should be updated')
        result = jobDumpJobsDB.loadView("JobDump", "jobStatusByWorkflowAndSite")
        pprint.pprint(jobDumpJobsDB.document("_all_docs"))
        pprint.pprint(result)
        self.assertEqual(fwjrDoc["fwjr"]["steps"]['cmsRun1']['input']['source'], [])

        return



    def atestJobSummary(self):
        """
        _testJobSummary_

        verify that job summary for jobs with fwjr are correctly created
        and that status is updated when updatesummary flag is enabled
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testFile = File(lfn = "SomeLFNC", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 1, \
               "Error: Splitting should have created one job."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "cinquo"
        testJobA["group"] = "DMWM"
        testJobA["taskType"] = "Analysis"

        change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        reportPath = os.path.join(getTestBase(),
                                  "WMCore_t/JobStateMachine_t/Report.pkl")
        myReport.unpersist(reportPath)

        change.propagate([testJobA], 'executing', 'created')
        testJobA["fwjr"] = myReport
        change.propagate([testJobA], 'jobfailed', 'executing')

        changeStateDB = self.couchServer.connectDatabase(dbname = "job_summary_t")
        allDocs = changeStateDB.document("_all_docs")

        self.assertEqual(len(allDocs["rows"]), 2,
                         "Error: Wrong number of documents")

        fwjrDoc = {'state': None}
        for resultRow in allDocs["rows"]:
            if resultRow["id"] != "_design/WMStats":
                fwjrDoc = changeStateDB.document(resultRow["id"])
                break

        self.assertEqual(fwjrDoc['state'], 'jobfailed',
                         "Error: summary doesn't have the expected job state")

        del testJobA["fwjr"]

        change.propagate([testJobA], 'jobcooloff', 'jobfailed', updatesummary = True)
        return


    def atestIndexConflict(self):
        """
        _testIndexConflict_

        Verify that in case of conflict in the job index
        we discard the old document and replace with a new
        one
        """
        change = ChangeState(self.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testFile = File(lfn = "SomeLFNC", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 1, \
               "Error: Splitting should have created one job."

        testJobA = jobGroup.jobs[0]
        testJobA["user"] = "dballest"
        testJobA["group"] = "CompOps"
        testJobA["taskType"] = "Processing"

        myReport = Report()
        reportPath = os.path.join(getTestBase(),
                                  "WMCore_t/JobStateMachine_t/Report.pkl")
        myReport.unpersist(reportPath)

        testJobA["fwjr"] = myReport
        change.propagate([testJobA], 'created', 'new')

        jobdatabase = self.couchServer.connectDatabase('changestate_t/jobs', False)
        fwjrdatabase = self.couchServer.connectDatabase('changestate_t/fwjrs', False)
        jobDoc = jobdatabase.document("1")
        fwjrDoc = fwjrdatabase.document("1-0")
        self.assertEqual(jobDoc["workflow"], "wf001", "Wrong workflow in couch job document")
        self.assertEqual(fwjrDoc["fwjr"]["task"], "Test", "Wrong task in fwjr couch document")

        testJobA.delete()

        myThread = threading.currentThread()
        myThread.dbi.processData("ALTER TABLE wmbs_job AUTO_INCREMENT = 1")

        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf002", task = "Test2")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFilesetB")
        testFileset.create()

        testFile = File(lfn = "SomeLFNB", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        testJobB = jobGroup.jobs[0]
        testJobB["user"] = "dballest"
        testJobB["group"] = "CompOps"
        testJobB["taskType"] = "Processing"
        testJobB["fwjr"] = myReport

        change.propagate([testJobB], 'created', 'new')
        jobDoc = jobdatabase.document("1")
        fwjrDoc = fwjrdatabase.document("1-0")
        self.assertEqual(jobDoc["workflow"], "wf002", "Job document was not overwritten")
        self.assertEqual(fwjrDoc["fwjr"]["task"], "Test2", "FWJR document was not overwritten")

        return

if __name__ == "__main__":
    unittest.main()
