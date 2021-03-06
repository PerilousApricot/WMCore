#!/usr/bin/env python
#pylint: disable-msg=E1101, W6501, W0142, C0103, W0401, E1103
# W0401: I am not going to import all those functions by hand
"""

DBSUpload test TestDBSUpload module and the harness

"""

import os
import threading
import time
import unittest

from nose.plugins.attrib import attr

from WMCore.WMFactory       import WMFactory
from WMQuality.TestInit     import TestInit
from WMCore.DAOFactory      import DAOFactory
from WMCore.Services.UUID   import makeUUID
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID   import makeUUID

from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.HeartbeatAPI  import HeartbeatAPI

from WMComponent.DBS3Buffer.DBSBufferFile   import DBSBufferFile
from WMComponent.DBS3Buffer.DBSBufferUtil   import DBSBufferUtil
from WMComponent.DBS3Buffer.DBSUploadPoller import DBSUploadPoller
from WMComponent.DBS3Buffer.DBSBufferBlock  import DBSBlock

from dbs.apis.dbsClient import DbsApi

class DBSUploadTest(unittest.TestCase):
    """
    TestCase for DBSUpload module

    """
    def setUp(self):
        """
        _setUp_

        setUp function for unittest

        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase = True)
        self.testInit.setSchema(customModules = ["WMComponent.DBS3Buffer"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        locationAction = self.bufferFactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "se1.cern.ch")
        locationAction.execute(siteName = "se1.fnal.gov")
        locationAction.execute(siteName = "malpaquet")

        self.dbsUrl = "https://localhost:1443/dbs/dev/global/DBSWriter"
        self.dbsApi = DbsApi(url = self.dbsUrl)
        return

    def tearDown(self):
        """
        _tearDown_

        tearDown function for unittest
        """
        return
        self.testInit.clearDatabase()

    def getConfig(self, dbs3UploadOnly = False):
        """
        _getConfig_

        This creates the actual config file used by the component.
        """
        config = Configuration()

        #First the general stuff
        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR", os.getcwd())

        config.section_("Agent")
        config.Agent.componentName = 'DBSUpload'
        config.Agent.useHeartbeat  = False

        #Now the CoreDatabase information
        #This should be the dialect, dburl, etc
        config.section_("CoreDatabase")
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        config.CoreDatabase.socket     = os.getenv("DBSOCK")

        config.component_("DBSUpload")
        config.DBSUpload.pollInterval     = 10
        config.DBSUpload.logLevel         = 'DEBUG'
        config.DBSUpload.DBSBlockMaxFiles = 5
        config.DBSUpload.DBSBlockMaxTime  = 1
        config.DBSUpload.DBSBlockMaxSize  = 999999999999
        #config.DBSUpload.dbsUrl           = "https://cmsweb-testbed.cern.ch/dbs/dev/global/DBSWriter"
        #config.DBSUpload.dbsUrl           = "https://dbs3-dev01.cern.ch/dbs/prod/global/DBSWriter"
        config.DBSUpload.dbsUrl           = self.dbsUrl
        config.DBSUpload.namespace        = 'WMComponent.DBS3Buffer.DBSUpload'
        config.DBSUpload.componentDir     = os.path.join(os.getcwd(), 'Components')
        config.DBSUpload.nProcesses       = 1
        config.DBSUpload.dbsWaitTime      = 0.1
        config.DBSUpload.datasetType      = "VALID"
        config.DBSUpload.dbs3UploadOnly   = dbs3UploadOnly
        return config

    def createParentFiles(self, acqEra):
        """
        _createParentFiles_

        Create several parentless files in DBSBuffer.  This simulates raw files
        in the T0.
        """
        parentlessFiles = []
        
        baseLFN = "/store/data/%s/Cosmics/RAW/v1/000/143/316/" % (acqEra)
        for i in range(10):
            testFile = DBSBufferFile(lfn = baseLFN + makeUUID() + ".root", size = 1024,
                                     events = 20, checksums = {"cksum": 1})
            testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                  appFam = "RAW", psetHash = "GIBBERISH",
                                  configContent = "MOREGIBBERISH")
            testFile.setDatasetPath("/Cosmics/%s-v1/RAW" % (acqEra))

            lumis = []
            for j in range(10):
                lumis.append((i * 10) + j)
            testFile.addRun(Run(143316, *lumis))

            testFile.setAcquisitionEra(acqEra)
            testFile.setProcessingVer("1")
            testFile.setGlobalTag("START54::All")
            testFile.create()
            testFile.setLocation("malpaquet")
            parentlessFiles.append(testFile)

        return parentlessFiles

    def createFilesWithChildren(self, moreParentFiles, acqEra):
        """
        _createFilesWithChildren_

        Create several parentless files and then create child files.
        """
        parentFiles = []
        childFiles = []
        
        baseLFN = "/store/data/%s/Cosmics/RAW/v1/000/143/316/" % (acqEra)
        for i in range(10):
            testFile = DBSBufferFile(lfn = baseLFN + makeUUID() + ".root", size = 1024,
                                     events = 20, checksums = {"cksum": 1})
            testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                  appFam = "RAW", psetHash = "GIBBERISH",
                                  configContent = "MOREGIBBERISH")
            testFile.setDatasetPath("/Cosmics/%s-v1/RAW" % (acqEra))

            lumis = []
            for j in range(10):
                lumis.append((i * 10) + j)
            testFile.addRun(Run(143316, *lumis))            

            testFile.setAcquisitionEra(acqEra)
            testFile.setProcessingVer("1")
            testFile.setGlobalTag("START54::All")
            testFile.create()
            testFile.setLocation("malpaquet")
            parentFiles.append(testFile)

        baseLFN = "/store/data/%s/Cosmics/RECO/v1/000/143/316/" % (acqEra)
        for i in range(5):
            testFile = DBSBufferFile(lfn = baseLFN + makeUUID() + ".root", size = 1024,
                                     events = 20, checksums = {"cksum": 1})
            testFile.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_3_1_1",
                                  appFam = "RECO", psetHash = "GIBBERISH",
                                  configContent = "MOREGIBBERISH")
            testFile.setDatasetPath("/Cosmics/%s-v1/RECO" % (acqEra))

            lumis = []
            for j in range(20):
                lumis.append((i * 20) + j)
            testFile.addRun(Run(143316, *lumis))            

            testFile.setAcquisitionEra(acqEra)
            testFile.setProcessingVer("1")
            testFile.setGlobalTag("START54::All")
            testFile.create()
            testFile.setLocation("malpaquet")
            testFile.addParents([parentFiles[i * 2]["lfn"],
                                 parentFiles[i * 2 + 1]["lfn"]])
            testFile.addParents([moreParentFiles[i * 2]["lfn"],
                                 moreParentFiles[i * 2 + 1]["lfn"]])
            childFiles.append(testFile)            

        return (parentFiles, childFiles)

    def verifyData(self, datasetName, files):
        """
        _verifyData_

        Verify the data in DBS3 matches what was uploaded to it.
        """
        (junk, primary, processed, tier) = datasetName.split("/")
        (acqEra, procVer) = processed.split("-")

        result = self.dbsApi.listDatasets(dataset = datasetName, detail = True,
                                          dataset_access_type = "VALID")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["data_tier_name"], tier,
                         "Error: Wrong data tier.")
        self.assertEqual(result[0]["processing_version"], 1,
                         "Error: Wrong processing version.")
        self.assertEqual(result[0]["acquisition_era_name"], acqEra,
                         "Error: Wrong acquisition era.")
        self.assertEqual(result[0]["physics_group_name"], "NoGroup",
                         "Error: Wrong physics group name.")
        
        results = self.dbsApi.listBlocks(dataset = datasetName, detail = True)
        self.assertEqual(len(results), len(files) / 5,
                         "Error: Wrong number of blocks.")
        for result in results:
            self.assertEqual(result["block_size"], 5120,
                             "Error: Wrong block size.")
            self.assertEqual(result["file_count"], 5,
                             "Error: Wrong number of files.")
            self.assertEqual(result["open_for_writing"], 0,
                             "Error: Block should be closed.")
        
        results = self.dbsApi.listFiles(dataset = datasetName, detail = True)
        for result in results:
            file = None
            for file in files:
                if file["lfn"] == result["logical_file_name"]:
                    break
            else:
                file = None

            self.assertTrue(file != None, "Error: File not found.")
            self.assertEqual(file["size"], result["file_size"],
                             "Error: File size mismatch.")
            self.assertEqual(file["events"], result["event_count"],
                             "Error: Event count mismatch.")

            dbsParents = self.dbsApi.listFileParents(logical_file_name = file["lfn"])
            if len(dbsParents) == 0:
                self.assertEqual(len(file["parents"]), 0,
                                 "Error: Missing parents.")
            else:
                for dbsParent in dbsParents[0]["parent_logical_file_name"]:
                    fileParent = None
                    for fileParent in file["parents"]:
                        if fileParent["lfn"] == dbsParent:
                            break
                    else:
                        fileParent = None

                    self.assertTrue(fileParent != None, "Error: Missing parent.")
                self.assertEqual(len(file["parents"]), len(dbsParents[0]["parent_logical_file_name"]),
                                 "Error: Wrong number of parents.")

            runLumis = self.dbsApi.listFileLumis(logical_file_name = file["lfn"])
            for runLumi in runLumis:
                fileRun = None
                for fileRun in file["runs"]:
                    if fileRun.run == runLumi["run_num"]:
                        for lumi in runLumi["lumi_section_num"]:
                            self.assertTrue(lumi in fileRun.lumis,
                                            "Error: Missing lumis.")
                        break
                else:
                    fileRun = None

                self.assertTrue(fileRun != None, "Error: Missing run.")

        self.assertEqual(len(files), len(results), "Error: Files missing.")
        return

    @attr("integration")
    def testBasicUpload(self):
        """
        _testBasicUpload_

        Verify that we can successfully upload to DBS3.  Also verify that the
        uploader correctly handles files parentage when uploading.
        """
        config = self.getConfig()
        dbsUploader = DBSUploadPoller(config = config)

        # First test verifies that uploader will poll and then not do anything
        # as the database is empty.
        dbsUploader.algorithm()

        acqEra = "Summer%s" % (int(time.time()))
        parentFiles = self.createParentFiles(acqEra)

        # The algorithm needs to be run twice.  On the first iteration it will
        # create all the blocks and upload one.  On the second iteration it will
        # timeout and upload the second block.
        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        # Verify the files made it into DBS3.
        self.verifyData(parentFiles[0]["datasetPath"], parentFiles)

        # Inject some more parent files and some child files into DBSBuffer.
        # Run the uploader twice, only the parent files should be added to DBS3.
        (moreParentFiles, childFiles) = \
                          self.createFilesWithChildren(parentFiles, acqEra)
        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(parentFiles[0]["datasetPath"],
                        parentFiles + moreParentFiles)

        # Run the uploader another two times to upload the child files.  Verify
        # that the child files were uploaded.
        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(childFiles[0]["datasetPath"], childFiles)
        return

    @attr("integration")
    def testDualUpload(self):
        """
        _testDualUpload_

        Verify that the dual upload mode works correctly.
        """
        config = self.getConfig(dbs3UploadOnly = True)
        dbsUploader = DBSUploadPoller(config = config)
        dbsUtil = DBSBufferUtil()

        # First test verifies that uploader will poll and then not do anything
        # as the database is empty.
        dbsUploader.algorithm()

        acqEra = "Summer%s" % (int(time.time()))
        parentFiles = self.createParentFiles(acqEra)
        (moreParentFiles, childFiles) = \
                          self.createFilesWithChildren(parentFiles, acqEra)

        allFiles = parentFiles + moreParentFiles
        allBlocks = []
        for i in range(4):
            blockName = parentFiles[0]["datasetPath"] + "#" + makeUUID()
            dbsBlock = DBSBlock(blockName, "malpaquet", 1)
            dbsBlock.status = "Open"                
            dbsUtil.createBlocks([dbsBlock])
            for file in allFiles[i * 5 : (i * 5) + 5]:
                dbsBlock.addFile(file)
                dbsUtil.setBlockFiles({"block": blockName, "filelfn": file["lfn"]})
                if i < 2:
                    dbsBlock.status = "InDBS"
                dbsUtil.updateBlocks([dbsBlock])
            dbsUtil.updateFileStatus([dbsBlock], "InDBS")
            allBlocks.append(dbsBlock)            

        blockName = childFiles[0]["datasetPath"] + "#" + makeUUID()
        dbsBlock = DBSBlock(blockName, "malpaquet", 1)
        dbsBlock.status = "InDBS"
        dbsUtil.createBlocks([dbsBlock])
        for file in childFiles:
            dbsBlock.addFile(file)
            dbsUtil.setBlockFiles({"block": blockName, "filelfn": file["lfn"]})

        dbsUtil.updateFileStatus([dbsBlock], "InDBS")

        dbsUploader.algorithm()
        time.sleep(5)
        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(parentFiles[0]["datasetPath"], parentFiles)

        # Change the status of the rest of the parent blocks so we can upload
        # them and the children.
        for dbsBlock in allBlocks:
            dbsBlock.status = "InDBS"            
            dbsUtil.updateBlocks([dbsBlock])

        dbsUploader.algorithm()
        time.sleep(5)

        self.verifyData(parentFiles[0]["datasetPath"], parentFiles + moreParentFiles)

        # Run the uploader one more time to upload the children.
        dbsUploader.algorithm()
        time.sleep(5)        
    
        self.verifyData(childFiles[0]["datasetPath"], childFiles)
        return

if __name__ == '__main__':
    unittest.main()
