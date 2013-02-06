#!/usr/bin/env python
"""
_DataProcessing_t_

Unit tests for the data processing workflow.
"""

import unittest
import os
import threading

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow

from WMCore.WorkQueue.WMBSHelper import WMBSHelper
import WMCore.WMSpec.StdSpecs.MeloProcessing
from WMCore.WMSpec.StdSpecs.MeloProcessing import getTestArguments, meloProcessingWorkload
from WMCore.Database.CMSCouch import CouchServer, Document

from WMQuality.TestInitCouchApp import TestInitCouchApp

def testingLfnPrefixName(site, lfn):
    return ("srm://testingSRMPrefix/", "se.meloprocessing.testing.prefix")

class MeloProcessingTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase = True)
        self.testInit.setupCouch("config_cache_t", "ConfigCache")
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("config_cache_t")
        
        self.oldPrefixFunc = WMCore.WMSpec.StdSpecs.MeloProcessing.remoteLFNPrefix
        WMCore.WMSpec.StdSpecs.MeloProcessing.remoteLFNPrefix = testingLfnPrefixName
        return
    
    def injectAnalysisConfig(self):
        """
        Create a bogus config cache document for the analysis workflow and
        inject it into couch.  Return the ID of the document.
        """

        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["pset_hash"] = "21cb400c6ad63c3a97fa93f8e8785127"
        newConfig["owner"] = {"group": "Analysis", "user": "mmascher"}
        newConfig["pset_tweak_details"] = {
                                           "process": {
                                                       "maxEvents": {"parameters_": ["input"], "input": 10},
                                                       "outputModules_": ["output", "secondOutput"],
                                                       "parameters_": ["outputModules_"],
                                                       "source": {"parameters_": ["fileNames"], "fileNames": []},
                                                       "output": {"parameters_": ["fileName"], "fileName": "outfile.root"},
                                                       "secondOutput": {"parameters_": ["fileName"], "fileName": "outfile2.root"},
                                                       "options": {"parameters_": ["wantSummary"], "wantSummary": True}
                                                      }
        }
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]
    
    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        WMCore.WMSpec.StdSpecs.MeloProcessing.remoteLFNPrefix = self.oldPrefixFunc
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        return
    
    def testMeloProcessingSpec(self):
        testArgs = getTestArguments()
        testArgs["CouchUrl"] = os.environ["COUCHURL"]
        testArgs["CouchDBName"] = "config_cache_t"
        testArgs["ConfigCacheID"] = self.injectAnalysisConfig()
        testWorkloadNoASO      = meloProcessingWorkload("TestWorkloadNoASO", testArgs)
        testArgs["AsyncDest"]  = "T2_US_Vanderbilt"
        testWorkloadAsyncVandy = meloProcessingWorkload("TestWorkloadVandy", testArgs)
        testArgs["AsyncDest"]  = "T1_US_FNAL"
        testWorkloadAsyncFNAL  = meloProcessingWorkload("TestWorkloadFNAL", testArgs)
        
        for testWorkload, isASO, isFnal in ( (testWorkloadNoASO, False, False),
                                             (testWorkloadAsyncVandy, True, False), 
                                             (testWorkloadAsyncFNAL, True, True) ):
            workloadName = testWorkload.name()
            processTask = testWorkload.getTaskByPath('/%s/MeloProcessing' % workloadName)
            logTask     = testWorkload.getTaskByPath('/%s/MeloProcessing/LogCollect' % workloadName)
            mergeTask1  = testWorkload.getTaskByPath('/%s/MeloProcessing/MeloProcessingMergeoutput' % workloadName)
            mergeTask2  = testWorkload.getTaskByPath('/%s/MeloProcessing/MeloProcessingMergesecondOutput' % workloadName)
            cleanTask1  = testWorkload.getTaskByPath('/%s/MeloProcessing/MeloProcessingCleanupUnmergedoutput' % workloadName)
            cleanTask2  = testWorkload.getTaskByPath('/%s/MeloProcessing/MeloProcessingCleanupUnmergedsecondOutput' % workloadName)
            
            for task in [processTask, logTask, mergeTask1, mergeTask2, cleanTask1, cleanTask2]:
                self.assertNotEqual( task, None )


            
            procStageOut   = processTask.getStep('stageOut1')
            procLogArch    = processTask.getStep('logArch1')
            logStep        = logTask.getStep('logCollect1')
            merge1StageOut = mergeTask1.getStep('stageOut1')
            merge2StageOut = mergeTask1.getStep('stageOut1')
            merge1LogArch  = mergeTask2.getStep('logArch1')
            merge2LogArch  = mergeTask2.getStep('logArch1')
            
            for step in [ procStageOut, procLogArch, logStep, merge1StageOut, merge2StageOut, \
                            merge1LogArch, merge2LogArch ]:
                self.assertNotEqual( step, None )
                
            if isASO and isFnal:
                # merge jobs can't land at FNAL, so we need to merge somewhere else and send 
                # only the merged output
                self.assertEqual( getattr(merge1StageOut.data, 'asyncDest'),'T1_US_FNAL' )
                self.assertEqual( getattr(merge2StageOut.data, 'asyncDest'),'T1_US_FNAL' )
                self.assertFalse( getattr(procStageOut.data, 'asyncDest', None) )
                self.assertTrue( getattr(procStageOut.data.output, 'doNotDirectMerge', False) )
                self.assertFalse( procStageOut.getPreserveLFN() )

            elif isASO and not isFnal:
                # send the unmerged via ASO, then the merges will run at their target site
                self.assertEqual( getattr(procStageOut.data, 'asyncDest'),'T2_US_Vanderbilt' )
                self.assertFalse( getattr(merge1StageOut.data, 'asyncDest', None) )
                self.assertFalse( getattr(merge2StageOut.data, 'asyncDest', None) )
                self.assertTrue( getattr(procStageOut.data.output, 'doNotDirectMerge', False) )
                self.assertTrue( procStageOut.getPreserveLFN() )
            
            else:
                for step in [ procStageOut, merge1StageOut, merge2StageOut ]:
                    self.assertFalse( getattr(step.data, 'asyncDest', None) )
        
    def aatestMeloProcessingWMBS(self):
        testArgs = getTestArguments()
        testArgs["CouchUrl"] = os.environ["COUCHURL"]
        testArgs["CouchDBName"] = "config_cache_t"
        testArgs["ConfigCacheID"] = self.injectAnalysisConfig()
        testWorkloadNoASO      = meloProcessingWorkload("TestWorkloadNoASO", testArgs)
        testArgs["AsyncDest"]  = "T2_US_Vanderbilt"
        testWorkloadAsyncVandy = meloProcessingWorkload("TestWorkloadVandy", testArgs)
        testArgs["AsyncDest"]  = "T2_US_Vanderbilt"
        testWorkloadAsyncFNAL  = meloProcessingWorkload("TestWorkloadFNAL", testArgs)
        
        for testWorkload, isASO, isFnal in ( (testWorkloadNoASO, False, False),
                                             (testWorkloadAsyncVandy, True, False), 
                                             (testWorkloadAsyncFNAL, True, True) ):
            testWorkload.setSpecUrl("somespec")
            testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DMWM")
    
            testWMBSHelper = WMBSHelper(testWorkload, "MeloProcessing", "SomeBlock")
            testWMBSHelper.createTopLevelFileset()
            testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)
    
            procWorkflow = Workflow(name = "TestWorkload",
                                    task = "/TestWorkload/MeloProcessing")
            procWorkflow.load()
            goldenOutputMods = ["output", "secondOutput"]

            for goldenOutputMod in goldenOutputMods:
                mergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["merged_output_fileset"]
                unmergedOutput = procWorkflow.outputMap[goldenOutputMod][0]["output_fileset"]
    
                mergedOutput.loadData()
                unmergedOutput.loadData()
    
                self.assertEqual(mergedOutput.name, "/TestWorkload/MeloProcessing/MeloProcessingMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Merged output fileset is wrong: %s" % mergedOutput.name)
                self.assertEqual(unmergedOutput.name, "/TestWorkload/MeloProcessing/unmerged-%s" % goldenOutputMod,
                                 "Error: Unmerged output fileset is wrong.")
    
            logArchOutput = procWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
            unmergedLogArchOutput = procWorkflow.outputMap["logArchive"][0]["output_fileset"]
            logArchOutput.loadData()
            unmergedLogArchOutput.loadData()
    
            self.assertEqual(logArchOutput.name, "/TestWorkload/MeloProcessing/unmerged-logArchive",
                             "Error: LogArchive output fileset is wrong.")
            self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/MeloProcessing/unmerged-logArchive",
                             "Error: LogArchive output fileset is wrong.")
    
            for goldenOutputMod in goldenOutputMods:
                mergeWorkflow = Workflow(name = "TestWorkload",
                                         task = "/TestWorkload/MeloProcessing/MeloProcessingMerge%s" % goldenOutputMod)
                mergeWorkflow.load()
    
                self.assertEqual(len(mergeWorkflow.outputMap.keys()), 2,
                                 "Error: Wrong number of WF outputs.")
    
                mergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["merged_output_fileset"]
                unmergedMergeOutput = mergeWorkflow.outputMap["Merged"][0]["output_fileset"]
    
                mergedMergeOutput.loadData()
                unmergedMergeOutput.loadData()
    
                self.assertEqual(mergedMergeOutput.name, "/TestWorkload/MeloProcessing/MeloProcessingMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Merged output fileset is wrong.")
                self.assertEqual(unmergedMergeOutput.name, "/TestWorkload/MeloProcessing/MeloProcessingMerge%s/merged-Merged" % goldenOutputMod,
                                 "Error: Unmerged output fileset is wrong.")
    
                logArchOutput = mergeWorkflow.outputMap["logArchive"][0]["merged_output_fileset"]
                unmergedLogArchOutput = mergeWorkflow.outputMap["logArchive"][0]["output_fileset"]
                logArchOutput.loadData()
                unmergedLogArchOutput.loadData()
    
                self.assertEqual(logArchOutput.name, "/TestWorkload/MeloProcessing/MeloProcessingMerge%s/merged-logArchive" % goldenOutputMod,
                                 "Error: LogArchive output fileset is wrong: %s" % logArchOutput.name)
                self.assertEqual(unmergedLogArchOutput.name, "/TestWorkload/MeloProcessing/MeloProcessingMerge%s/merged-logArchive" % goldenOutputMod,
                                 "Error: LogArchive output fileset is wrong.")
    
            topLevelFileset = Fileset(name = "TestWorkload-MeloProcessing-SomeBlock")
            topLevelFileset.loadData()
    
            procSubscription = Subscription(fileset = topLevelFileset, workflow = procWorkflow)
            procSubscription.loadData()
    
            self.assertEqual(procSubscription["type"], "Analysis",
                             "Error: Wrong subscription type.")
            self.assertEqual(procSubscription["split_algo"], "LumiBased",
                             "Error: Wrong split algo.")
    
            unmergedReco = Fileset(name = "/TestWorkload/MeloProcessing/unmerged-output")
            unmergedReco.loadData()
            recoMergeWorkflow = Workflow(name = "TestWorkload",
                                         task = "/TestWorkload/MeloProcessing/MeloProcessingMergeoutput")
            recoMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedReco, workflow = recoMergeWorkflow)
            mergeSubscription.loadData()
    
            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo.")
    
            unmergedAlca = Fileset(name = "/TestWorkload/MeloProcessing/unmerged-secondOutput")
            unmergedAlca.loadData()
            alcaMergeWorkflow = Workflow(name = "TestWorkload",
                                         task = "/TestWorkload/MeloProcessing/MeloProcessingMergesecondOutput")
            alcaMergeWorkflow.load()
            mergeSubscription = Subscription(fileset = unmergedAlca, workflow = alcaMergeWorkflow)
            mergeSubscription.loadData()
    
            self.assertEqual(mergeSubscription["type"], "Merge",
                             "Error: Wrong subscription type.")
            self.assertEqual(mergeSubscription["split_algo"], "ParentlessMergeBySize",
                             "Error: Wrong split algo.")
    
            for procOutput in ["output", "secondOutput"]:
                unmerged = Fileset(name = "/TestWorkload/MeloProcessing/unmerged-%s" % procOutput)
                unmerged.loadData()
                cleanupWorkflow = Workflow(name = "TestWorkload",
                                          task = "/TestWorkload/MeloProcessing/MeloProcessingCleanupUnmerged%s" % procOutput)
                cleanupWorkflow.load()
                cleanupSubscription = Subscription(fileset = unmerged, workflow = cleanupWorkflow)
                cleanupSubscription.loadData()
    
                self.assertEqual(cleanupSubscription["type"], "Cleanup",
                                 "Error: Wrong subscription type.")
                self.assertEqual(cleanupSubscription["split_algo"], "SiblingProcessingBased",
                                 "Error: Wrong split algo.")
    
            procLogCollect = Fileset(name = "/TestWorkload/MeloProcessing/unmerged-logArchive")
            procLogCollect.loadData()
            procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                              task = "/TestWorkload/MeloProcessing/LogCollect")
            procLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
            logCollectSub.loadData()
    
            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")
    
            procLogCollect = Fileset(name = "/TestWorkload/MeloProcessing/MeloProcessingMergeoutput/merged-logArchive")
            procLogCollect.loadData()
            procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                              task = "/TestWorkload/MeloProcessing/MeloProcessingMergeoutput/MeloProcessingoutputMergeLogCollect")
            procLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
            logCollectSub.loadData()
    
            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")
    
            procLogCollect = Fileset(name = "/TestWorkload/MeloProcessing/MeloProcessingMergesecondOutput/merged-logArchive")
            procLogCollect.loadData()
            procLogCollectWorkflow = Workflow(name = "TestWorkload",
                                              task = "/TestWorkload/MeloProcessing/MeloProcessingMergesecondOutput/MeloProcessingsecondOutputMergeLogCollect")
            procLogCollectWorkflow.load()
            logCollectSub = Subscription(fileset = procLogCollect, workflow = procLogCollectWorkflow)
            logCollectSub.loadData()
    
            self.assertEqual(logCollectSub["type"], "LogCollect",
                             "Error: Wrong subscription type.")
            self.assertEqual(logCollectSub["split_algo"], "MinFileBased",
                             "Error: Wrong split algo.")
    
            return

if __name__ == '__main__':
    unittest.main()
