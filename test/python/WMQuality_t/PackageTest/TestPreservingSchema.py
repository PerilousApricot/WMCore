import nose, tempfile, shutil
import threading, inspect, unittest
import os
from time import gmtime, strftime
from WMCore.Services.Requests   import JSONRequests
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup  import DefaultConfig
from WMCore.DAOFactory         import DAOFactory
import WMQuality.TestInit
WMQuality.TestInit.trashDatabases = True


import WMQuality.DatabaseCache

def setup_module():
    WMQuality.DatabaseCache.enterPackage( )
    
def teardown_module():
    WMQuality.DatabaseCache.leavePackage( )

def timestamp():
    print "%s: %s" % (strftime("%Y-%m-%d %H:%M:%S", gmtime()), \
                      inspect.currentframe().f_back.f_lineno)
    
class PreserveSchemaBase(unittest.TestCase):

    def setUp(self):
        """
        setUP global values
        Database setUp is done in base class
        """
        self.reqmgrDBName      = "reqmgr_t_reqmgr"
        self.configCacheDBName = "reqmgr_t_ccache"
        self.wmstatDBName      = "reqmgr_t_wmstat"
        timestamp()
           
        from WMQuality.TestInitCouchApp import TestInitCouchApp
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging() # logLevel = logging.SQLDEBUG
        timestamp()
        self.testInit.setDatabaseConnection() 
        timestamp()    
        self.testInit.setupCouch("%s" % self.reqmgrDBName, "ReqMgr")
        timestamp()
        self.testInit.setupCouch("%s" % self.configCacheDBName, \
                                            "ConfigCache", "GroupUser")
        timestamp()
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMCore.RequestManager.RequestDB"],
                                useDefault = False)
        timestamp()
        myThread = threading.currentThread()
        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        locationAction = self.daofactory(classname = "Locations.New")
        timestamp()
        locationAction.execute(siteName = "site1", seName = "se1.cern.ch")
        timestamp()
        locationAction.execute(siteName = "site2", seName = "se1.fnal.gov")
        timestamp()
        return

    def tearDown(self):
        """
        tearDown 

        Tear down everything
        """
        self.testInit.tearDownCouch()
        return

class TestUnchangedSchema(PreserveSchemaBase):
    def testUnsupportedFormat(self):
        # test not accepted type should return 406 error
        #url = self.urlbase + 'ping'
        #methodTest('GET', url, accept='text/das', output={'code':406})
        pass
    
    def testUnsupportedFormat2(self):
        # test not accepted type should return 406 error
        #url = self.urlbase + 'ping'
        #methodTest('GET', url, accept='text/das', output={'code':406})
        pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    print nose.__file__
    print "chm"
    nose.main()
