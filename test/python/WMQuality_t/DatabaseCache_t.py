import threading, inspect, unittest
from time import gmtime, strftime
import WMQuality.DatabaseCache
from WMQuality.TestInit import TestInit
WMQuality.TestInit.trashDatabases = True
import WMQuality.TestHarness

def timestamp():
    print "%s: %s" % (strftime("%Y-%m-%d %H:%M:%S", gmtime()), \
                      inspect.currentframe().f_back.f_lineno)

class TestSwitchModules(unittest.TestCase):
    @classmethod
    def setup_class(cls): #@NoSelf
        WMQuality.DatabaseCache.disableOfflineCaching()
        WMQuality.DatabaseCache.enterPackage()
        
    @classmethod    
    def teardown_class(cls): #@NoSelf
        WMQuality.DatabaseCache.enableOfflineCaching()
        WMQuality.DatabaseCache.leavePackage()
        
    def testModule1(self):
        self.testInit = TestInit()
        self.testInit.setLogging() # logLevel = logging.SQLDEBUG
        timestamp()
        self.testInit.setDatabaseConnection() 
        timestamp()    

        self.testInit.setSchema(customModules = ["WMCore.RequestManager.RequestDB"],
                                useDefault = False)
        timestamp()
        
    def testModule2(self):
        self.testInit = TestInit()
        self.testInit.setLogging() # logLevel = logging.SQLDEBUG
        timestamp()
        self.testInit.setDatabaseConnection() 
        timestamp()    

        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        timestamp()
        
    def testModule3(self):
        self.testInit = TestInit()
        self.testInit.setLogging() # logLevel = logging.SQLDEBUG
        timestamp()
        self.testInit.setDatabaseConnection() 
        timestamp()    

        self.testInit.setSchema(customModules = ["WMCore.RequestManager.RequestDB"],
                                useDefault = False)
        timestamp()
         
    def tearDown(self):
        self.testInit.clearDatabase()

class TestSQLCaching(unittest.TestCase):
    
    @classmethod
    def setup_class(cls): #@NoSelf
        WMQuality.DatabaseCache.disableOfflineCaching()
        WMQuality.DatabaseCache.enterPackage()
        
    @classmethod    
    def teardown_class(cls): #@NoSelf
        WMQuality.DatabaseCache.enableOfflineCaching()
        WMQuality.DatabaseCache.leavePackage()
        
    def setUp(self):
        self.testInit = TestInit()
        self.testInit.setLogging() # logLevel = logging.SQLDEBUG
        timestamp()
        self.testInit.setDatabaseConnection() 
        timestamp()    

        self.testInit.setSchema(customModules = ["WMCore.RequestManager.RequestDB"],
                                useDefault = False,
                                disableCaching = True)
        timestamp()
        return
    
    def tearDown(self):
        self.testInit.clearDatabase()
    
    def testCachingReaddDeletedRows(self):
        deleteSQL = "DELETE FROM reqmgr_request_type"
        selectSQL = "SELECT Count(*) FROM reqmgr_request_type"

        myThread = threading.currentThread()
        result = myThread.dbi.processData(selectSQL)
        numRows = result[0].fetchone()
        
        moduleList = ["WMCore.RequestManager.RequestDB"]
        WMQuality.DatabaseCache.cacheSQL(moduleList)
        self.assertEqual( myThread.dbi.processData(selectSQL)[0].fetchone(),
                          numRows,
                          "Number of rows changed during caching")
        
        # delete some rows
        myThread.dbi.processData(deleteSQL)
        
        self.assertTrue( WMQuality.DatabaseCache.loadCachedSQL( moduleList ), \
                         "The cache should've worked")
        
        self.assertEqual( myThread.dbi.processData(selectSQL)[0].fetchone(),
                          numRows,
                          "Number of rows changed when reloading DB from cache")
        
    def testCachingFailOnMissingTable(self):
        deleteSQL = "DROP TABLE reqmgr_request_type"
        selectSQL = "SELECT Count(*) FROM reqmgr_request_type"

        myThread = threading.currentThread()
        result = myThread.dbi.processData(selectSQL)
        numRows = result[0].fetchone()
        
        moduleList = ["WMCore.RequestManager.RequestDB"]
        WMQuality.DatabaseCache.cacheSQL(moduleList)
        self.assertEqual( myThread.dbi.processData(selectSQL)[0].fetchone(),
                          numRows,
                          "Number of rows changed during caching")
        
        # delete a table
        try:
            myThread.dbi.processData("SET foreign_key_checks = 0")
            myThread.dbi.processData(deleteSQL)
        finally:
            myThread.dbi.processData("SET foreign_key_checks = 1")

        
        self.assertFalse( WMQuality.DatabaseCache.loadCachedSQL( moduleList ), \
                         "The cache should've complained")
        
    def testCachingDeleteExtraneousTable(self):
        createSQL = "CREATE TABLE test_table ( val INT(11) NOT NULL )"
        selectSQL = "SELECT Count(*) FROM test_table"

        myThread = threading.currentThread()
        
        moduleList = ["WMCore.RequestManager.RequestDB"]
        WMQuality.DatabaseCache.cacheSQL(moduleList)
        
        # createTable
        myThread.dbi.processData(createSQL)
        myThread.dbi.processData(selectSQL)
        self.assertTrue( WMQuality.DatabaseCache.loadCachedSQL( moduleList ), \
                         "The cache should be okay")
        self.assertRaises( Exception, myThread.dbi.processData, selectSQL)
        
        
    def testCachingDeleteInsertedROW(self):
        insertSQL = "INSERT INTO reqmgr_request_type(type_name) VALUES ('DUMMY')"
        selectSQL = "SELECT Count(*) FROM reqmgr_request_type"

        myThread = threading.currentThread()
        result = myThread.dbi.processData(selectSQL)
        numRows = result[0].fetchone()
        
        moduleList = ["WMCore.RequestManager.RequestDB"]
        WMQuality.DatabaseCache.cacheSQL(moduleList)
        self.assertEqual( myThread.dbi.processData(selectSQL)[0].fetchone(),
                          numRows,
                          "Number of rows changed during caching")
        
        # delete some rows
        myThread.dbi.processData(insertSQL)
        
        self.assertTrue( WMQuality.DatabaseCache.loadCachedSQL( moduleList ), \
                         "The cache should've worked")
        
        self.assertEqual( myThread.dbi.processData(selectSQL)[0].fetchone(),
                          numRows,
                          "Number of rows changed when reloading DB from cache")


if __name__ == "__main__":
    WMQuality.TestHarness.main()
