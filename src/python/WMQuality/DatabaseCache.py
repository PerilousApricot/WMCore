'''
Created by Andrew Melo <andrew.melo@gmail.com> on Sep 26, 2012

'''
import os, pickle
from WMCore.DAOFactory  import DAOFactory
import threading
import traceback

testInitWasTriggered  = False
offlineCachingEnabled = True
suiteDepth            = 0
cachedModules         = []
cachedDump            = {}

def compareModules(requestedModules):
    global cachedModules
    cachedModules.sort()
    requestedModules.sort()
    return cachedModules == requestedModules

def getStatePath( ):
    return os.environ.get('WMCORE_TEST_DB_STATE_PATH', None)

def enableOfflineCaching():
    global offlineCachingEnabled
    offlineCachingEnabled = True

def disableOfflineCaching():
    global offlineCachingEnabled
    offlineCachingEnabled = True

def loadPickledState():
    global cachedModules, cachedDump
    if getStatePath():
        try:
            state = pickle.load( open(getStatePath(),'r' ) )
        except:
            return
        
        cachedModules = state['modules']
        cachedDump    = state['dump']

def savePickledState():
    """
    returns True if the pickle was loaded
    returns if the pickle couldn't be loaded
    """ 
    
    global cachedModules, cachedDump
    if getStatePath():
        try:
            state = { 'modules' : cachedModules,
                      'dump'    : cachedDump }
            pickle.dump( state, open(getStatePath(), 'w') )
        except:
            try:
                os.unlink( getStatePath() )
            except:
                pass
            return False
        return True
    return False
            
def enterPackage( ):
    global suiteDepth, offlineCachingEnabled
    import WMQuality.TestInit
    WMQuality.TestInit.deleteDatabaseAfterEveryTest( "I'm Serious" )
    suiteDepth += 1
    if suiteDepth == 1 and offlineCachingEnabled:
        loadPickledState()    
    
def leavePackage( ):
    global testInitWasTriggered, suiteDepth
    suiteDepth -= 1
    if suiteDepth == 0 and testInitWasTriggered:
        if not savePickledState():
            destroyCachedSQL()

def cacheSQL( modulesInCache ):
    global cachedDump, cachedModules, suiteDepth
    # cache information about the current SQL schema
    if suiteDepth > 0:
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.Database",
                                         logger = myThread.logger,
                                         dbinterface = myThread.dbi)
    
        cacheAction = daofactory(classname = "DumpAll")
        cachedDump = cacheAction.execute()
        cachedModules = modulesInCache
        return True
    elif suiteDepth == 0:
        return False
    else:
        raise RuntimeError, "Negative suite depth!"

def attemptTinyCreate( modulesRequested, params = None ):
    global cachedDump, allowedToDeleteAll, suiteDepth, cachedModules
    if suiteDepth > 0:
        if compareModules(modulesRequested):
            raise RuntimeError, "execute creates"
            init = WMInit()
            try:
                init.setSchema( modulesRequested, params = params, tinyCreate = True )
                return True
            except:
                return False
                
        else: # not the same modules requested
            return False
    elif suiteDepth == 0:
        return False
    else:
        raise RuntimeError, "Negative suite depth!"

def enableDatabaseTeardown():
    global testInitWasTriggered
    trace = traceback.extract_stack()
    # really really really doublecheck that this was called from TestInit
    if len(trace) > 2 and \
       trace[-2][2] == 'clearDatabase' and \
       trace[-2][3] == 'WMQuality.DatabaseCache.enableDatabaseTeardown()' and \
       trace[-2][0].endswith('WMQuality/TestInit.py'):
        testInitWasTriggered = True
        
def disableDatabaseTeardown():
    global testInitWasTriggered
    testInitWasTriggered = False
    
def destroyCachedSQL():
    # if testInit.clearDatabase() is called, and the caching is enabled, we
    # deferred tearing down the database. In that case, we can't defer it
    # forever, so we should call it
    
    # currently TestInit just unconditionally blows away databases in the
    # clearDatabase() function, which is crazy, but who am I to argue...
    
    global testInitWasTriggered
    if testInitWasTriggered:
        # be extra sure that TestInit.clearDatabase() was called. It currently
        # blows everything away.
        
        # import here to prevent recursive import
        from WMQuality.TestInit import TestInit
        initObject = TestInit()
        initObject.clearDatabase( ignoreCaching = True )
    
    
