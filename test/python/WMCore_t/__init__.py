import WMQuality.DatabaseCache

def setup_package():
    WMQuality.DatabaseCache.enterPackage( )
    
def teardown_package():
    WMQuality.DatabaseCache.leavePackage( )