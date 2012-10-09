"""
WMCore.Database.MySQL.DumpAll - 
    Returns a dump of the current database. Used for testing.
    
Created by Andrew Melo <andrew.melo@gmail.com> on Sep 26, 2012
"""

from WMCore.Database.DBFormatter import DBFormatter

class DumpAll(DBFormatter):    

    def execute(self, subscription = None, conn = None, transaction = False):
        assert conn, "need a connection"
        # get tables
        sql = """SELECT table_name FROM information_schema.tables 
                 WHERE table_schema = (SELECT DATABASE())"""
        
        result = self.dbi.processData(sql, {}, conn = conn,
                                      transaction = transaction)
        tableResult = result[0].data
        tableList = []
        for row in tableResult:
            tableList.append( row[0] )
        
        # dump each table. I'm sure this could be done faster with binds, but
        # the tables are small and I couldn't get the syntax to work right
        dumpedValue = {}        
        for table in tableList:
            sql = """SELECT * FROM %s""" % table
            tableDump = self.dbi.processData( sql, {} , conn=conn,
                                              transaction = transaction )
            dumpedValue[table] = tableDump
        
        return dumpedValue
