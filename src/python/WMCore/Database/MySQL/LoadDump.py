"""
WMCore.Database.MySQL.LoadDump - 
    Given a dump (produced by DumpAll), set the database to the previous state
    
    0) Disable FK checking
    1) Delete any tables that didn't exist previously
    2) Check every previous table still exists
        NOTE: I assume nobody does stuff like ALTER TABLE. I didn't see that in
              the code, so it should be okay
    3) Delete all rows out of all tables
    4) Insert rows from the dump
    5) Reenable FK checking
    
     Used for testing.
    
Created by Andrew Melo <andrew.melo@gmail.com> on Sep 26, 2012
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadDump(DBFormatter):    

    def execute(self, dump, conn = None, transaction = False):
        success = True
        try:
            # get tables
            sql = """SELECT table_name FROM information_schema.tables 
                     WHERE table_schema = (SELECT DATABASE())"""
            
            result = self.dbi.processData(sql, conn = conn,
                                               transaction = transaction)
            tableResult = result[0].data
            tableList = []
            for row in tableResult:
                tableList.append( row[0] )
            
            # disable FK checking
            self.dbi.processData("SET foreign_key_checks = 0", conn=conn,
                                                      transaction = transaction)
            
            # Delete each table not in the dump
            for table in tableList:
                if table not in dump:
                    sql = """DROP TABLE %s""" % table
                    self.dbi.processData( sql, conn=conn,
                                               transaction = transaction )
                    
            # make sure every table in the dump is still in the database
            for table in dump:
                if table not in tableList:
                    # we're short a table that should still be there
                    success = False
                    return False
                    
            # Delete all the rows in the tables left
            for table in tableList:
                if table in dump:
                    sql = """DELETE FROM %s""" % table
                    self.dbi.processData( sql, conn=conn,
                                               transaction = transaction )
                
            for table in dump:
                if not dump[table][0].fetchall():
                    # table started out empty
                    continue
                
                currentData = dump[table][0].fetchall()
                currentKeys = dump[table][0].fetchkeys()
                
                # generate column list
                columns    = ", ".join( currentKeys )
                bindList   = [ ":" + s for s in currentKeys]
                bindString = ", ".join( bindList )  
                sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, columns, bindString)  
                self.dbi.processData( sql, currentData, conn=conn,
                                      transaction = transaction )
        except:
            success = False
            raise
        finally:
            self.dbi.processData("SET foreign_key_checks = 1", conn=conn,
                                                      transaction = transaction)

        
        return success
