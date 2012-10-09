"""
_GetCurrentDatabase_

Used for testing

"""

from WMCore.Database.DBFormatter import DBFormatter

class GetCurrentDatabase(DBFormatter):    

    def execute(self, subscription = None, conn = None, transaction = False):
        transaction = False
        sql = """SELECT DATABASE()"""

        result = self.dbi.processData(sql, {}, conn = conn,
                                      transaction = transaction)
        
        print "--result of select database %s" % result[0].fetchall()
        
        self.dbi.processData("CREATE TABLE IF NOT EXISTS haha_2 ( id INT(11) NOT NULL )", {}, conn = conn,
                                      transaction = transaction)

        self.dbi.processData("DROP TABLE IF EXISTS haha_2", {}, conn = conn,
                                      transaction = transaction)

        result = self.dbi.processData(sql, {}, conn = conn,
                                      transaction = transaction)
        
        return result[0].fetchall()
