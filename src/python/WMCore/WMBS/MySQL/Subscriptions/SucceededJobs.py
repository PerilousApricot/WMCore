#!/usr/bin/env python
"""
_SucceededJobs_

MySQL implementation of Subscriptions.SucceededJobs
"""

__all__ = []
__revision__ = "$Id: SucceededJobs.py,v 1.1 2010/06/01 21:09:17 riahi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class SucceededJobs(DBFormatter):
    sql = """SELECT id FROM wmbs_job WHERE outcome = 1 and jobgroup IN
             (SELECT id FROM wmbs_jobgroup WHERE subscription = :subscription)"""

    def format(self, result):
        results = DBFormatter.format(self, result)

        subIDs = []
        for row in results:
            subIDs.append(row[0])

        return subIDs

    def execute(self, subscription = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"subscription": subscription},
                         conn = conn, transaction = transaction)
        return self.format(result)

