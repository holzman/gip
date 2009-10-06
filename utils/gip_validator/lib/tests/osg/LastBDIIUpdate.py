#!/usr/bin/env python

import time
from lib.validator_base import Base
from lib.gip_ldap import read_bdii
from lib.validator_common import message, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp

class LastBDIIUpdate(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def run(self, site_list):
        results = []
        for site in site_list:
            results.append(self.main(site))
        return results

    def main(self, site):
        """
Last BDII Update Timestamp

This check reports the timestamp last reported to the BDII for each site.<br>
If it is more than 10 minutes old, the BDII update process is considered <br>
broken.
        """
        BDII_Timestamp = self.getLastUpdateFromBDII(site)
        Local_Timestamp = time.time()
        self.testTimestamps(BDII_Timestamp, Local_Timestamp, site)
        test_result = {"site"       : site, 
                       "type"       : 'OSG', 
                       "name"       : 'LastBDIIUpdate_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result
        
    def getLastUpdateFromBDII(self, site):
        entries = read_bdii(self.cp, query="(GlueLocationLocalID=TIMESTAMP)", base="mds-vo-name=%s,mds-vo-name=local,o=grid" % site)
        bdii_time = ""
        for entry in entries:
            # we are only interested in the first TIMESTAMP stanza
            bdii_time = entry.glue['LocationVersion']
            break

        return bdii_time
    
    def getLastUpdateFromMyOSG(self, site):
        pass
    
    def testTimestamps(self, bdii_time, local_time, site):
        TimeError = False
        try:
            bdii_time = float(bdii_time)
        except ValueError:
            TimeError = True
            msg = "BDII Timestamp error for site: %s\nBDII Timestamp: %s" % (site, str(bdii_time)) 
            self.appendMessage(MSG_CRITICAL, msg)
            bdii_time = 0

        try:
            local_time = float(local_time)
        except ValueError:
            TimeError = True
            msg = "Local Timestamp error for site: %s\nBDII Timestamp: %s" % (site, str(local_time)) 
            self.appendMessage(MSG_CRITICAL, msg)
            local_time = 0
            
        diff_minutes = abs((local_time - bdii_time) / 60)
        if TimeError:
            msg = "The BDII Timestamp and/or the Local Timestamp had an error" 
            self.appendMessage(MSG_CRITICAL, msg)
        elif diff_minutes > 30:
            msg = "The BDII Timestamp and the Local Timestamp differ by %s minutes" % str(diff_minutes) 
            self.appendMessage(MSG_CRITICAL, msg)
