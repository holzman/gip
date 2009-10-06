#!/usr/bin/env python

from lib.validator_base import Base
from lib.gip_ldap import query_bdii
from lib.validator_config import cp_getBoolean, cp_get
from lib.validator_common import message, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp

class MissingSites(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.itb_grid = cp_getBoolean(self.cp, "validator", "itb", False)
        if self.itb_grid: 
            self.cp.set('bdii', 'endpoint', cp_get(self.cp, "bdii", "itb_endpoint"))
        else:
            self.cp.set('bdii', 'endpoint', cp_get(self.cp, "bdii", "osg_endpoint"))

        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        """
Missing Sites Check

This check queries the BDII to determine if a site is missing
        """
        fd = query_bdii(self.cp, query="(objectClass=GlueCE)", base="mds-vo-name=%s,mds-vo-name=local,o=grid" % site)
        line = fd.readline().lower()
        if not line.startswith("dn:"):
            msg = "Missing - Check CEMon logs to see any errors and when the last time it reported."
            self.appendMessage(MSG_CRITICAL, msg)
            
        test_result = {"site"       : site, 
                       "type"       : 'OSG', 
                       "name"       : 'MissingSites_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result

    def run(self, site_list):
        results = []
        for site in site_list:
            results.append(self.main(site))
        return results
