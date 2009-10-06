#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_config import cp_get
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp, runlcginfosites

class LcgInfoSitesCloseSE(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.bdii_endpoint = cp_get(cp, "bdii", "osg_endpoint")
        self.opts = "closeSE"
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def run(self, site_list):
        """
LCG InfoSites Close SE Query

lcg-infosites --is is.grid.iu.edu --vo ops closeSE

Checks the BDII for close SE for the following vo's:  
    MIS, OPS, CMS, and ATLAS.  
    
NOTE: This test is not really intended for a site administrator other than to 
see if their site's CE and SE is configured properly and is visible to VO's 
using the lcg tools.
        """
        vos = ['mis', 'ops', 'cms', 'atlas']
        for vo in vos:
            self.appendMessage(MSG_INFO,  self.getInfoForVO(vo))

        test_result = {"site"       : "GLITE", 
                       "type"       : 'GLITE', 
                       "name"       : 'LCGInfoCEList_GLITE',
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return [test_result,]

    def getInfoForVO(self, vo):
        pout = runlcginfosites(self.bdii_endpoint, vo, self.opts).readlines()
        pout.pop(0) # Pop the header
        ce = " "
        template = "%-30s\t%-30s\n"
        output = "\n" + template % ("CE", "SE")
        first = True
        for line in pout:
            if line.startswith("Name"):
                first = True
                ce = line.split(":")[1].strip()
            else:
                if first: first = False
                else: ce = " "
                se = line.strip()
                if len(se) > 0: output += template % (ce, se)
        return output
