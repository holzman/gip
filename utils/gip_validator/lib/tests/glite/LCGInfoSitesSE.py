#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_config import cp_get
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp, runlcginfosites

class LCGInfoSitesSE(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.bdii_endpoint = cp_get(cp, "bdii", "osg_endpoint")
        self.opts = 'se'
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def run(self, site_list):
        """
LCG InfoSites SE Query

lcg-infosites --is is.grid.iu.edu --vo ops se

Checks SE's, Type, Available Space (Kb), and Used Space (Kb) for the following 
vo's:  
    MIS, OPS, CMS, and ATLAS.  

NOTE: This test is not really intended for a site administrator other than to 
see if their site's SE is configured properly and is visible to VO's using the 
lcg tools.
        """
        vo_sep = "=" * 70
        vos = ['mis', 'ops', 'cms', 'atlas']
        for vo in vos:
            self.appendMessage(MSG_INFO, "VO: %s\n%s\n%s" % (vo, vo_sep, self.getInfoForVO(vo)))

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
        pout.pop(0) # Pop the separator line
        output = "VO: %s" % vo
        template = "%(se)-30s\t%(avail)-20s\t%(used)-20s\t%(type)-6s\n"
        header = {"se":"SE", \
                  "avail":"Available Space (Kb)", \
                  "used":"Used Space (Kb)", \
                  "type":"Type"}
        output += template % header
        for line in pout:
            items = line.split()
            record = {"avail" : items[0], "used" : items[1], "type" : items[2], "se" : items[3]}
            output += template % record

        output += "\n\n"
        return output
