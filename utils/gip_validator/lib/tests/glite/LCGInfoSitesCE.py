#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_config import cp_get
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp, runlcginfosites

class LCGInfoSitesCE(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.bdii_endpoint = cp_get(cp, "bdii", "osg_endpoint")
        self.opts = 'ce'
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def run(self, site_list):
        """
LCG InfoSites Query

lcg-infosites --is is.grid.iu.edu --vo ops ce

Checks CE, # CPU's, Free, Running Jobs, Waiting Jobs, Total Jobs for the 
following vo's:  
    MIS, OPS, CMS, and ATLAS.  

NOTE: This test is not really intended for a site administrator other than to 
see if their site's CE is configured properly and is visible to VO's using the 
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

        pout.pop(0) # Pop Some stupid comment about bdii
        pout.pop(0) # Pop the header
        pout.pop(0) # Pop the separator line

        template = "%(ce)-56s\t%(cpus)-5s\t%(free)-15s\t%(waiting)-15s\t%(running)-15s\t%(total)-15s\n"
        header = {"cpus" : "CPU's", \
                  "free" : "Free", \
                  "total" : "Total Jobs",
                  "running" : "Running Jobs",
                  "waiting" : "Waiting jobs",
                  "ce" : "CE"}

        output = "VO: %s\n" % vo
        output += template % header
        for line in pout:
            items = line.split()
            record = {"cpus"  : items[0],
                      "free"  : items[1],
                      "total" : items[2],
                      "running" : items[3],
                      "waiting" : items[4],
                      "ce" : items[5]
                     }
            output += template % record
        output += "\n\n"
        return output
