#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_config import cp_get
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp, runlcginfo

class LCGInfoCEList(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.bdii_endpoint = cp_get(cp, "bdii", "osg_endpoint")
        self.opt = "--list-ce"
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def run(self, site_list):
        """
LCG Info Query

lcg-info --list-ce --vo ops --bdii is.grid.iu.edu:2170

Prints out a list of CE's configured for the following vo's:
MIS, OPS, CMS, and ATLAS.  .  NOTE: This test is not really intended for a site 
administrator other than to see if their site's CE is configured properly and is 
visible to VO's using the lcg tools.
        """
        vo_sep = "=" * 70
        vos = ['mis', 'ops', 'cms', 'atlas']
        for vo in vos:
            self.appendMessage(MSG_INFO,  "VO: %s\n%s\n%s" % (vo, vo_sep, self.getInfoForVO(vo)))

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
        pout = runlcginfo(self.opt, self.bdii_endpoint, vo).readlines()
        se_list = "CE:\n"
        for line in pout:
            if len(line.strip()) > 0:
                se_list += "%s\n" % line.split(":")[1].strip()
        return se_list
