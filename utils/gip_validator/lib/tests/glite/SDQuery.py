#!/usr/bin/env python

import re

from lib.validator_base import Base
from lib.validator_config import cp_get
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL, getBDIIParts
from lib.validator_common import passed, getTimestamp, runCommand

class SDQuery(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.bdii_endpoint = cp_get(cp, "bdii", "egee_endpoint")
        self.sitedns = [i.strip() for i in cp_get(self.cp, "validator", "domains", "").split(',')]
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def run(self, site_list):
        """
SD Query

glite-sd-query -e -t srm
        """
        pout = self.runquery()
        for site in self.sitedns:
            re_site = re.compile('([\s\S]*)' + site + '([\s\S]*)')
            m = re_site.match(pout)
            if m:
                self.appendMessage(MSG_INFO, "%s Passes" % site)
            else:
                self.appendMessage(MSG_CRITICAL, "%s Fails" % site)

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

    def runquery(self):
        parts = getBDIIParts(self.bdii_endpoint)
        cern_bdii = "%s:%s" % (parts["bdii"], parts["port"])
        command = "/bin/bash -c 'export LCG_GFAL_INFOSYS=%s; glite-sd-query -e -t srm'" % cern_bdii
        return runCommand(command)
