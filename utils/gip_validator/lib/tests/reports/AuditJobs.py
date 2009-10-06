#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp

class AuditJobs(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        """
Audit Jobs

This is a report that displays the following information:
    * Computing Element / Queue
    * Stanza Source
    * State Waiting Jobs
    * State Running Jobs
    * State Total Jobs
    * State Free Job Slots
        """
        self.format_results(self.getJobInfo())
        test_result = {"site"       : site, 
                       "type"       : 'REPORTS', 
                       "name"       : 'AuditJobs_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result

    def getJobInfo(self):
        results_list = []
        for entry in self.entries:
            entry_details = {}
            dn = list(entry.dn)
            if dn[0].split("=")[0] == 'GlueCEUniqueID':
                entry_details["source"] = 'GlueCEUniqueID'
                entry_details["ce"] = dn[0].split("=")[1]
            elif dn[0].split("=")[0] == 'GlueVOViewLocalID':
                entry_details["source"] = 'GlueVOViewLocalID'
                entry_details["ce"] = dn[1].split("=")[1]
            else:
                continue

            entry_details["CEStateWaitingJobs"] = entry.glue["CEStateWaitingJobs"]
            entry_details["CEStateRunningJobs"] = entry.glue["CEStateRunningJobs"]
            entry_details["CEStateTotalJobs"] = entry.glue["CEStateTotalJobs"]
            entry_details["CEStateFreeJobSlots"] = entry.glue["CEStateFreeJobSlots"]
            results_list.append(entry_details)

        return results_list

    def format_results(self, results):
        template = "%(ce)-50s\t%(source)-20s\t%(CEStateWaitingJobs)-13s\t%(CEStateRunningJobs)-13s\t%(CEStateTotalJobs)-13s\t%(CEStateFreeJobSlots)-13s\n"
        header = {"ce":"CE", \
                  "source":"Source", \
                  "CEStateWaitingJobs":"Waiting Jobs", \
                  "CEStateRunningJobs":"Running Jobs", \
                  "CEStateTotalJobs":"Total Jobs", \
                  "CEStateFreeJobSlots":"Free JobSlots"}
        output = template % header
        for each in results:
            output += template % each
        self.appendMessage(MSG_INFO, output)
