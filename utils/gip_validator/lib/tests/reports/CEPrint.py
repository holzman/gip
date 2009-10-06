#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp


class CEPrint(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        """
        Print out the following information for each CE at a site:
          - LRMS and version
          - Free batch slots
          - Running jobs and waiting jobs
          - Total batch slots
          - Max wall clock time

        For each attached VO view, print:
          - VO
          - Running jobs
          - Waiting jobs
        """
        ce_entries = []
        vo_entries = []
        ce_vo = {}
        for entry in self.entries:
            if 'GlueCE' in entry.objectClass:
                ce_entries.append(entry)
                ce_vo[entry] = []
            if 'GlueVOView' in entry.objectClass and 'GlueCETop' in entry.objectClass:
                vo_entries.append(entry)

        for entry in vo_entries:
            for entry2 in ce_entries:
                desired_ck = 'GlueCEUniqueID=%s' % entry2.glue['CEUniqueID']
                if entry.glue['ChunkKey'] == desired_ck:
                    ce_vo[entry2].append(entry)

        for entry in ce_entries:
            out = '\t* CE: %s\n' % entry.glue['CEUniqueID']
            out += '\t\tLRMS type: %s, Version: %s\n' % (entry.glue['CEInfoLRMSType'], entry.glue['CEInfoLRMSVersion'])
            out += '\t\tSlots used %s, Free %s\n' % (entry.glue['CEStateFreeJobSlots'], entry.glue['CEStateRunningJobs'])
            out += '\t\tTotal batch slots: %s\n' % entry.glue['CEPolicyAssignedJobSlots']
            out += '\t\tMax wall time: %s\n' % entry.glue['CEPolicyMaxWallClockTime']
            for vo in ce_vo[entry]:
                out += '\t\t- VO: %s\n' % vo.glue['VOViewLocalID']
                out += '\t\t\tRunning %s, Waiting %s\n' % (vo.glue['CEStateRunningJobs'], vo.glue['CEStateWaitingJobs'])
            self.appendMessage(MSG_INFO, out[:-1])

        test_result = {"site"       : site, 
                       "type"       : 'REPORTS', 
                       "name"       : 'CEPrint_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result
