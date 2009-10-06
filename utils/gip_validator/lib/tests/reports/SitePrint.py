#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp

class SitePrint(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        """
Print out the following information for each site:
    - CE names
    - Close SE
    - VOView for each CE
        """
        ce_entries = []
        cese_entries = []
        se_entries = []
        vo_entries = []
        ce_se = {}
        ce_vo = {}
        for entry in self.entries:
            if 'GlueCE' in entry.objectClass:
                ce_entries.append(entry)
                ce_se[entry] = []
                ce_vo[entry] = []
            if 'GlueVOView' in entry.objectClass and 'GlueCETop' in entry.objectClass:
                vo_entries.append(entry)
            if 'GlueSE' in entry.objectClass:
                se_entries.append(entry)
            if 'GlueCESEBind' in entry.objectClass:
                cese_entries.append(entry)
        for entry in cese_entries:
            for entry2 in se_entries:
                if entry.glue['CESEBindSEUniqueID'] == entry2.glue['SEUniqueID']:
                    for entry3 in ce_entries:
                        if entry.glue['CESEBindCEUniqueID'] == entry3.glue['CEUniqueID']:
                            ce_se[entry3].append(entry2)
        for entry in vo_entries:
            for entry2 in ce_entries:
                desired_ck = 'GlueCEUniqueID=%s' % entry2.glue['CEUniqueID']
                if entry.glue['ChunkKey'] == desired_ck:
                    ce_vo[entry2].append(entry)

        for entry in ce_entries:
            out = '\t* CE: %s' % entry.glue['CEUniqueID']
            if len(ce_se) > 0:
                for se in ce_se[entry]:
                    out += ', Close SE: %s' % se.glue['SEUniqueID']
            out += '\n'
            for vo in ce_vo[entry]:
                out += '\t\t- VO: %s\n' % vo.glue['VOViewLocalID']

            self.appendMessage(MSG_INFO, out[:-1])

        test_result = {"site"       : site, 
                       "type"       : 'REPORTS', 
                       "name"       : 'SitePrint_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result
