#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp

class OSGVersions(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        self.format_results(self.getJobInfo())
        test_result = {"site"       : site, 
                       "type"       : 'REPORTS', 
                       "name"       : 'OSGVersions_%s' % site, 
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
            dn = list(entry.dn)
            if not dn[0].split("=")[0] == 'GlueLocationLocalID':
                continue
            elif dn[0].split("=")[0].endswith("_VERSION"):
                clusterID = dn[2].split("=")[1]
                version_type = entry.glue["GlueLocationLocalID"]
                found = self.searchResultsList(results_list, clusterID, version_type)
                if found is None:
                    version = {"cluster" : clusterID,
                               "type" : version_type,
                               "timestamp" : entry.glue["GlueLocationPath"],
                               "version" : entry.glue["GlueLocationVersion"]}
                    results_list.append(version)

        return results_list

    def searchResultsList(self, results_list, clusterID, version_type):
        for item in results_list:
            if item["cluster"] == clusterID and item["type"] == version_type:
                return item
        return None

    def format_results(self, results):
        template = "%(cluster)-50s\t%(type)-20s\t%(version)-13s\t%(timestamp)-13s\n"
        output = template % {"cluster": "Cluster ID", "type": "Version Type", "version": "Version", "timestamp": "Timestamp"}
        for each in results:
            output += template % each
        self.appendMessage(MSG_INFO, output)
