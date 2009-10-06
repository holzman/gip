#!/usr/bin/env python
import os
import urllib2

from lib.validator_base import Base
from lib.validator_common import message, MSG_CRITICAL 
from lib.validator_common import getFQDNsBySiteName, addToPath, runCommand
from lib.validator_common import passed, getTimestamp, getTempFilename


slap_conf = """
include     /etc/openldap/schema/core.schema
include     $GIP_LOCATION/../schema/Glue-CORE.schema
include     $GIP_LOCATION/../schema/Glue-CE.schema
include     $GIP_LOCATION/../schema/Glue-CESEBind.schema
include     $GIP_LOCATION/../schema/Glue-SE.schema

database    bdb
suffix      "o=Grid"
"""

def createSlapConf():
    conffile = getTempFilename()
    fp = open(conffile, 'w')
    fp.write(os.path.expandvars(slap_conf))
    return conffile

slap_cmd = "slapadd -u -c -f %(slap_conf)s -l %(slap_info)s"

def makeSafe(site):
    return site.replace('-', '_')


class SchemaCheck(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.slap_conf = createSlapConf()
        self.info_file = getTempFilename()
        addToPath("/usr/sbin")
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        """
Test schema of site
        """
        fqdns = getFQDNsBySiteName(self.cp, site)
        for fqdn in fqdns:
            url = self.cp.get("validator", "schema_check_url") % (fqdn, site)
            fp2 = open(self.info_file, 'a')
            fp = urllib2.urlopen(url)
            ctr = 0
            for line in fp:
                ctr += 1
                if ctr < 3:
                    if line.find("Error Message") < 0:
                        self.appendMessage(MSG_CRITICAL, "Site %s not serving with CEMon." % site)
                fp2.write(line)
            fp2.close()
        stdin, fp3 = os.popen4(slap_cmd % {'slap_conf': self.slap_conf, 'slap_info': self.info_file})
        stdin.close()
        output = fp3.read()
        if not len(output) == 0:
            self.appendMessage(MSG_CRITICAL, "slapadd schema check failed for site %s.  Output:\n%s" % (site, output))

        test_result = {"site"       : site, 
                       "type"       : 'UTILITY', 
                       "name"       : 'SchemaCheck_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result
    
    def cleanup(self):
        runCommand("rm -f %s" % self.slap_conf)
        runCommand("rm -f %s" % self.info_file)
