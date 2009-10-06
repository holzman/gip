#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_common import message, MSG_INFO, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp
from lib.validator_common import getFQDNsBySiteName, getUrlFd
from lib.validator_config import cp_get, cp_getBoolean
from lib.gip_ldap import read_bdii, read_ldap, prettyDN

# Globals
CR = '\r'
LF = '\n'
html_lf = '<br>'

class BDII_vs_CEMon(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)

        self.itb_grid = cp_getBoolean(self.cp, "validator", "itb", False)
        if self.itb_grid: 
            endpoint = cp_get(self.cp, "bdii", "itb_endpoint")
            self.url = "http://is-itb.grid.iu.edu/data/cemon_transitory/%(fqdn)s"
        else:
            self.url = "http://is.grid.iu.edu/data/cemon_transitory/%(fqdn)s"
            endpoint = cp_get(self.cp, "bdii", "osg_endpoint")

        endpoint_parts = endpoint.split(":") 
        self.bdii = endpoint_parts[1]
        self.port = endpoint_parts[2]

        self.excludes = cp_get(self.cp, "gip_tests", "compare_excludes", "")
        self.excludes = [i.strip() for i in self.excludes.split(',')] 

        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        bdii_entries = read_bdii(self.cp, query="", base='mds-vo-name=%s,mds-vo-name=local,o=grid' % site)
        cemon_entries = []
        site_fqdns = getFQDNsBySiteName(self.cp, site)
        for fqdn in site_fqdns: 
            self.url = self.url % {"fqdn" : fqdn}
            cemon_entries.extend(read_ldap(getUrlFd(self.url)))
        # perform testing
        self.compare(bdii_entries, cemon_entries)
        test_result = {"site"       : site, 
                       "type"       : 'UTILITY', 
                       "name"       : 'MissingSites_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result
    
    def run(self, site_list):
        results = []
        for site in site_list:
            results.append(self.main(site))
        return results
    
    def compare(self, bdii_entries, cemon_entries):
        bad_entries = list(set(bdii_entries).symmetric_difference(set(cemon_entries)))
        filtered_entries = []
        
        for entry in bad_entries:
            if not entry.objectClass == ('GlueTop', ):
                filtered_entries.append(entry)
        bad_entries = filtered_entries
        
        msg = 'The following entries %i (out of %i) are inconsistent between BDII and CEMon:\n' % (len(bad_entries), len(bdii_entries) + len(cemon_entries))
        dns = []
        for entry in bad_entries:
            dn = prettyDN(entry.dn) + ' (in CEMon %s; in BDII %s)' % (entry in cemon_entries, entry in bdii_entries)
            dn += '\n%s' % entry
            dns.append(dn)
        
        dns.sort()
        for entry in dns:
            msg += entry + '\n'
        
        self.appendMessage(MSG_INFO, msg)
        
