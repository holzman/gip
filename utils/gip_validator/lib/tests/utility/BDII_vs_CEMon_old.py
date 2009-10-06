#!/usr/bin/env python

from lib.validator_base import Base
from lib.validator_common import message, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp
from lib.validator_common import getFQDNsBySiteName, getUrlFd
from lib.validator_config import cp_get, cp_getBoolean
from lib.gip_ldap import read_bdii, read_ldap

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
        for entry in bdii_entries:
            found_entry = False
            for entry2 in cemon_entries:
                if entry.dn[0] == entry2.dn[0]:
                    found_entry = True
                    self.compare_lists(entry.objectClass, entry2.objectClass, "objectClass")
                    self.compare_lists(entry.glue, entry2.glue, "glue")
                    self.compare_lists(entry.nonglue, entry2.nonglue, "non-glue")
                else:
                    continue
            if not found_entry:
                self.appendMessage(MSG_CRITICAL, "%s stanza not found in CEMon entries" % entry.dn[0])

    def compare_lists(self, bdii_list, cemon_list, list_name=""):
        diffList_cemon = [x for x in cemon_list if x not in bdii_list]
        diffList_bdii = [x for x in bdii_list if x not in cemon_list]
        
        for item in diffList_cemon:
            self.appendMessage(MSG_CRITICAL, "BDII %s list is missing entry %s" % (list_name, str(item)))
        
        for item in diffList_bdii:
            self.appendMessage(MSG_CRITICAL, "CEMon %s list is missing entry %s" % (list_name, str(item)))
        
    def compare_dicts(self, bdii_dict, cemon_dict, dict_name=""):
        bdii_set = set(bdii_dict)
        cemon_set = set(cemon_dict)
        
        # items that bdii_set contains that cmeon_set does not
        for item in bdii_set.difference(cemon_set):
            self.appendMessage(MSG_CRITICAL, "CEMon %s dictionary is missing entry %s" % (dict_name, str(item)))
        
        # items that cemon_set contains that bdii_set does not
        for item in cemon_set.difference(bdii_set):
            self.appendMessage(MSG_CRITICAL, "BDII %s dictionary is missing entry %s" % (dict_name, str(item)))
