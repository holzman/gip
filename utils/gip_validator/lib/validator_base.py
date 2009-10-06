'''
Created on Sep 15, 2009

@author: tiradani
'''
import os
from lib.gip_ldap import query_bdii, read_ldap

class Base:
    def __init__(self, cp, query_source="gip_info"):
        self.cp = cp
        self.type = query_source
        self.entries = ""

    def load_entries(self, site):
        if self.type.lower() == "gipinfo": # We want info from the gip_info script
            path = os.path.expandvars("$GIP_LOCATION/bin/gip_info")
            fd = os.popen(path)
        else: # assume we want to read from the bdii
            fd = query_bdii(self.cp, query="", base="mds-vo-name=%s,mds-vo-name=local,o=grid" % site)

        return read_ldap(fd)

    def main(self, site):
        test_result = {"site":site, "name": "", "messages":[], "result":"", "timestamp":""}
        return test_result
    
    def run(self, site_list):
        results = []
        for site in site_list:
            self.entries = self.load_entries(site)
            results.append(self.main(site))
        return results
