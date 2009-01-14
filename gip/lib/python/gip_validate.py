#!/usr/bin/env python

import os
import re
import sys
import types
import urllib2
import urlparse

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
import GipUnittest
from gip_ldap import query_bdii, read_ldap, read_bdii, prettyDN
from gip_common import cp_get, getFQDNBySiteName
from gip_testing import getTestConfig, runTest

class GipValidate(GipUnittest.GipTestCase):
    def __init__(self, site, cp):
        GipUnittest.GipTestCase.__init__(self, 'testGipOutput')
        self.type = cp_get(self.cp, "gip_tests", "validate_type", "")
        self.name = 'testGipOutput_%s' % site
        self.site = site
        self.entries = None
        self.site_id = None

    def testGipOutput(self):
        """
GIP Validator
This check tests the ldif as reported by cemon for:

    * The following stanzas mustappear at least once:
        o GlueCEUniqueID
        o GlueVOViewLocalID
        o GlueSubClusterUniqueID
        o GlueClusterUniqueID
        o GlueCESEBindSEUniqueID
        o GlueCESEBindGroupCEUniqueID
        o GlueLocationLocalID
        o GlueServiceUniqueID
        o GlueSEUniqueID
        o GlueSEAccessProtocolLocalID
        o GlueSEControlProtocolLocalID
        o GlueSALocalID
    * The GlueSiteUniqueID and GlueSiteName must be the same for EGEE compatibility
    * The CE Stanza for the following conditions:
        o CEInfoDefaultSE != UNAVAILABLE
        o CEPolicyMaxCPUTime != 0
        o CEInfoTotalCPUs != 0
        o CEStateEstimatedResponseTime != 0
        o CEStateWorstResponseTime != 0
        o CEPolicyMaxWallClockTime != 0
    * The ldiff must have newlines appended after every key value combination
    * The all foriegn keys and chunk keys must have corresponding stanzas
    * All entries must conform to the attribute:value format
    * Test SRM ads for the following:
        o endpoint type is SRM
        o Version is 1.1.0 or 2.2.0 (1.1 or 2.2 generate warnings)
        o Site unique ID is not blank
        o Site unique ID is actual unique ID used for this site.
        o If dCache, make sure that the /srm/managervX string is correct.
    * Test DN ads for the following:
        o o=grid appears once
        o mds-vo-name=local appears once
        o mds-vo-name=<site> appears once
        o they appear in the correct order
        """
        if self.type.lower() == "url": # we want info from the web status page
            fqdn = getFQDNBySiteName(self.cp, self.site)
            url = cp_get(self.cp, "gip_tests", "validator_url", "") % (fqdn, self.site)
            fd = urllib2.urlopen(url)
#            line = fd.readline()
#            if "html" in fd.readline():
#                self.assertEquals(0, 1, "CEMon not reporting for site %s" % self.site)
#            fd = urllib2.urlopen(url)
        elif self.type.lower() == "gipinfo": # We want info from the gip_info script
            path = os.path.expandvars("$GIP_LOCATION/bin/gip_info")
            fd = os.popen(path)
        else: # assume we want to read from the bdii
            fd = query_bdii(self.cp, query="", base="mds-vo-name=%s,mds-vo-name=local,o=grid" % self.site)

        self.entries = read_ldap(fd)
        self.site_id = self.getSiteUniqueID()

        self.test_existence_all()
        self.test_egee_site_unique_id()
        self.test_ce()
        self.test_site()
        self.test_missing_newlines()
        self.test_missing_values()
        self.test_dn()
        self.test_srm()

    def test_missing_newlines(self):
        r = re.compile("Glue\w*:\s")
        for entry in self.entries:
            for key, val in entry.glue.items():
                if not isinstance(val, types.StringType):
                    continue
                m = r.search(val)
                self.expectEquals(m, None, msg="Entry %s, key %s is missing the newline character." % (prettyDN(entry.dn), key))

    def test_missing_values(self):
        for entry in self.entries:
            for key, val in entry.glue.items():
                self.expectNotEquals(val, "", msg="No value for entry %s, key %s." % (prettyDN(entry.dn), key))

    def test_existence_all(self):
        self.test_existence("GlueCEUniqueID")
        self.test_existence("GlueVOViewLocalID")
        self.test_existence("GlueSubClusterUniqueID")
        self.test_existence("GlueClusterUniqueID")
        self.test_existence("GlueCESEBindSEUniqueID")
        self.test_existence("GlueCESEBindGroupCEUniqueID")
        self.test_existence("GlueLocationLocalID")
        self.test_existence("GlueServiceUniqueID")
        self.test_existence("GlueSEUniqueID")
        self.test_existence("GlueSEAccessProtocolLocalID")
        # need to check to see if there are any SE's other than the classic SE
        #  because the classic SE will not have a GlueSEControlProtocolLocalID
        #  stanza
        if self.nonClassicSeExist(): self.test_existence("GlueSEControlProtocolLocalID")
        self.test_existence("GlueSALocalID")
        self.test_chunk_keys()
        self.test_foreign_keys()

    def test_ce(self):
        self.test_value_not_equal("GlueCE", "CEInfoDefaultSE", "UNAVAILABLE")
        self.test_value_not_equal("GlueCE", "CEPolicyMaxCPUTime", "0")
        self.test_value_not_equal("GlueCE", "CEInfoTotalCPUs", "0")
        self.test_value_not_equal("GlueCE", "CEPolicyMaxWallClockTime", "0")
        #self.test_value_not_equal("GlueCE", "CEStateEstimatedResponseTime", "0")
        #self.test_value_not_equal("GlueCE", "CEStateWorstResponseTime", "0")

    def test_site(self):
        self.test_url()

    def test_url(self):
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            if 'SiteWeb' not in entry.glue:
                parts = ''
                self.assertNotEquals(parts, '', msg="No SiteWeb for: %s" % prettyDN(entry.dn))
                continue
            parts = urlparse.urlparse(entry.glue['SiteWeb'])
            self.expectNotEquals(parts, '', msg="Invalid website: %s" % entry.glue['SiteWeb'])

    def test_sponsors(self):
        r = re.compile("(\w+):([0-9]+)")
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            try:
                m = r.findall(entry.glue['SiteSponsor'])
                self.assertNotEquals(m, None, msg="Invalid site sponsor: %s" % entry.glue['SiteSponsor'])
                tot = 0
                num_sponsors = 0
                for e in m:
                    num_sponsors += 1
                    tot += int(e[1])
                if num_sponsors == 1 and tot == 0:
                    tot = 100
                self.expectNotEquals(tot, 100, msg="Site sponsorship does not add up to 100: %s" % entry.glue['SiteSponsor'])
            except:
                self.expectNotEquals('', '', msg="Site sponsorship does not exist")

    def test_existence(self, name, full=False, key_check="", orig_dn=""):
        for entry in self.entries:
            if full and entry.dn[0] == name:
                return
            if (not full) and entry.dn[0].startswith(name):
                return
        if len(orig_dn) > 0:
            self.expectEquals(0, 1, msg="(%s Check) GLUE Entity %s does not exist for %s." % (key_check, name, orig_dn))
        else:
            self.expectEquals(0, 1, msg="GLUE Entity %s does not exist." % name)


    def test_chunk_keys(self):
        for entry in self.entries:
            if 'ChunkKey' not in entry.glue:
                continue
            self.test_existence(entry.glue['ChunkKey'], full=True, key_check="ChunkKey", orig_dn=prettyDN(entry.dn))

    def test_foreign_keys(self):
        for entry in self.entries:
            if 'ForeignKey' not in entry.glue:
                continue
            self.test_existence(entry.glue['ForeignKey'], full=True, key_check="ForeignKey", orig_dn=prettyDN(entry.dn))

    def test_egee_site_unique_id(self):
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            if ('SiteName' not in entry.glue) or ('SiteUniqueID' not in entry.glue):
                if 'SiteName' not in entry.glue:
                    self.expectEquals(0, 1, msg="No SiteName for %s" % prettyDN(entry.dn))
                if 'SiteUniqueID' not in entry.glue:
                    self.expectEquals(0, 1, msg="No SiteUniqueID for %s" % prettyDN(entry.dn))
                continue
            self.expectEquals(entry.glue['SiteName'], entry.glue['SiteUniqueID'], \
                msg="For EGEE compat., must have GlueSiteName == GlueSiteUniqueID")

    def test_value_not_equal(self, objClass, attribute, value):
        for entry in self.entries:
            if objClass not in entry.objectClass:
                continue
            self.expectNotEquals(entry.glue[attribute], value, msg="GLUE attribute %s for entity %s in\n %s \n is equal to %s" % (attribute, objClass, prettyDN(entry.dn), value))

    def test_srm(self):
        valid_versions = ['1.1.0', '2.2.0']
        deprecated_versions = ['1.1', '2.2']
        fk_re = re.compile("GlueSiteUniqueID=(.*)")

        for entry in self.entries:
            if 'GlueService' not in entry.objectClass:
                continue

            if entry.glue['ServiceType'].lower().find('srm') < 0:
                continue
            self.expectEquals(entry.glue['ServiceType'], 'SRM', msg="ServiceType must be equal to 'SRM'")

            try:
                version = entry.glue['ServiceVersion']
            except:
                version = ""
            self.expectTrue(version in valid_versions, msg="ServiceVersion must be one of %s." % valid_versions)
            if version in deprecated_versions:
                self.expectEquals(0, 1, msg="Version string %s is deprecated." % version)

            fk = entry.glue['ForeignKey']
            m = fk_re.match(fk)
            self.expectNotEquals(m, None, msg="Invalid GlueForeignKey.")

            site_unique_id = m.groups()[0]
            self.expectEquals(site_unique_id, self.site_id, msg="Incorrect site unique ID for service.")

            path = self.getPath(entry.glue['ServiceEndpoint'])
            if path.startswith("/srm/managerv"):
                if version.startswith('2'):
                    self.expectEquals(path, '/srm/managerv2', msg='SRM version 2 path must end with /srm/managerv2')
                elif version.startswith('1'):
                    self.expectEquals(path, '/srm/managerv1', msg='SRM version 1 path must end with /srm/managerv1')

    def getSiteUniqueID(self):
        """
        Determine the unique ID for this site.
        """
        site_entries = read_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local,o=grid" % self.site, query="(objectClass=GlueSite)")
        self.expectEquals(len(site_entries), 1, msg="Multiple GlueSite entries for site %s." % self.site)
        return site_entries[0].glue['SiteUniqueID']

    def getPath(self, surl):
        """
        Given a SRM SURL, determine the path of the SRM endpoint
        (i.e., for dCache, this is /srm/managervX)
        """
        surl = surl.replace("srm://", "https://").replace("httpg://", "https://")
        parts = urlparse.urlparse(surl)

        return parts[2]

    def test_dn(self):
        for entry in self.entries:
            dn = list(entry.dn)
            fulldn = prettyDN(entry.dn)
            self.expectEquals(dn.pop(), "o=grid", msg="DN %s does not end with o=grid" % fulldn)
            self.expectEquals(dn.pop().lower(), "mds-vo-name=local", msg="DN %s does not end with mds-vo-name=local,o=grid" % fulldn)
            if (self.type.lower() == "url"): # the gip_info does not have the mds-vo-name=site
                self.expectEquals(dn.pop().lower(), ("mds-vo-name=%s" % self.site).lower(), msg="DN %s does not end with mds-vo-name=%s,mds-vo-name=local,o=grid" % (fulldn, self.site))
            for d in dn:
                self.expectTrue(d.find("o=grid") < 0, msg="There is an extra o=grid entry in DN %s" % fulldn)
                self.expectTrue(d.startswith("mds-vo-name") == False, "There is an extra mds-vo-name entry in DN %s" % fulldn)

    def nonClassicSeExist(self):
        result = False
        se_names = self.getSENames()
        for name in se_names:
            if not ("classic" in name.lower()):
                result = True
                break
        return result
        
    def getSENames(self):
        se_names = []
        for entry in self.entries:
            dn = list(entry.dn)
            stanza_type = dn[0].split("=")[0]
            if stanza_type == "GlueSEUniqueID":
                se_names.append(str(entry.glue['SEName']))
        return se_names
    
def main(args):
    """
    The main entry point for when gip_validate is run in standalone mode.
    """
    cp = getTestConfig(args)
    try:
        type = args[1]
        if type == "url":
            args.pop(1)
            cp.set("gip_tests", "validate_type", "url")
        elif type == "gipinfo":
            args.pop(1)
            cp.set("gip_tests", "validate_type", "gipinfo")
        else:
            cp.set("gip_tests", "validate_type", "bdii")
    except:
        cp.set("gip_tests", "validate_type", "bdii")

    runTest(cp, GipValidate)

if __name__ == '__main__':
    main(sys.argv)
