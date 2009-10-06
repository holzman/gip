#!/usr/bin/env python

import re
import types
import urlparse

from lib.gip_ldap import read_bdii, prettyDN
from lib.validator_config import cp_getBoolean, cp_get
from lib.validator_common import message, MSG_CRITICAL, MSG_INFO
from lib.validator_common import passed, getTimestamp
from lib.validator_base import Base

class ValidateGip(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.itb_grid = cp_getBoolean(self.cp, "validator", "itb", False)
        if self.itb_grid: 
            self.cp.set('bdii', 'endpoint', cp_get(self.cp, "bdii", "itb_endpoint"))
        else:
            self.cp.set('bdii', 'endpoint', cp_get(self.cp, "bdii", "osg_endpoint"))

        self.site_id = ""
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))
        
    def main(self, site):
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
        try:
            self.site_id = self.getSiteUniqueID(site)
        except:
            msg="Site: %s does not exist in the BDII."
            self.appendMessage(MSG_CRITICAL, msg % site)

        self.test_existence_all()
        self.test_egee_site_unique_id()
        self.test_ce()
        self.test_url()
        self.test_missing_newlines()
        self.test_missing_values()
        self.test_dn()
        self.test_srm()
        test_result = {"site"       : site, 
                       "type"       : 'OSG', 
                       "name"       : 'ValidateGIP_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result

    def test_missing_newlines(self):
        msg="Entry %s, key %s is missing the newline character."
        r = re.compile("Glue\w*:\s")
        for entry in self.entries:
            for key, val in entry.glue.items():
                if not isinstance(val, types.StringType):
                    continue
                m = r.search(val)
                if not m == None:
                    self.appendMessage(MSG_CRITICAL, msg % (prettyDN(entry.dn), key))

    def test_missing_values(self):
        msg="No value for entry %s, key %s."
        for entry in self.entries:
            for key, val in entry.glue.items():
                if val == "":
                    self.appendMessage(MSG_CRITICAL, msg % (prettyDN(entry.dn), key))

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
        # Currently disabling the defaultSE check, will enable once we have agreement on the appropriate setting 
        #self.test_value_not_equal("GlueCE", "CEInfoDefaultSE", "UNAVAILABLE")
        self.test_value_not_equal("GlueCE", "CEPolicyMaxCPUTime", "0", MSG_INFO)
        self.test_value_not_equal("GlueCE", "CEInfoTotalCPUs", "0", MSG_INFO)
        self.test_value_not_equal("GlueCE", "CEPolicyMaxWallClockTime", "0", MSG_INFO)
        #self.test_value_not_equal("GlueCE", "CEStateEstimatedResponseTime", "0")
        #self.test_value_not_equal("GlueCE", "CEStateWorstResponseTime", "0")

    def test_url(self):
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            if 'SiteWeb' not in entry.glue:
                msg = "No SiteWeb for: %s" % prettyDN(entry.dn)
                self.appendMessage(MSG_CRITICAL, msg)
                continue
            parts = urlparse.urlparse(entry.glue['SiteWeb'])
            if parts == "":
                msg = "Invalid website: %s" % entry.glue['SiteWeb']
                self.appendMessage(MSG_CRITICAL, msg)

    def test_sponsors(self):
        r = re.compile("(\w+):([0-9]+)")
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            try:
                m = r.findall(entry.glue['SiteSponsor'])
                if m == None:
                    msg="Invalid site sponsor: %s" % entry.glue['SiteSponsor']
                    self.appendMessage(MSG_CRITICAL, msg)
                    return
                tot = 0
                num_sponsors = 0
                for e in m:
                    num_sponsors += 1
                    tot += int(e[1])
                if num_sponsors == 1 and tot == 0:
                    tot = 100
                if tot != 100:
                    msg = "Site sponsorship does not add up to 100: %s" % entry.glue['SiteSponsor']
                    self.appendMessage(MSG_CRITICAL, msg)
            except KeyError:
                msg = "Site sponsorship does not exist"
                self.appendMessage(MSG_CRITICAL, msg)

    def test_existence(self, name, full=False, key_check="", orig_dn=""):
        for entry in self.entries:
            if full and entry.dn[0] == name: return
            if (not full) and entry.dn[0].startswith(name): return
        if len(orig_dn) > 0:
            msg = "(%s Check) GLUE Entity %s does not exist for %s." % (key_check, name, orig_dn)
        else:
            msg = "GLUE Entity %s does not exist." % name
        self.appendMessage(MSG_CRITICAL, msg)

    def test_chunk_keys(self):
        for entry in self.entries:
            if 'ChunkKey' not in entry.glue: continue
            self.test_existence(entry.glue['ChunkKey'], full=True, key_check="ChunkKey", orig_dn=prettyDN(entry.dn))

    def test_foreign_keys(self):
        for entry in self.entries:
            if 'ForeignKey' not in entry.glue: continue
            self.test_existence(entry.glue['ForeignKey'], full=True, key_check="ForeignKey", orig_dn=prettyDN(entry.dn))

    def test_egee_site_unique_id(self):
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            if ('SiteName' not in entry.glue) or ('SiteUniqueID' not in entry.glue):
                if 'SiteName' not in entry.glue:
                    msg = "No SiteName for %s" % prettyDN(entry.dn)
                    self.appendMessage(MSG_CRITICAL, msg)
                if 'SiteUniqueID' not in entry.glue:
                    msg = "No SiteUniqueID for %s" % prettyDN(entry.dn)
                    self.appendMessage(MSG_CRITICAL, msg)
                continue
            if entry.glue['SiteName'] != entry.glue['SiteUniqueID']:
                msg = "For EGEE compat., must have GlueSiteName == GlueSiteUniqueID"
                self.appendMessage(MSG_CRITICAL, msg)

    def test_value_not_equal(self, objClass, attribute, value, msg_type=MSG_CRITICAL):
        for entry in self.entries:
            if objClass not in entry.objectClass: continue
            if entry.glue[attribute] == value:
                msg = "GLUE attribute %s for entity %s in\n %s \n is equal to %s" % \
                    (attribute, objClass, prettyDN(entry.dn), value)
                self.appendMessage(msg_type, msg)

    def test_srm(self):
        valid_versions = ['1.1.0', '2.2.0']
        deprecated_versions = ['1.1', '2.2']
        fk_re = re.compile("GlueSiteUniqueID=(.*)")

        for entry in self.entries:
            if 'GlueService' not in entry.objectClass:
                continue

            if entry.glue['ServiceType'].lower().find('srm') < 0: continue
            if entry.glue['ServiceType'] != 'SRM':
                msg="ServiceType must be equal to 'SRM'"
                self.appendMessage(MSG_CRITICAL, msg)

            try:
                version = entry.glue['ServiceVersion']
            except KeyError:
                version = ""
            if not version in valid_versions:
                msg = "ServiceVersion must be one of %s." % valid_versions
                self.appendMessage(MSG_CRITICAL, msg)
                
            if version in deprecated_versions:
                msg = "Version string %s is deprecated." % version
                self.appendMessage(MSG_CRITICAL, msg)

            fk = entry.glue['ForeignKey']
            m = fk_re.match(fk)
            if m == None:
                self.appendMessage(MSG_CRITICAL, "Invalid GlueForeignKey.")

            site_unique_id = m.groups()[0]
            if site_unique_id != self.site_id:
                msg="Incorrect site unique ID for SRM service. %s != %s" % (site_unique_id, self.site_id)
                self.appendMessage(MSG_CRITICAL, msg)

            path = self.getPath(entry.glue['ServiceEndpoint'])
            if path.startswith("/srm/managerv"):
                if version.startswith('2'):
                    if path != '/srm/managerv2':
                        msg = 'SRM version 2 path must end with /srm/managerv2'
                        self.appendMessage(MSG_CRITICAL, msg)
                elif version.startswith('1'):
                    if path != '/srm/managerv1':
                        msg = 'SRM version 1 path must end with /srm/managerv1'
                        self.appendMessage(MSG_CRITICAL, msg)

    def getSiteUniqueID(self, site):
        """
        Determine the unique ID for this site.
        """
        site_entries = read_bdii(self.cp, query="(objectClass=GlueSite)", base="mds-vo-name=%s,mds-vo-name=local,o=grid" % site)
        
        if len(site_entries) > 1:
            msg = "Multiple GlueSite entries for site %s." % site
        elif len(site_entries) < 1:
            msg = "There are no GlueSite entries for site %s." % site
        if len(site_entries) != 1: self.appendMessage(MSG_CRITICAL, msg)

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
            if dn.pop() != "o=grid":
                msg = "DN %s does not end with o=grid" % fulldn
                self.appendMessage(MSG_CRITICAL, msg)
            if dn.pop().lower() != "mds-vo-name=local":
                msg = "DN %s does not end with mds-vo-name=local,o=grid" % fulldn
                self.appendMessage(MSG_CRITICAL, msg)
            for d in dn:
                if not d.find("o=grid") < 0:
                    msg = "There is an extra o=grid entry in DN %s" % fulldn
                    self.appendMessage(MSG_CRITICAL, msg)
                if d.startswith("mds-vo-name"):
                    msg = "There is an extra mds-vo-name entry in DN %s" % fulldn
                    self.appendMessage(MSG_CRITICAL, msg)

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
