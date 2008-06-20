import re
import types
import urlparse
from gip_ldap import getSiteList, prettyDN

class GipValidate:
    def __init__(self, entries):
        self.entries = entries
        self.results = []

    def run(self):
        self.test_existence_all()
        self.test_egee_site_unique_id()
        self.test_ce()
        self.test_site()
        self.test_missing_newlines()
        return self.results

    def test_missing_newlines(self):

        r = re.compile("Glue\w*:\s")
        for entry in self.entries:
            for key, val in entry.glue.items():
                if not isinstance(val, types.StringType):
                    continue
                m = r.search(val)
                if m:
                    self.results.append("Entry %s, key %s is missing the newline character." % (prettyDN(entry.dn), key))

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
        self.test_existence("GlueSEControlProtocolLocalID")
        self.test_existence("GlueSALocalID")

    def test_ce(self):
        self.test_value_not_equal("GlueCE", "CEInfoDefaultSE", "UNAVAILABLE")
        self.test_value_not_equal("GlueCE", "CEPolicyMaxCPUTime", "0")
        self.test_value_not_equal("GlueCE", "CEInfoTotalCPUs", "0")
        self.test_value_not_equal("GlueCE", "CEStateEstimatedResponseTime", "0")
        self.test_value_not_equal("GlueCE", "CEStateWorstResponseTime", "0")

    def test_site(self):
        self.test_url()
        self.test_sponsors()

    def test_url(self):
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            parts = urlparse.urlparse(entry.glue['SiteWeb'])
            if parts == '':
                self.results.append("Invalid website: %s" % entry.glue['SiteWeb'])

    def test_sponsors(self):
        r = re.compile("(\w+):([0-9]+)")
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            m = r.findall(entry.glue['SiteSponsor'])
            if not m:
                self.results.append("Invalid site sponsor: %s" % entry.glue['SiteSponsor'])
            tot = 0
            for e in m:
                tot += int(e[1])
            if tot != 100:
                self.results.append("Site sponsorship does not add up to 100: %s" % entry.glue['SiteSponsor'])

    def test_existence(self, name, full=False):
        for entry in self.entries:
            if full and entry.dn[0] == name:
                return
            if (not full) and entry.dn[0].startswith(name):
                return
        self.results.append("GLUE Entity %s does not exist." % name)

    def test_chunk_keys(self):
        for entry in self.entries:
            if 'ChunkKey' not in entry.glue:
                continue
            self.test_existence(entry.glue['ChunkKey'], full=True)

    def test_egee_site_unique_id(self):
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            if entry.glue['SiteName'] != entry.glue['SiteUniqueID']:
                self.results.append("For EGEE compat., must have GlueSiteName == GlueSiteUniqueID")

    def test_value_not_equal(self, objClass, attribute, value):
        for entry in self.entries:
            if objClass not in entry.objectClass:
                continue
            if entry.glue[attribute] == value:
                self.results.append("GLUE attribute %s for entity %s is equal to %s" % (attribute, objClass, value))
