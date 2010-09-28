#!/usr/bin/env python
################################################################################
# GIP Validator
# Author:       Brian Bockelman, Anthony Tiradani
# Notes:        All GIP dependencies have been added to this one script to
#               allow for maximum portability.  (hence the shear size of it)
################################################################################

import os
import re
import sys
import types
import urlparse
import optparse
import libxml2
import urllib
import time
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

################################################################################
# Constants
################################################################################
MSG_INFO = "INFO"
MSG_CRITICAL = "CRIT"
MSG_UNKNOWN = "UNKNOWN"
osg_endpoint = "ldap://is.grid.iu.edu:2170"
itb_endpoint = "ldap://is-itb.grid.iu.edu:2170"
egee_endpoint = "ldap://lcg-bdii.cern.ch:2170"
pps_endpoint = "ldap://pps-bdii.cern.ch:2170"

################################################################################
# GIP LDAP Module
################################################################################
# True if the current version of Python is 2.3 or higher
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
if not py23:
    from sets24 import Set
    from sets24 import _TemporarilyImmutableSet
else:
    from sets import Set
    from sets import _TemporarilyImmutableSet

class _hdict(dict): 
    def __hash__(self):
        items = self.items()
        items.sort()
        return hash(tuple(items))

class LdapData:
    glue = {}
    nonglue = {}
    objectClass = []
    dn = []

    def __init__(self, data, multi=False):
        self.ldif = data
        glue = {}
        nonglue = {}
        objectClass = []
        for line in self.ldif.split('\n'):
            if line.startswith('dn: '):
                dn = line[4:].split(',')
                dn = [i.strip() for i in dn]
                continue
            try:
                p = line.split(': ', 1)
                attr = p[0]
                try:
                    val = p[1]
                except KeyError:
                    val = ""
            except:
                #print >> sys.stderr, line.strip()
                raise
            val = val.strip()
            if attr.startswith('Glue'):
                if attr == 'GlueSiteLocation':
                    val = tuple([i.strip() for i in val.split(',')])
                if multi and attr[4:] in glue:
                    glue[attr[4:]].append(val)
                elif multi:
                    glue[attr[4:]] = [val]
                else:
                    glue[attr[4:]] = val
            elif attr == 'objectClass':
                objectClass.append(val)
            elif attr.lower() == 'mds-vo-name':
                continue
            else:
                if multi and attr in nonglue:
                    nonglue[attr].append(val)
                elif multi:
                    nonglue[attr] = [val]
                else:
                    nonglue[attr] = val
        objectClass.sort()
        self.objectClass = tuple(objectClass)
        try:
            self.dn = tuple(dn)
        except:
            #print >> sys.stderr, "Invalid GLUE:\n%s" % data
            raise
        for entry in glue:
            if multi:
                glue[entry] = tuple(glue[entry])
        for entry in nonglue:
            if multi:
                nonglue[entry] = tuple(nonglue[entry])
        self.nonglue = _hdict(nonglue)
        self.glue = _hdict(glue)
        self.multi = multi

    def to_ldif(self):
        """
        Convert the LdapData back into LDIF.
        """
        ldif = 'dn: ' + ','.join(self.dn) + '\n'
        for obj in self.objectClass:
            ldif += 'objectClass: %s\n' % obj
        for entry, values in self.glue.items():
            if entry == 'SiteLocation':
                if self.multi:
                    for value in values:
                        ldif += 'GlueSiteLocation: %s\n' % \
                            ', '.join(list(value))
                else:
                    ldif += 'GlueSiteLocation: %s\n' % \
                        ', '.join(list(values))
            elif not self.multi:
                ldif += 'Glue%s: %s\n' % (entry, values)
            else:
                for value in values:
                    ldif += 'Glue%s: %s\n' % (entry, value)
        for entry, values in self.nonglue.items():
            if not self.multi:
                ldif += '%s: %s\n' % (entry, values)
            else:
                for value in values:
                    ldif += '%s: %s\n' % (entry, value)
        return ldif

    def __hash__(self):
        return hash(tuple([normalizeDN(self.dn), self.objectClass, self.glue, self.nonglue]))

    def __str__(self):
        output = 'Entry: %s\n' % str(self.dn)
        output += 'Classes: %s\n' % str(self.objectClass)
        output += 'Attributes: \n'
        for key, val in self.glue.items():
            output += ' - %s: %s\n' % (key, val)
        for key, val in self.nonglue.items():
            output += ' - %s: %s\n' % (key, val)
        return output

    def __eq__(ldif1, ldif2):
        if not compareDN(ldif1, ldif2):
            return False
        if not compareObjectClass(ldif1, ldif2):
            return False
        if not compareLists(ldif1.glue.keys(), ldif2.glue.keys()):
            return False
        if not compareLists(ldif1.nonglue.keys(), ldif2.nonglue.keys()):
            return False
        for entry in ldif1.glue:
            if not compareLists(ldif1.glue[entry], ldif2.glue[entry]):
                return False
        for entry in ldif1.nonglue:
            if not compareLists(ldif1.nonglue[entry], ldif2.nonglue[entry]):
                return False
        return True

def read_ldap(fp, multi=False):
    entry_started = False
    mybuffer = ''
    entries = []
    counter = 0
    lines = fp.readlines()

    for line in lines[1:]:
        counter += 1
        if line.startswith('dn:'):
            if lines[counter-1].strip():
                lines.insert(counter-1, '\n')

    for origline in lines:
        line = origline.strip()
        if len(line) == 0 and entry_started == True:
            entries.append(LdapData(mybuffer[1:], multi=multi))
            entry_started = False
            mybuffer = ''
        elif len(line) == 0 and entry_started == False:
            pass
        else: # len(line) > 0
            if not entry_started:
                entry_started = True
            if origline.startswith(' '):
                mybuffer += origline[1:-1]
            else:
                mybuffer += '\n' + line

    if entry_started == True:
        entries.append(LdapData(mybuffer[1:], multi=multi))
    return entries

def query_bdii(endpoint, query="(objectClass=GlueCE)", base="o=grid", filter=""):
    r = re.compile('ldap://(.*):([0-9]*)')
    m = r.match(endpoint)
    if not m:
        raise Exception("Improperly formatted endpoint: %s." % endpoint)
    info = {}
    info['hostname'], info['port'] = m.groups()
    info['query'] = query
    info['base'] = base
    info['filter'] = filter

    if query == '':
        cmd = 'ldapsearch -h %(hostname)s -p %(port)s -xLLL -b %(base)s' \
            " " % info
    else:
        cmd = 'ldapsearch -h %(hostname)s -p %(port)s -xLLL -b %(base)s' \
            " '%(query)s' %(filter)s" % info

    std_in, std_out, std_err = os.popen3(cmd)
    return std_out

def compareLists(l1, l2):
    s1 = Set(l1)
    s2 = Set(l2)
    if len(s1.symmetric_difference(s2)) == 0:
        return True
    return False

def normalizeDN(dn_tuple):
    dn = ''
    for entry in dn_tuple:
        if entry.lower().find("mds-vo-name") >= 0 or \
                 entry.lower().find("o=grid") >=0:
            return dn[:-1]
        dn += entry + ','

def _starts_with_suffix(ldif):
    if ldif.dn[0].lower().find("mds-vo-name") >= 0 or ldif.dn[0].lower().find("o=grid") >=0:
        return True
    else:
        return False

def compareDN(ldif1, ldif2):
    dn1_startswith_suffix = _starts_with_suffix(ldif1)
    dn2_startswith_suffix = _starts_with_suffix(ldif2)
    if (dn1_startswith_suffix and dn2_startswith_suffix):
        for idx in range(len(ldif1.dn)):
            try:
                dn1 = ldif1.dn[idx]
                dn2 = ldif2.dn[idx]
            except IndexError:
                return False
            if dn1.lower() != dn2.lower():
                return False
        return True
    elif (dn1_startswith_suffix == False and dn2_startswith_suffix == False):
        for idx in range(len(ldif1.dn)):
            try:
                dn1 = ldif1.dn[idx]
                dn2 = ldif2.dn[idx]
            except IndexError:
                return False
            if dn1.lower().find("mds-vo-name") >= 0 or dn1.lower().find("o=grid") >=0:
                continue
            if dn1.lower() != dn2.lower():
                return False
        return True
    return False

def compareObjectClass(ldif1, ldif2):
    return compareLists(ldif1.objectClass, ldif2.objectClass)

def read_bdii(endpoint, query="", base="o=grid", multi=False):
    fp = query_bdii(endpoint, query=query, base=base)
    return read_ldap(fp, multi=multi)

def getSiteList(endpoint):
    query = "(&(objectClass=GlueTop)(!(objectClass=GlueSchemaVersion)))"
    fp = query_bdii(endpoint, query)
    entries = read_ldap(fp)
    sitenames = []
    for entry in entries:
        dummy, sitename = entry.dn[0].split('=')
        sitenames.append(sitename)
    return sitenames

def prettyDN(dn_list):
    dn = ''
    for entry in dn_list:
        dn += entry + ','
    return dn[:-1]

################################################################################
# GIP Validator 
################################################################################
def message(msg_type, msg_str):
    msg_type = str(msg_type)
    msg_str = str(msg_str)
    return {"type": msg_type, "msg": msg_str}

class ValidateGip:
    def __init__(self, ITB=False, localFile=False):
        self.itb_grid = ITB
        self.entries = ""
        self.site_id = ""
        self.messages = []
        self.site = ""
        self.localFile = localFile

        if self.itb_grid:
            self.endpoint = itb_endpoint
            self.wlcg_endpoint = pps_endpoint
        else:
            self.endpoint = osg_endpoint
            self.wlcg_endpoint = egee_endpoint

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def load_entries(self, site):
        # if self.localFile == True, then site will contain a file path
        if self.localFile:
            fd = open(site, 'r')
        else:
            bdii_base = "mds-vo-name=%s,mds-vo-name=local,o=grid" % site
            fd = query_bdii(self.endpoint, query="", base=bdii_base)
        return read_ldap(fd)

    def run(self, site_list):
        results = []
        for site in site_list:
            self.site = site
            self.entries = self.load_entries(site)
            if len(self.entries) > 0:
                results.append(self.main(site))
            else:
                test_result = {
                    "site"       : site, 
                    "type"       : 'OSG', 
                    "name"       : 'ValidateGIP_%s' % site, 
                    "messages"   : [{'msg': 'Could not get LDIF Entries.', 'type': 'CRIT'},],
                    "timestamp"  : time.strftime("%a %b %d %T UTC %Y", time.gmtime()),
                    "unixtimestamp"  : time.time(),
                    "result"     : MSG_UNKNOWN
                }
                results.append(test_result)
        return results

    def main(self, site):
        if not self.localFile:
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
        self.test_sponsors()
        self.test_site_missing()
        self.test_last_update_time()
        test_result = {"site"       : site, 
                       "type"       : 'OSG', 
                       "name"       : 'ValidateGIP_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : time.strftime("%a %b %d %T UTC %Y", time.gmtime()),
                       "unixtimestamp"  : time.time()
                      }
        if self.passed(self.messages):
            test_result["result"] = "PASS" 
        else:
            test_result["result"] = MSG_CRITICAL 

        self.messages = []
        return test_result

    def passed(self, msg_list):
        for msg in msg_list:
            if msg["type"] == MSG_CRITICAL: return False
        return True 

    def getTimestamp(self, format="%a %b %d %T UTC %Y"):
        return time.strftime(format, time.gmtime())

    def getSiteUniqueID(self, site):
        """
        Determine the unique ID for this site.
        """
        query = "(objectClass=GlueSite)"
        base = "mds-vo-name=%s,mds-vo-name=local,o=grid" % site
        site_entries = read_bdii(self.endpoint, query, base)
        
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

    def test_missing_newlines(self):
        msg="Entry %s, key %s is missing the newline character."
        r = re.compile("Glue\w*:\s")
        for entry in self.entries:
            for key, val in entry.glue.items():
                if not isinstance(val, types.StringType):
                    continue
                m = r.search(val)
                if not m == None:
                    self.appendMessage(MSG_CRITICAL,msg%(prettyDN(entry.dn),key))

    def test_missing_values(self):
        msg="No value for entry %s, key %s."
        for entry in self.entries:
            for key, val in entry.glue.items():
                if val == "":
                    self.appendMessage(MSG_CRITICAL,msg%(prettyDN(entry.dn),key))

    def test_existence_all(self):
        self.test_existence("GlueCEUniqueID")
        self.test_existence("GlueVOViewLocalID")
        self.test_existence("GlueSubClusterUniqueID")
        self.test_existence("GlueClusterUniqueID")
        self.test_existence("GlueCESEBindSEUniqueID")
        self.test_existence("GlueCESEBindGroupCEUniqueID")
        self.test_existence("GlueLocationLocalID")
        # Commenting out because ATLAS runs CE's where they do not advertise
        # GUMS and they do not have an attached SE, therefore they will not 
        # have any GlueServiceUniqueID stanzas.
        #self.test_existence("GlueServiceUniqueID")
        self.test_existence("GlueSEUniqueID")
        self.test_existence("GlueSEAccessProtocolLocalID")
        # need to check to see if there are any SE's other than the classic SE
        #  because the classic SE will not have a GlueSEControlProtocolLocalID
        #  stanza
        if self.nonClassicSeExist():
            self.test_existence("GlueSEControlProtocolLocalID")
        self.test_existence("GlueSALocalID")
        self.test_chunk_keys()
        self.test_foreign_keys()

    def test_ce(self):
        self.test_value_not_equal("GlueCE", "CEPolicyMaxCPUTime", "0", MSG_INFO)
        self.test_value_not_equal("GlueCE", "CEInfoTotalCPUs", "0", MSG_INFO)
        self.test_value_not_equal("GlueCE", "CEPolicyMaxWallClockTime", "0",
            MSG_INFO)

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
        bdii_base = "mds-vo-name=%s,mds-vo-name=local,o=grid" % self.site
        fd = query_bdii(self.endpoint, query="", base=bdii_base)
        local_entries = read_ldap(fd, multi=True)

        for entry in local_entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            try:
                try:
                    sponsors = entry.glue['SiteSponsor']
                except:
                    msg="Invalid site sponsor: %s" % entry.glue['SiteSponsor']
                    self.appendMessage(MSG_CRITICAL, msg)
                    return
                tot = 0
                num_sponsors = 0
                for s in sponsors:
                    try:
                        amount = int(s.split(":")[1])
                    except:
                        msg="Invalid site sponsor percentage: %s" % entry.glue['SiteSponsor']
                        self.appendMessage(MSG_CRITICAL, msg)
                        return
                    num_sponsors += 1
                    tot += amount
                if num_sponsors == 1 and tot == 0:
                    tot = 100
                if tot != 100:
                    msg = "Site sponsorship does not add up to 100: %s"
                    msg = msg % entry.glue['SiteSponsor']
                    self.appendMessage(MSG_CRITICAL, msg)
            except KeyError:
                msg = "Site sponsorship does not exist"
                self.appendMessage(MSG_CRITICAL, msg)

    def test_existence(self, name, full=False, key_check="", orig_dn="", case_sensitive=True):
        for entry in self.entries:
            if case_sensitive:
                if full and entry.dn[0] == name:
                    return
                if (not full) and entry.dn[0].startswith(name):
                    return
            else:
                if full and entry.dn[0].lower() == name.lower():
                    return
                if (not full) and entry.dn[0].lower().startswith(name.lower()):
                    return
        if len(orig_dn) > 0:
            msg = "(%s Check) GLUE Entity %s does not exist for %s."
            msg = msg % (key_check, name, orig_dn)
        else:
            msg = "GLUE Entity %s does not exist." % name
        self.appendMessage(MSG_CRITICAL, msg)

    def test_chunk_keys(self):
        for entry in self.entries:
            if 'ChunkKey' not in entry.glue: continue
            self.test_existence(entry.glue['ChunkKey'], full=True,
                key_check="ChunkKey", orig_dn=prettyDN(entry.dn),
                case_sensitive=False)

    def test_foreign_keys(self):
        for entry in self.entries:
            if 'ForeignKey' not in entry.glue: continue
            self.test_existence(entry.glue['ForeignKey'], full=True,
                key_check="ForeignKey", orig_dn=prettyDN(entry.dn),
                case_sensitive=False)

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
                msg = "GLUE attribute %s for entity %s in\n %s \n is equal to %s"
                msg = msg % (attribute, objClass, prettyDN(entry.dn), value)
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

            # Note:  We don't have a site_id if we are reading from a local file
            #  Don't do this particular test - means that the ldif *could* be failing
            #  this test and we won't know
            if not self.localFile:
                site_unique_id = m.groups()[0]
                if site_unique_id != self.site_id:
                    msg = "Incorrect site unique ID for SRM service. %s != %s"
                    msg = msg % (site_unique_id, self.site_id)
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

    def test_dn(self):
        for entry in self.entries:
            dn = list(entry.dn)
            fulldn = prettyDN(entry.dn)
            if dn.pop() != "o=grid":
                msg = "DN %s does not end with o=grid" % fulldn
                self.appendMessage(MSG_CRITICAL, msg)
            if dn.pop().lower() != "mds-vo-name=local":
                msg="DN %s does not end with mds-vo-name=local,o=grid" % fulldn
                self.appendMessage(MSG_CRITICAL, msg)
            for d in dn:
                if not d.find("o=grid") < 0:
                    msg="There is an extra o=grid entry in DN %s" % fulldn
                    self.appendMessage(MSG_CRITICAL, msg)
                if d.startswith("mds-vo-name"):
                    msg="There is an extra mds-vo-name entry in DN %s" % fulldn
                    self.appendMessage(MSG_CRITICAL, msg)

    def test_site_missing(self):
        # If we are reading from a local file, this test is irrelevant
        if not self.localFile:
            query = "(objectClass=GlueCE)"
            base = "mds-vo-name=%s,mds-vo-name=local,o=grid" % self.site
            fd = query_bdii(self.endpoint, query, base)
            line = fd.readline().lower()
            if not line.startswith("dn:"):
                msg = "Missing - Check CEMon logs to see any errors and when the last time it reported."
                self.appendMessage(MSG_CRITICAL, msg)

    def test_interop_reporting(self):
        isInterop = self.checkIsInterop(site)
        if isInterop:
            query="(objectClass=GlueSite)"
            base="mds-vo-name=%s,mds-vo-name=local,o=grid" % site
            # try the OSG BDII
            self.cp.set('bdii', 'endpoint', self.osg_endpoint)
            data = read_bdii(self.cp, query, base)

            if len(data) < 1:
                msg = "%s does not exist in the OSG BDII" % site
                self.appendMessage(MSG_CRITICAL, msg)

            # try the WLCG BDII
            self.cp.set('bdii', 'endpoint', self.egee_endpoint)
            data = read_bdii(self.cp, query, base)
            if len(data) < 1:
                msg = "%s does not exist in the WLCG BDII" % site
                self.appendMessage(MSG_CRITICAL, msg)

    def checkIsInterop():
        myosg_url="http://myosg.grid.iu.edu/rgsummary/xml?datasource=summary&"\
                "summary_attrs_showwlcg=on&all_resources=on&gridtype=on&"\
                "gridtype_1=on&has_wlcg=on"
        myosg_xml = urllib.urlopen(myosg_summary_url).read()
        xml_doc = libxml2.parseDoc(myosg_summary_xml)
        
        for rg in xml_doc.xpathEval('//ResourceGroup'):
            for rg_name in rg.xpathEval('GroupName'):
                if rg_name.content == self.site:
                    for res in rg.xpathEval('Resource'):
                        for interop in res.xpathEval('InteropBDII'):
                            InteropBDII = interop.content
                            if smart_bool(InteropBDII):
                                return True
        return False

    def test_last_update_time(self):
        # another test that is irrelevant if we are reading from a local file
        if not self.localFile:
            bdii_time = self.getLastUpdateFromBDII()
            local_time = time.time()
    
            TimeError = False
            try:
                bdii_time = float(bdii_time)
            except ValueError:
                TimeError = True
                msg = "BDII Timestamp error for site: %s\nBDII Timestamp: %s" % \
                    (self.site, str(bdii_time)) 
                self.appendMessage(MSG_CRITICAL, msg)
                bdii_time = 0
    
            try:
                local_time = float(local_time)
            except ValueError:
                TimeError = True
                msg = "Local Timestamp error for site: %s\nBDII Timestamp: %s" % \
                    (self.site, str(local_time)) 
                self.appendMessage(MSG_CRITICAL, msg)
                local_time = 0
                
            diff_minutes = abs((local_time - bdii_time) / 60)
            if TimeError:
                msg = "The BDII Timestamp and/or the Local Timestamp had an error" 
                self.appendMessage(MSG_CRITICAL, msg)
            elif diff_minutes > 30:
                msg = "The BDII Timestamp and the Local Timestamp differ by "\
                    "%s minutes" % str(diff_minutes) 
                self.appendMessage(MSG_CRITICAL, msg)

    def getLastUpdateFromBDII(self):
        query = "(GlueLocationLocalID=TIMESTAMP)"
        base = "mds-vo-name=%s,mds-vo-name=local,o=grid" % self.site
        entries = read_bdii(self.endpoint, query, base)
        bdii_time = ""
        for entry in entries:
            # we are only interested in the first TIMESTAMP stanza
            bdii_time = entry.glue['LocationVersion']
            break

        return bdii_time
    
################################################################################
# Validator Main
################################################################################
def smart_bool(s):
    if s is True or s is False: return s
    s = str(s).strip().lower()
    return not s in ['false','f','n','0','']

class ValidatorMain:
    def __init__(self):
        self.ITB = False
        self.format = ""
        self.site_list = []
        self.local_file = ""

    def parseArgs(self, args):
        p = optparse.OptionParser()
        help_msg = 'Site list, if OIM, then pull list from OIM.'
        p.add_option('-s', '--sites', dest='sites', help=help_msg, default='OIM')
        help_msg = 'Specifies which grid to pull info from, ITB or Prod.'
        p.add_option('-g', '--grid', dest='grid', help=help_msg, default='Prod')
        help_msg = 'Print results.'
        p.add_option('-p', '--print', dest='print_results', help=help_msg, default="False")
        help_msg = 'Read from local file.  Overrides -s, --sites, -g, --grid.'
        p.add_option('-l', '--local-file', dest='local_file', help=help_msg, default="False")
        help_msg = 'Comma separated file list.  The file should contain ldif.'
        p.add_option('-f', '--file-list', dest='file_list', help=help_msg, default="False")
        (options, args) = p.parse_args()

        if not (options.grid.lower() == "prod"): self.ITB = True

        self.local_file = smart_bool(options.local_file)
        # get site list
        # if a local file has been specified, it overrides all other options 
        # except for the print option
        if self.local_file:
            self.site_list = options.file_list.split(",")
        else:
            if options.sites == "OIM":
                sitelist, itb_sitelist = self.getOIMSites()
                if options.grid.lower() == "itb":
                    self.ITB = True
                    self.site_list = itb_sitelist
                else:
                    self.site_list = sitelist
            else:
                self.site_list = options.sites.split(',')

        self.print_results = smart_bool(options.print_results)

    def getOIMSites(self):
        # get sites from OIM
        prod_tmp = ""
        itb_tmp = ""
        prod_grid_type = "osg production resource"
        itb_grid_type = "osg integration test bed resource"
        
        myosg_summary_url = "http://myosg.grid.iu.edu/wizardsummary/xml?"\
            "datasource=summary&summary_attrs_showservice=on&"\
            "summary_attrs_showfqdn=on&summary_attrs_showwlcg=on&"\
            "gip_status_attrs_showfqdn=on&account_type=cumulative_hours&"\
            "ce_account_type=gip_vo&se_account_type=vo_transfer_volume&"\
            "start_type=7daysago&start_date=04%2F23%2F2009&end_type=now&"\
            "end_date=04%2F30%2F2009&all_resources=on&gridtype_1=on&service=on"\
            "&service_1=on&active=on&active_value=1&disable_value=1"

        myosg_summary_xml = urllib.urlopen(myosg_summary_url).read()
        xml_summary = libxml2.parseDoc(myosg_summary_xml)
        
        for rg in xml_summary.xpathEval('//ResourceGroup'):
            for grid_type in rg.xpathEval ('GridType'):
                
                if (grid_type.content.lower() == prod_grid_type):
                    for rg_name in rg.xpathEval('GroupName'):
                        prod_tmp += rg_name.content + "\n"

                if (grid_type.content.lower() == itb_grid_type):
                    for rg_name in rg.xpathEval ('GroupName'):
                        itb_tmp += rg_name.content + "\n"

        sitelist = prod_tmp.split()
        itbsitelist = itb_tmp.split()

        return sitelist, itbsitelist

    def determineReturnCode(self, results):
        # if there are multiple sites and no other errors, then simply return 0
        # since the likleyhood of all sites returning error free are slim to
        # none.
        if len(self.site_list) > 1:
            return 0

        # since this is a single site test, return 1 if there are any critical
        # errors
        if results[0]['result'].upper() == MSG_CRITICAL:
            return 1
        
        # could not contact the BDII or get info from gip_info
        if results[0]['result'].upper() == MSG_UNKNOWN:
            return 2

        # otherwise return 0 (Success!!)
        return 0

    def printResults(self, results):
        xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml += '<GIPValidator>\n'
        for result in results:
            xml += "    <ResourceGroup type='%s'>\n" % result['type']
            xml += "        <Name>%s</Name>\n" % result['site']
            xml += "        <Timestamp>%s</Timestamp>\n" % result['timestamp']
            xml += "        <UnixTimestamp>%s</UnixTimestamp>\n" % result['unixtimestamp']
            xml += "        <Result>%s</Result>\n" % result['result']
            xml += "        <Messages>\n"
            for msg in result['messages']:
                xml += "            <Message>%s</Message>\n" % str(msg['msg'])
            xml += "        </Messages>\n"
            xml += "    </ResourceGroup>\n"
        xml += '</GIPValidator>\n'
        print xml
        
    def main(self, args):
        self.parseArgs(args)

        test = ValidateGip(ITB=self.ITB, localFile=self.local_file)
        results = test.run(self.site_list)
        if self.print_results: self.printResults(results)
        return self.determineReturnCode(results)

if __name__ == '__main__':
    v = ValidatorMain()
    sys.exit(v.main(sys.argv))
