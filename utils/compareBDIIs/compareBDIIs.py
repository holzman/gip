#!/usr/bin/python

import os
import sys
import re
import optparse

# True if the current version of Python is 2.3 or higher
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3

################################################################################
# GIP LDAP Module
################################################################################
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
################################################################################

class compareBDIIs:
    def __init__(self, master_bdii_endpoint, compare_bdii_endpoint, resource_group):
        bdii_base = "mds-vo-name=%s,mds-vo-name=local,o=grid" % resource_group        
        self.master_entries = read_ldap(query_bdii(master_bdii_endpoint, query="", base=bdii_base))
        self.compare_entries = read_ldap(query_bdii(compare_bdii_endpoint, query="", base=bdii_base))

    def compare(self):
        bad_entries = list(set(self.master_entries).symmetric_difference(set(self.compare_entries)))
        filtered_entries = []
        bad_entry_dns = []
        for entry in bad_entries:
            if not entry.objectClass == ('GlueTop', ):
                filtered_entries.append(entry)
                bad_entry_dns.append(entry.dn)
        bad_entries = filtered_entries
        bad_entry_dns.sort()
        
        output = ""
        
        missing_dns = []
        errors = []
        content_differences = {}
        for dn in bad_entry_dns:
            MasterEntry = self.getEntry(self.master_entries, dn)
            CompareEntry = self.getEntry(self.compare_entries, dn)
            
            MasterEntryMissing = False
            CompareEntryMissing = False
            if MasterEntry is None: 
                MasterEntryMissing = True
            if CompareEntry is None: 
                CompareEntryMissing = True

            if CompareEntryMissing or MasterEntryMissing:
                # stanza is in the bad entries because it is missing in one of the BDIIs
                msg = '(in Compare BDII %s; in Master BDII %s) : %s\n' % (str(CompareEntryMissing), str(MasterEntryMissing), prettyDN(dn)) 
                missing_dns.append(msg)
            
            if CompareEntryMissing and MasterEntryMissing:
                # We have encountered an error that really shouldn't be possible
                msg = "ERROR:  DN: %s shows up in the bad entries list but cannot be found in either BDII"
                errors.append(msg)
            
            if not CompareEntryMissing and not MasterEntryMissing:
                # We have an stanza that is in both BDIIs, but the contents are different
                # Attributes to ignore:
                #    CEStateRunningJobs
                #    CEStateFreeJobSlots
                #    CEStateTotalJobs
                #    CEStateFreeCPUs
                #    CEStateWorstResponseTime
                #    CEStateEstimatedResponseTime
                #    CEStateWaitingJobs
                dn = prettyDN(MasterEntry.dn)
                bad_glue = set(MasterEntry.glue.keys()).symmetric_difference(set(CompareEntry.glue.keys()))
                if len(bad_glue) > 0:
                    # we have missing keys in one or the other entry
                    pass
                else:
                    # no missing keys, loop through the values and ignore the appropriate ones
                    ignore_keys = ["CEStateRunningJobs", "CEStateFreeJobSlots", "CEStateTotalJobs",
                                   "CEStateFreeCPUs", "CEStateWorstResponseTime", "CEStateEstimatedResponseTime",
                                   "CEStateWaitingJobs"]
                    for key in MasterEntry.glue.keys():
                        if key in ignore_keys:
                            continue
                        if MasterEntry.glue[key] != CompareEntry.glue[key]:
                            d = {"key":key, "MasterValue":MasterEntry.glue[key], "CompareValue":CompareEntry.glue[key]}
                            if dn in content_differences.keys():
                                content_differences[dn] += "    Master[%(key)s] differs from Compare[%(key)s]: (MasterValue/CompareValue: (%(MasterValue)s/%(CompareValue)s)\n" % d
                            else:
                                content_differences[dn] = "    Master[%(key)s] differs from Compare[%(key)s]: (MasterValue/CompareValue: (%(MasterValue)s/%(CompareValue)s)\n" % d
                                 
        diffs = ""
        content_keys = content_differences.keys()
        content_keys.sort()
        for key in content_keys:
            diffs += "DN: %s\n%s" % (key, content_differences[key])
        
        output += "Missing DNs:\n%s\n\nContent Differences: \n%s\n\nErrors: %s" % ("".join(missing_dns), diffs, "".join(errors))
        
       
        return output
    
    def getEntry(self, entries, dn):
        for entry in entries:
            if prettyDN(entry.dn).upper() == prettyDN(dn).upper():
                return entry
        return None
    
if __name__ == "__main__":
    p = optparse.OptionParser()
    help_msg = 'Master BDII endpoint.  Example: ldap://is.grid.iu.edu:2170'
    p.add_option('-m', '--master-bdii', dest='master', help=help_msg, default='')
    help_msg = 'BDII endpoint to compare.  Example: ldap://is.grid.iu.edu:2170'
    p.add_option('-c', '--compare-bdii', dest='compare', help=help_msg, default='')
    help_msg = 'Resource Group to query for.  Example: USCMS-FNAL-WC1'
    p.add_option('-r', '--resource-group', dest='resource_group', help=help_msg, default='')

    options, _ = p.parse_args()

    if options.master == '' or options.compare == '':
        print >> sys.stderr, "You must specify the master and compare bdii's"
        sys.exit(1)
    
    if options.resource_group == '':
        print >> sys.stderr, "You must specify the resource group to query for."
        sys.exit(1)

    c = compareBDIIs(options.master, options.compare, options.resource_group)
    results = c.compare()
    print results
