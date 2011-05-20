
"""
Simplify interaction with and querying of an LDAP server
"""

__author__ = "Brian Bockelman"

import os
import re
import sys
import gip_sets as sets

class _hdict(dict): #pylint: disable-msg=C0103
    """
    Hashable dictionary; used to make LdapData objects hashable.
    """
    def __hash__(self):
        items = self.items()
        items.sort()
        return hash(tuple(items))


class LdapData:

    """
    Class representing the logical information in the GLUE entry.
    Given the LDIF GLUE, represent it as an object.

    """

    #pylint: disable-msg=W0105
    glue = {}
    """
    Dictionary representing the GLUE attributes.  The keys are the GLUE entries,
    minus the "Glue" prefix.  The values are the entries loaded from the LDIF.
    If C{multi=True} was passed to the constructor, then these are all tuples.
    Otherwise, it is just a single string.
    """

    nonglue = {}
    """
    Dictionary representing arbitrary non-GLUE attributes.  Handled similarly
    to the GLUE attributes.
    """

    objectClass = []
    """
    A list of the GLUE objectClasses this entry implements.
    """

    dn = []
    """
    A list containing the components of the DN.
    """

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
                # changed so we can handle the case where val is none
                #attr, val = line.split(': ', 1)
                p = line.split(': ', 1)
                attr = p[0]
                try:
                    val = p[1]
                except:
                    val = ""
            except:
                print >> sys.stderr, line.strip()
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
                #raise ValueError("Invalid data:\n%s\nBad attribute:%s" % (data,
                #    attr))
        objectClass.sort()
        self.objectClass = tuple(objectClass)
        try:
            self.dn = tuple(dn)
        except:
            print >> sys.stderr, "Invalid GLUE:\n%s" % data
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

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __eq__(self, other):
        ldif1 = self
        ldif2 = other
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
    """
    Convert a file stream into LDAP entries.

    @param fp: Input stream containing LDIF data.
    @type fp: File-like object
    @keyword multi: If True, then the resulting LdapData objects can have
        multiple values per GLUE attribute.
    @returns: List containing one LdapData object per LDIF entry.
    """
    entry_started = False
    mybuffer = ''
    entries = []
    counter = 0
    lines = fp.readlines()

    # Put in newlines before dn: stanzas if they don't exist already
    for line in lines[1:]:
        counter += 1
        if line.startswith('dn:'):
            if lines[counter-1].strip():
                lines.insert(counter-1, '\n')

    # Now parse the LDIF into separate entries split on newlines
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
    #Catch the case where we started the entry and got to the end of the file
    #stream
    if entry_started == True:
        entries.append(LdapData(mybuffer[1:], multi=multi))
    return entries

def query_bdii(cp, query="(objectClass=GlueCE)", base="o=grid", filter=""):
    """
    Query a BDII for data.
    @param cp: Site configuration; will read the bdii.endpoint entry to find
        the LDAP endpoint
    @type cp: ConfigParser
    @keyword query: Query string for the LDAP server.  Defaults to 
        (objectClass=GlueCE)
    @keyword base: Base DN for the LDAP server.
    @keyword filter: Attribute filter to narrow return results.
    @returns: File stream of data returned by the BDII.
    """
    endpoint = cp.get('bdii', 'endpoint')
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
    #print cmd
    fp = os.popen(cmd)
    return fp

def compareLists(l1, l2):
    """
    Compare two lists of items; turn them into sets and then look at the
    symmetric differences.
    """
    s1 = sets.Set(l1)
    s2 = sets.Set(l2)
    if len(s1.symmetric_difference(s2)) == 0:
        return True
    return False

def normalizeDN(dn_tuple):
    """
    Normalize a DN; because there are so many problems with mds-vo-name
    and presence/lack of o=grid, just remove those entries.
    """
    dn = ''
    for entry in dn_tuple:
        if entry.lower().find("mds-vo-name") >= 0 or \
                 entry.lower().find("o=grid") >=0:
            return dn[:-1]
        dn += entry + ','

def _starts_with_suffix(ldif):
    if ldif.dn[0].lower().find("mds-vo-name") >= 0 or \
            ldif.dn[0].lower().find("o=grid") >=0:
        return True
    else:
        return False

def compareDN(ldif1, ldif2):
    """
    Compare two DNs of LdapData objects.
    
    Returns true if both objects have the same LDAP DN.
    """
    dn1_startswith_suffix = _starts_with_suffix(ldif1)
    dn2_startswith_suffix = _starts_with_suffix(ldif2)

    if (dn1_startswith_suffix and dn2_startswith_suffix):
        for idx in range(len(ldif1.dn)):
            dn1 = ldif1.dn[idx]
            dn2 = ldif2.dn[idx]
            if dn1 != dn2:
                return False
        return True
    elif (dn1_startswith_suffix == False and dn2_startswith_suffix == False):
        for idx in range(len(ldif1.dn)):
            dn1 = ldif1.dn[idx]
            dn2 = ldif2.dn[idx]
            if dn1.lower().find("mds-vo-name") >= 0 or \
                    dn1.lower().find("o=grid") >=0:
                continue
            if dn1 != dn2:
                return False
        return True
    return False

def compareObjectClass(ldif1, ldif2):
    """
    Compare the object classes of two LdapData objects.
    Returns true if the lists of data match.
    """
    return compareLists(ldif1.objectClass, ldif2.objectClass)

def read_bdii(cp, query="", base="o=grid", multi=False):
    """
    Query a BDII instance, then parse the results.

    @param cp: Site configuration; see L{query_bdii}
    @type cp: ConfigParser
    @keyword query: LDAP query filter to use
    @keyword base: Base DN to query on.
    @keyword multi: If True, then resulting LdapData can have multiple values
        per attribute
    @returns: List of LdapData objects representing the data the BDII returned.
    """
    fp = query_bdii(cp, query=query, base=base)
    return read_ldap(fp, multi=multi)

def getSiteList(cp):
    """
    Get the listing of all sites in a BDII.

    @param cp: Site configuration; see L{query_bdii}.
    @type cp: ConfigParser
    @returns: List of site names.
    """
    fp = query_bdii(cp, query="(&(objectClass=GlueTop)" \
        "(!(objectClass=GlueSchemaVersion)))")
    entries = read_ldap(fp)
    sitenames = []
    for entry in entries:
        dummy, sitename = entry.dn[0].split('=')
        sitenames.append(sitename)
    return sitenames

def prettyDN(dn_list):
    """
    Take the DN in list form and transform it back into the traditional,
    comma-separated format.
    """
    dn = ''
    for entry in dn_list:
        dn += entry + ','
    return dn[:-1]

