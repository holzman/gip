
"""
Simplify interaction with and querying of an LDAP server
"""

__author__ = "Brian Bockelman"

import os
import re


class _hdict(dict):
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

    glue = {}
    """
    Dictionary representing the GLUE attributes.  The keys are the GLUE entries,
    minus the "Glue" prefix.  The values are the entries loaded from the LDIF.
    If C{multi=True} was passed to the constructor, then these are all tuples.
    Otherwise, it is just a single string.
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
        objectClass = []
        for line in self.ldif.split('\n'):
            if line.startswith('dn: '):
                dn = line[4:].split(',')
                dn = [i.strip() for i in dn]
                continue
            try:
                attr, val = line.split(': ', 1)
            except:
                print line.strip()
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
            else:
                raise ValueError("Invalid data:\n%s" % data)
        objectClass.sort()
        self.objectClass = tuple(objectClass)
        self.dn = tuple(dn)
        for entry in glue:
            if multi:
                glue[entry] = tuple(glue[entry])
        self.glue = _hdict(glue)
        self.multi = multi

    def to_ldif(self):
        ldif = 'dn: ' + ','.join(self.dn) + '\n'
        for obj in self.objectClass:
            ldif += 'objectClass: %s\n' % obj
        for entry, values in self.glue.items():
            if not self.multi:
                ldif += 'Glue%s: %s\n' % (entry, values)
            else:
                for value in values:
                    ldif += 'Glue%s: %s\n' % (entry, value)
        return ldif

    def __hash__(self):
        return hash(tuple([normalizeDN(self.dn), self.objectClass, self.glue]))

    def __str__(self):
        output = 'Entry: %s\n' % str(self.dn)
        output += 'Classes: %s\n' % str(self.objectClass)
        output += 'Attributes: \n'
        for key, val in self.glue.items():
            output += ' - %s: %s\n' % (key, val)
        return output

    def __eq__(ldif1, ldif2):
        if not compareDN(ldif1, ldif2):
            return False
        if not compareObjectClass(ldif1, ldif2):
            return False
        if not compareLists(ldif1.glue.keys(), ldif2.glue.keys()):
            return False
        for entry in ldif1.glue:
            if not compareLists(ldif1.glue[entry], ldif2.glue[entry]):
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
    buffer = ''
    entries = []
    counter = 0
    for origline in fp.readlines():
        counter += 1
        line = origline.strip()
        if len(line) == 0 and entry_started == True:
            entries.append(LdapData(buffer[1:], multi=multi))
            entry_started = False
            buffer = ''
        elif len(line) == 0 and entry_started == False:
            pass
        else: # len(line) > 0
            if not entry_started:
                entry_started = True 
            if origline.startswith(' '):
                buffer += origline[1:-1]
            else:
                buffer += '\n' + line
    #Catch the case where we started the entry and got to the end of the file
    #stream
    if entry_started == True:
        entries.append(LdapData(buffer[1:], multi=multi))
    return entries

def query_bdii(cp, query="(objectClass=GlueCE)", base="o=grid"):
    """
    Query a BDII for data.
    @param cp: Site configuration; will read the bdii.endpoint entry to find
        the LDAP endpoint
    @type cp: ConfigParser
    @keyword query: Query string for the LDAP server.
    @keyword base: Base DN for the LDAP server.
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
    if query == '':
        cmd = 'ldapsearch -h %(hostname)s -p %(port)s -xLLL -b %(base)s' \
            " " % info
    else:
        cmd = 'ldapsearch -h %(hostname)s -p %(port)s -xLLL -b %(base)s' \
            " '%(query)s'" % info
    #print cmd
    fp = os.popen(cmd)
    return fp

def compareLists(l1, l2):
    s1 = set(l1)
    s2 = set(l2)
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

def compareDN(ldif1, ldif2):
   for idx in range(len(ldif1.dn)):
       dn1 = ldif1.dn[idx]
       dn2 = ldif2.dn[idx]
       if dn1.lower().find("mds-vo-name") >= 0 or \
               dn1.lower().find("o=grid") >=0:
           break
       if dn1 != dn2:
           return False
   return True

def compareObjectClass(ldif1, ldif2):
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

