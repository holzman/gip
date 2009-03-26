#!/usr/bin/env python
import unittest
import os
import sys
import tempfile
import urllib2
import datetime

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from ldap import read_ldap, query_bdii, getSiteList
from gip_common import config, addToPath
from gip_testing import runTest, streamHandler

slap_conf = """
include		/etc/openldap/schema/core.schema
include		$GIP_LOCATION/test/schema/Glue-CORE.schema
include		$GIP_LOCATION/test/schema/Glue-CE.schema
include		$GIP_LOCATION/test/schema/Glue-CESEBind.schema
include		$GIP_LOCATION/test/schema/Glue-SE.schema

database        bdb
suffix		"o=Grid"
"""

def getTempFilename():
    try:
        conffile = tempfile.NamedTemporaryFile()
        conffile = conffile.name
    except:
        conffile = tempfile.mktemp()
    return conffile

def createSlapConf():
    conffile = getTempFilename()
    fp = open(conffile, 'w')
    fp.write(os.path.expandvars(slap_conf))
    return conffile

slap_cmd = "slapadd -u -c -f %(slap_conf)s -l %(slap_info)s"

def makeSafe(site):
    return site.replace('-', '_')


class TestSchema(unittest.TestCase):

    def __init__(self, site, cp):
        setattr(self, "testSchema_%s" % site, self.testSchema)
        unittest.TestCase.__init__(self, 'testSchema_%s' % site)
        self.site = site
        self.cp = cp
        self.slap_conf = createSlapConf()

    def testSchema(self):
        """
        Test schema of site %s.
        """ % self.site
        info_file = getTempFilename()
        fp2 = open(info_file, 'w')
        #fp = query_bdii(self.cp, base="mds-vo-name=%s,mds-vo-name=local" \
        #    ",o=grid" % self.site)
        url = self.cp.get('test', 'goc') % self.site
        fp = urllib2.urlopen(url)
        ctr = 0
        for line in fp:
            #print line.strip()
            ctr += 1
            if ctr < 3:
                self.failUnless(line.find("Error Message") < 0, \
                    msg="Site %s not serving with CEMon." % self.site)
            fp2.write(line)
        fp2.close()
        stdin, fp3 = os.popen4(slap_cmd % {'slap_conf': self.slap_conf,\
            'slap_info': info_file})
        stdin.close()
        output = fp3.read()
        self.assertEquals(len(output), 0, msg="slapadd schema check failed" \
            " for site %s.  Output:\n%s" % (self.site, output))

def main():
    """
    The main entry point for when schema_check is run in standalone mode.
    """
    addToPath("/usr/sbin")
    cp = config()
    stream = streamHandler(cp)
    runTest(cp, TestSchema, stream)

if __name__ == '__main__':
    main()
