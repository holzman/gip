
"""
Testing framework for the GIP.

This allows one to replace output from command-line invocations with saved
outputs from the test/command_output directory.
"""

import os
import re
import sys
import types
import unittest
import datetime
import urlparse

from gip_common import cp_get, pathFormatter, parseOpts
from gip_ldap import getSiteList, prettyDN

replace_command = False

commands = {}

def lookupCommand(cmd):
    cmd = cmd.strip()
    env = os.environ['GIP_TESTING']
    m = re.match("suffix=(.*)", env)
    suffix = None
    if m:
        suffix = m.groups()[0]
    if cmd not in commands:
        fd = open(os.path.expandvars("$VDT_LOCATION/test/command_output/" \
            "commands"))
        for line in fd:
            if line.startswith("#") or len(line.strip()) == 0:
                continue
            command, val = line.split(':', 1)
            val = val.strip()
            command = command.strip()
            if suffix:
                command = '%s_%s' % (command, suffix)
            commands[val.strip()] = command
    return commands[cmd]

def runCommand(cmd, force_command=False):
    if replace_command and not force_command:
        try:
            filename = lookupCommand(cmd)
        except Exception, e:
            print >> sys.stderr, e
            return runCommand(cmd, force_command=True)
        return open(os.path.expandvars("$VDT_LOCATION/test/command_output/%s" \
            % filename))
    else:
        return os.popen(cmd)

def generateTests(cp, cls, args=[]):
    """
    Given a class and args, generate a test case for every site in the BDII.

    @param cp: Site configuration
    @type cp: ConfigParser
    @param cls: Test class to use to generate a test suite.  It is assumed
        that the constructor for this class has signature cls(cp, site_name)
    @type cls: class
    @keyword args: List of sites; if it is not empty, then tests will only be
        generated for the given sites.
    """
    sites = getSiteList(cp)
    kw, passed, args = parseOpts(sys.argv[1:])
    tests = []
    for site in sites:
        if len(args) > 0 and site not in args:
            continue
        if site == 'local' or site == 'grid':
            continue
        case = cls(site, cp)
        tests.append(case)
    return unittest.TestSuite(tests)

def streamHandler(cp):
    """
    Given the ConfigParser, find the preferred stream for test output

    @param cp: Site configuration
    @type cp: ConfigParser
    """

    streamName = cp_get(cp, "TestRunner", "StreamName", "")
    if (streamName is None) or (streamName == ""):
        return sys.stderr
    elif (streamName.lower() == "stdout") or (streamName.lower() == "sys.stdout"):
        return sys.stdout
    elif (streamName.lower() == "file"):
        logDir = pathFormatter(cp_get(cp, "TestRunner", "LogDir", "/tmp"))
        logPrefix = cp_get(cp, "TestRunner", "LogPrefix", "")
        logFile = logDir + "/" + logPrefix \
            + datetime.datetime.now().strftime("%A_%b_%d_%Y_%H_%M_%S")
        return open(logFile, 'w')

def runTest(cp, cls, out=None, per_site=True):
    """
    Given a test class, generate and run a test suite

    @param cp: Site configuration
    @type cp: ConfigParser
    @param cls: Test class to use to generate a test suite.  It is assumed
        that the constructor for this class has signature cls(cp, site_name).
        If per_site=False, then the signature is assumed to be cls().
    @type cls: class
    @keyword per_site: Set to true if there is one instance of the test class
        per site.
    @param out: A stream where the output from the test suite is logged
    @type out: stream
    """
    if per_site:
        testSuite = generateTests(cp, cls, sys.argv[1:])
    else:
        testSuite = suite = unittest.TestLoader().loadTestsFromTestCase(cls)

    if out is None:
        testRunner = unittest.TextTestRunner(verbosity=2)
    else:
        testRunner = unittest.TextTestRunner(stream=out, verbosity=2)
    result = testRunner.run(testSuite)
    sys.exit(not result.wasSuccessful())

class GipValidate(unittest.TestCase):

    def __init__(self, entries):
        self.entries = entries

    def run(self):
        self.test_existence_all()
        self.test_egee_site_unique_id()
        self.test_ce()
        self.test_site()
        self.test_missing_newlines()

    def test_missing_newlines(self):
        r = re.compile("Glue\w*:\s")
        for entry in self.entries:
            for key, val in entry.glue.items():
                if not isinstance(val, types.StringType):
                    continue
                m = r.search(val)
                self.assertEquals(m, None, msg="Entry %s, key %s is missing" \
                    " the newline character." % (prettyDN(entry.dn), key))

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
            parts= urlparse.urlparse(entry.glue['SiteWeb'])
            self.assertNotEquals(parts, '', msg="Invalid website: %s" % \
                entry.glue['SiteWeb'])
           
    def test_sponsors(self):
        r = re.compile("(\w+):([0-9]+)")
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            m = r.findall(entry.glue['SiteSponsor'])
            self.assertNotEquals(m, None, msg="Invalid site sponsor: %s" % \
                entry.glue['SiteSponsor'])
            tot = 0
            for e in m:
                tot += int(e[1])
            self.assertEquals(tot, 100, msg="Site sponsorship does not add up "\
                " to 100: %s" % entry.glue['SiteSponsor'])

    def test_existence(self, name, full=False):
        for entry in self.entries:
            if full and entry.dn[0] == name:
                return
            if (not full) and entry.dn[0].startswith(name):
                return
        self.assertEquals(0, 1, msg="GLUE Entity %s does not exist." % name)

    def test_chunk_keys(self):
        for entry in self.entries:
            if 'ChunkKey' not in entry.glue:
                continue
            self.test_existence(entry.glue['ChunkKey'], full=True)
        
    def test_egee_site_unique_id(self):
        for entry in self.entries:
            if 'GlueSite' not in entry.objectClass:
                continue
            self.assertEquals(entry.glue['SiteName'], \
                entry.glue['SiteUniqueID'], msg="For EGEE compat., must have " \
                "GlueSiteName == GlueSiteUniqueID")

    def test_value_not_equal(self, objClass, attribute, value):
        for entry in self.entries:
            if objClass not in entry.objectClass:
                continue
            self.assertNotEquals(entry.glue[attribute], value, msg="GLUE " \
                "attribute %s for entity %s is equal to %s" % (attribute, \
                objClass, value))

