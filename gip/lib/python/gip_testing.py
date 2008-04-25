
"""
Testing framework for the GIP.

This allows one to replace output from command-line invocations with saved
outputs from the test/command_output directory.
"""

import os
import re
import sys
import unittest
import datetime

from gip_common import cp_get, pathFormatter, parseOpts
from gip_ldap import getSiteList

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
