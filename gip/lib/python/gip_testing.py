
"""
Testing framework for the GIP.

This allows one to replace output from command-line invocations with saved
outputs from the test/command_output directory.
"""

import os
import sys
import unittest

from ldap import getSiteList

replace_command = False

commands = {}

def lookupCommand(cmd):
    cmd = cmd.strip()
    if cmd not in commands:
        fd = open(os.path.expandvars("$VDT_LOCATION/test/command_output/" \
            "commands"))
        for line in fd:
            if line.startswith("#") or len(line.strip()) == 0:
                continue
            command, val = line.split(':', 1)
            val = val.strip()
            commands[val.strip()] = command.strip()
    return commands[cmd]

def runCommand(cmd, force_command=False):
    if replace_command and not force_command:
        try:
            filename = lookupCommand(cmd)
        except Exception, e:
            print e
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
    tests = []
    for site in sites:
        if len(args) > 0 and site not in args:
            continue
        if site == 'local' or site == 'grid':
            continue
        case = cls(site, cp)
        tests.append(case)
    return unittest.TestSuite(tests)

def runBdiiTest(cp, cls):
    """
    Given a test class, generate and run a test suite

    @param cp: Site configuration
    @type cp: ConfigParser
    @param cls: Test class to use to generate a test suite.  It is assumed
        that the constructor for this class has signature cls(cp, site_name)
    @type cls: class
    """
    testSuite = generateTests(cp, cls, sys.argv[1:])
    testRunner = unittest.TextTestRunner(verbosity=2)
    result = testRunner.run(testSuite)
    sys.exit(not result.wasSuccessful())

