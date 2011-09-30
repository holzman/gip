
"""
Testing framework for the GIP.

This allows one to replace output from command-line invocations with saved
outputs from the test/command_output directory.
"""

import os
import re
import sys
import types
import fcntl
import unittest
import time
import urlparse
import GipUnittest
import ConfigParser
import popen2
import select
import cStringIO

from gip_common import getLogger

from gip_common import cp_get, cp_getBoolean, pathFormatter, parseOpts, config
from gip_common import strContains
from gip_ldap import getSiteList, prettyDN

py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
if py23: import optparse

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
        fd = open(os.path.expandvars("$GIP_LOCATION/../test/command_output/" \
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
        return open(os.path.expandvars("$GIP_LOCATION/../test/command_output/%s" \
            % filename))
    else:
        # Modified from
        # http://code.activestate.com/recipes/52296-capturing-the-output-and-error-streams-from-a-unix/
        # (maybe someday we can use the subprocess module)
        
        child = popen2.Popen3(cmd, capturestderr=True)

        stdout = child.fromchild
        stderr = child.childerr

        outfd = stdout.fileno()
        errfd = stderr.fileno()

        outeof = erreof = 0
        outdata = cStringIO.StringIO()
        errdata = cStringIO.StringIO()

        fdlist = [outfd, errfd]
        for fd in fdlist: # make stdout/stderr nonblocking
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
            
        while fdlist:
            time.sleep(.001) # prevent 100% CPU spin 
            ready = select.select(fdlist, [], [])
            if outfd in ready[0]:
                outchunk = stdout.read()
                if outchunk == '':
                    fdlist.remove(outfd)
                else:
                    outdata.write(outchunk)
            if errfd in ready[0]:
                errchunk = stderr.read()
                if errchunk == '':
                    fdlist.remove(errfd)
                else:
                    errdata.write(errchunk)

        exitStatus = child.wait()
        outdata.seek(0)
        errdata.seek(0)
        
        if exitStatus:
            log = getLogger("GIP.common")
            log.info('Command %s exited with %d, stderr: %s' % (cmd, os.WEXITSTATUS(exitStatus), errdata.readlines()))

        return outdata

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
    #sites = getSiteList(cp)
    try:
        sites = cp_get(cp, "gip_tests", "site_names", "")
        if len(sites) > 0:
            sites = [i.strip() for i in sites.split(',')]
        else:
            sites = getSiteList(cp)
    except:
        sites = getSiteList(cp)

    kw, passed, args = parseOpts(sys.argv[1:])
    tests = []
    for site in sites:
        if len(args) > 0 and site not in args:
            continue
        if site == 'local' or site == 'grid':
            continue
        case = cls(site, cp)
        
        # try to set the cp object for GipUnittest children.
        #  if not a GipUnittest child, then fail out and continue as normal
        try:
            case.setCp(cp)
        except:
            pass

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
            + time.strftime("%A_%b_%d_%Y_%H_%M_%S")
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
    usexml = cp_getBoolean(cp, "gip_tests", "use_xml", default=False)
    if per_site:
        testSuite = generateTests(cp, cls, sys.argv[1:])
    else:
        testSuite = unittest.TestLoader().loadTestsFromTestCase(cls)

    if usexml:
        testRunner = GipUnittest.GipXmlTestRunner()
    else:
        if out is None:
            #testRunner = unittest.TextTestRunner(verbosity=2)
            testRunner = GipUnittest.GipTextTestRunner(verbosity=2)
        else:
            #testRunner = unittest.TextTestRunner(stream=out, verbosity=2)
            testRunner = GipUnittest.GipTextTestRunner(stream=out, verbosity=2)
    result = testRunner.run(testSuite)
    sys.exit(not result.wasSuccessful())

def runlcginfo(opt, bdii="is.grid.iu.edu", port="2170", VO="ops"):
    cmd = "lcg-info " + opt + " --vo " + VO + " --bdii " + bdii + ":" + port
    print >> sys.stderr, cmd
    return runCommand(cmd)

def runlcginfosites(bdii="is.grid.iu.edu", VO="ops", opts_list=[]):
    cmd = "lcg-infosites --is " + bdii + " --vo " + VO + " "
    for opt in opts_list:
        cmd += opt + " "
    return runCommand(cmd)

def interpolateConfig(cp):
    
    grid = cp_get(cp, "site", "group", "")
    if cp_getBoolean(cp, "gip_tests", "oim_aware", False):
        sitelist_cmd = "wget -O - http://oim.grid.iu.edu/pub/resource/show.php?format=plain-text 2>/dev/null | grep \",%s,\" | grep \",CE\" | cut -f1 -d," % grid
        sitelist = runCommand(sitelist_cmd).read().split()
        sitelist = ",".join(sitelist)
        cp.set("gip_tests", "site_names", sitelist)
    else:
        if cp_get(cp, "gip_tests", "site_names", "") == "":
            cp.set("gip_tests", "site_names", cp_get(cp, "site", "name", ""))

    if cp_get(cp, "gip_tests", "site_dns", "") == "":
        host_parts = cp_get(cp, "ce", "name", "").split('.')
        site_dns = "%s.%s" % (host_parts[:-2], host_parts[:-1])
        cp.set("gip_tests", "site_dns", site_dns)

    if cp_get(cp, "gip_tests", "required_site", "") == "":
        cp.set("gip_tests", "required_sites", cp_get(cp, "gip_tests", "site_names", ""))

    cp.set("gip_tests", "bdii_port", "2170")
    cp.set("gip_tests", "egee_port", "2170")
    cp.set("gip_tests", "interop_url", "http://oim.grid.iu.edu/publisher/get_osg_interop_bdii_ldap_list.php?grid=%s&format=html" % grid)
        
    if strContains(grid, "ITB"):
        cp.set("bdii", "endpoint", "ldap://is-itb.grid.iu.edu:2170")
        cp.set("gip_tests", "bdii_addr", "is-itb.grid.iu.edu")
        cp.set("gip_tests", "egee_bdii", "pps-bdii.cern.ch")
        cp.set("gip_tests", "egee_bdii_conf_url", "http://egee-pre-production-service.web.cern.ch/egee-pre-production-service/bdii/pps-all-sites.conf")
        web_server = "http://is-itb.grid.iu.edu"
    else:
        cp.set("gip_tests", "bdii_addr", "is.grid.iu.edu")
        cp.set("gip_tests", "egee_bdii", "lcg-bdii.cern.ch")
        cp.set("gip_tests", "egee_bdii_conf_url", "http://lcg-bdii-conf.cern.ch/bdii-conf/bdii.conf")
        web_server = "http://is.grid.iu.edu"

    cp.set("gip_tests", "update_url", web_server + "/cgi-bin/status.cgi")
    cp.set("gip_tests", "schema_check_url", web_server + "/data/cemon_processed_osg/%s.processed?which=%s")
    cp.set("gip_tests", "validator_url", web_server + "/data/cemon_processed_osg/%s.processed?which=%s")

    if cp_get(cp, "gip_tests", "compare_excludes", "") == "":
        compare_excludes="GlueCEStateFreeJobSlots,GlueCEStateRunningJobs,GlueCEStateTotalJobs,GlueSiteLocation,GlueSAStateAvailableSpace,GlueSAStateUsedSpace"
        cp.set("gip_tests", "compare_excludes", compare_excludes)

    if cp_get(cp, "gip_tests", "enable_glite", "") == "":
        cp.set("gip_tests", "enable_glite", "False")

    if cp_get(cp, "gip_tests", "results_dir", "") == "":
        results_dir = os.path.expandvars("$GIP_LOCATION/../apache/htdocs/")
        cp.set("gip_tests", "results_dir", results_dir)

def getTestConfig(*args):
    cp = config(args[1:])
    try:
        cp.readfp(open(os.path.expandvars('$GIP_LOCATION/etc/gip_tests.conf')))
    except:
        pass

    interpolateConfig(cp)

    section = "gip_tests"
    if not cp.has_section(section):
        cp.add_section(section)

    if py23:
        p = optparse.OptionParser()
        p.add_option('-c', '--config', dest='config', help='Configuration file.', default='gip.conf')
        p.add_option('-f', '--format', dest='format', help='Unittest output format', default='xml')
        (options, args) = p.parse_args()
        xml = options.format
    else:
        keywordOpts, passedOpts, givenOpts = parseOpts(args)
        if keywordOpts["format"]:
             xml = keywordOpts["format"]
        if keywordOpts["f"]:
             xml = keywordOpts["f"]

    try:
        if xml == "xml":
            cp.set(section, "use_xml", "True")
        else:
            cp.set(section, "use_xml", "False")
    except:
        cp.set(section, "use_xml", "False")

    return cp
