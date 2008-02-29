
"""
Common functions for GIP providers and plugins.

A set of general-purpose functions to help GIP plugin/provider authors write
probes which are consistent and correct.

This module should generally follow PEP 8 coding guidelines.
"""

__author__ = "Brian Bockelman"

import os
import ConfigParser
import sys
import re

from UserDict import UserDict

import gip_testing
from gip_testing import runCommand

# This evaluates to true if Python 2.3 or higher is
# available.
py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
"""
True if the current version of Python is 2.3 or higher; enables a few extra
capabilities which Python 2.2 does not have.
"""

if py23:
    import optparse, logging, logging.config

# Default log level for our FakeLogger object.
loglevel = "info"

def check_gip_location():
    """
    This function checks to make sure that GIP_LOCATION is set and exists.
    If GIP_LOCATION is not set and $VDT_LOCATION/gip exists, then it adds::

        GIP_LOCATION=$VDT_LOCATION/gip

    to the process's environment

    It raises ValueErrors if neither situation holds.

    This function is automatically run by the *config* function, so it is
    generally not necessary for provider authors to use this directly.
    """
    if "GIP_LOCATION" not in os.environ:
        vdt_loc = os.path.expandvars("$VDT_LOCATION/gip")
        if "VDT_LOCATION" in os.environ:
            if os.path.exists(vdt_loc):
                os.environ["GIP_LOCATION"] = vdt_loc
            else:
                raise ValueError("The $GIP_LOCATION variable is not set and " \
                    "$VDT_LOCATION/gip does not exist.  Please set the " \
                    "$GIP_LOCATION variable and run again.")
        else:
            raise ValueError("The $GIP_LOCATION and $VDT_LOCATION variables " \
                "are not set; please set $GIP_LOCATION to the proper path and" \
                " run this script again.")
    else:
        loc = os.path.expandvars("$GIP_LOCATION")
        if not os.path.exists(loc):
            raise ValueError("The $GIP_LOCATION variable points to %s, " \
                "which does not exist." % loc)

def check_testing_environment():
    """
    Check to see if the GIP_TESTING environment variable has been set.
    If so, set the gip_testing.replace_command variable to be true.  This causes
    the GIP to read output from static files instead of running the command.
    This is useful if you want to test, say, PBS on a laptop with no PBS 
    installed.
    """
    if 'GIP_TESTING' in os.environ:
        gip_testing.replace_command = True

def config(*args):
    """
    Load up the config file.  It's taken from the command line, option -c
    or --config; default is $GIP_LOCATION/etc/gip.conf

    If python 2.3 is not available, the command line option is not checked.

    If any arguments are supplied to this function, they will be interpreted
    as filenames for additional config files to read.  If the filename 
    considers environmental variables, they will be expanded.
    """
    check_gip_location()
    check_testing_environment()
    cp = ConfigParser.ConfigParser()
    files = list(args)
    if py23:
        p = optparse.OptionParser()
        p.add_option('-c', '--config', dest='config', \
            help='Configuration file.', default='gip.conf')
        (options, args) = p.parse_args()
        files += [i.strip() for i in options.config.split(',')]
    files += ["$GIP_LOCATION/etc/gip.conf"]
    files = [os.path.expandvars(i) for i in files]
    cp.read(files)
    
    # Set up the config object to be compatible with the OSG attributes
    # file.
    config_compat(cp)
    return cp

def config_compat(cp):
    """
    Currently, all of the configuration information is kept in::

        $VDT_LOCATION/monitoring/osg-attributes.conf

    This function will take in the ConfigParser object `cp` and update it
    with the configurations found from the OSG monitoring.

    If gip.override=True, then the config object overrides the OSG settings.
    If not, then the OSG settings override.

    If VDT_LOCATION is not defined, this function does nothing.
    """
    if "VDT_LOCATION" not in os.environ:
        return

    try:
        override = cp.getboolean("gip", "override")
    except:
        override = False
    osg = None
    try:
        osg = Attributes("$VDT_LOCATION/monitoring/osg-attributes.conf")
    except Exception, e:
        log.error("Unable to open OSG attributes: %s" % str(e))
        osg = None

    if osg != None:
        # Write the attributes from the flat attributes file to the
        # ConfigParser object, which is organized by sections.
        __write_config(cp, override, osg["OSG_HOSTNAME"], "ce", "name")
        __write_config(cp, override, osg["OSG_DEFAULT_SE"], "se", "name")
        __write_config(cp, override, osg["OSG_SITE_NAME"], "site", "name")
        __write_config(cp, override, osg["OSG_SITE_CITY"], "site", "city")
        __write_config(cp, override, osg["OSG_SITE_COUNTRY"], "site", "country")
        __write_config(cp, override, osg["OSG_SITE_LONGITUDE"], "site",
                       "longitude")
        __write_config(cp, override, osg["OSG_SITE_LATITUDE"], "site",
                       "latitude")
        __write_config(cp, override, osg["OSG_APP"], "osg_dirs", "app")
        __write_config(cp, override, osg["OSG_DATA"], "osg_dirs", "data")
        __write_config(cp, override, osg["OSG_WN_TMP"], "osg_dirs", "wn_tmp")
        __write_config(cp, override, osg["OSG_JOB_MANAGER"], "ce", 
                       "job_manager")

    # Do the same but with the gip stuff.
    try:
        gip = Attributes("$VDT_LOCATION/monitoring/gip-attributes.conf")
    except Exception, e:
        log.error("Unable to open GIP attributes: %s" % str(e))
        return

    if gip["OSG_GIP_SIMPLIFIED_SRM"].lower() == "y":
        #simple_path = os.path.join(gip["OSG_GIP_SIMPLIFIED_SRM_PATH"], "$VO")
        simple_path = gip["OSG_GIP_SIMPLIFIED_SRM_PATH"]
        __write_config(cp, override, simple_path, "vo", "default")
    for key in gip.keys():
        if key.startswith("OSG_GIP_VO_DIR"):
            vo, dir = gip[key].split(',')
            __write_config(cp, override, dir, "vo", vo)

def __write_config(cp, override, new_val, section, option):
    """
    Helper function for config_compat; should not be called directly.
    """
    if not cp.has_section(section):
        cp.add_section(section)
    if override and (not cp.has_option(section, option)):
        cp.set(section, option, new_val)
    elif (not override):
        cp.set(section, option, new_val)

def getOsgAttributes():
    """
    Return a dictionary-like object containing the OSG attributes as configured
    by the configure-osg.sh script and stored in the 
    $VDT_LOCATION/monitoring/osg-attributes.conf file.
    """
    return Attributes("$VDT_LOCATION/monitoring/osg-attributes.conf")

class VoMapper:
    """
    This class maps a username to VO.

    After it loads the username -> VO mapping, it can be used as a dictionary.

    The map_location variable holds the location of the user-to-vo map; this
    defaults to vo.user_vo_map in the config file.  The `parse` method
    re-parses the file.
    """

    def __init__(self, cp):
        self.cp = cp
        try:
            self.map_location = cp.get("vo", "user_vo_map")
        except:
            self.map_location = "$VDT_LOCATION/monitoring/osg-user-vo-map.txt"
        self.voi = []
        self.voc = []
        self.userMap = {}
        self.voMap = {}
        self.parse()
    

    def parse(self):
        """
        Parse the user-to-vo map specified at `self.map_location`
        """
        fp = open(os.path.expandvars(self.map_location), 'r')
        for line in fp:
            try:
                line = line.strip()
                if line.startswith("#voi"):
                    self.voi = line.split()[1:]
                elif line.startswith("#VOc"):
                    self.voc = line.split()[1:]
                elif line.startswith("#"):
                    continue
                else:
                    user, vo = line.split()
                    self.userMap[user] = vo
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass
        for i in range(len(self.voi)):
            try:
                 self.voMap[self.voi[i]] = self.voc[i]
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass

    def __getitem__(self, username):
        try: 
            return self.voMap[self.userMap[username]]
        except KeyError:
            raise ValueError("Unable to map user: %s" % username)

def FakeLogger():
    """
    Super simple logger for python installs which don't have the logging
    package.
    """
    def debug(msg, *args):
        print >> sys.stderr, msg

    def info(msg, *args):
        print >> sys.stderr, msg

    def warning(msg, *args):
        print >> sys.stderr, msg

    def error(msg, *args):
        print >> sys.stderr, msg

    def exception(msg, *args):
        print >> sys.stderr, msg

if py23:
    try:
        logging.config.fileConfig(os.path.expandvars("$GIP_LOCATION/etc/" \
            "logging.conf"))
    except:
        pass

def getLogger(name):
    """
    Returns a logger object corresponding to `name`.

    @param name: Name of the logger object.
    """
    if not py23:
        return FakeLogger()
    else: 
        return logging.getLogger(name)

log = getLogger("GIP.common")

def addToPath(new_element):
    """
    Add a directory to the path.
    """
    path = os.environ.get("PATH")
    path = "%s:%s" % (str(new_element), path)
    os.environ["PATH"] = path

def HMSToMin(hms):
    """
    Helper function to convert something of the form HH:MM:SS to number of 
    minutes.
    """
    h, m, s = hms.split(':')
    return int(h)*60 + int(m) + int(round(int(s)/60.0))

class _Constants:
        def __init__(self):
                self.CR = '\r'
                self.LF = '\n'

class Attributes(UserDict):
    """
    Given a filename containing attributes, parse it into a dictionary.
    """

    def __init__(self, attribute_file):
        UserDict.__init__(self, dict=None)
        self.constants = _Constants()
        self.load_attributes(attribute_file)

    def load_attributes(self, attribute_file):
        f = open(os.path.expandvars(attribute_file))
        s = f.read()
        e = s.split(self.constants.LF)
        if (len(e[len(e)-1]) == 0):
            e.pop()

        # Look for lines that match the pattern "key=value"
        # this will also stip out quotation marks
        test = re.compile('^(.*)=(.*)')
        for line in e:
            valid = test.match(line)
            if valid:
                grp = line.split("=")
                self[grp[0]] = grp[1][1:len(grp[1]) - 1]

def getTemplate(template, name):
    """
    Return a template from a file.

    @param template: Name of the template file in $GIP_LOCATION/templates.
    @param name: Entry in the template file; for now, this is the first
        entry of the DN.
    @return: Template string
    @raise e: ValueError if it is unable to find the template in the file.
    """
    fp = open(os.path.expandvars("$GIP_LOCATION/templates/%s" % template))
    start_str = "dn: %s" % name
    buffer = ''
    recording = False
    for line in fp:
        if line.startswith(start_str):
            recording = True
        if recording:
            buffer += line
            if line == '\n':
                break
    if not recording:
        raise ValueError("Unable to find %s in template %s" % (name, template))
    return buffer[:-1]

def printTemplate(template, info):
    """
    Print out the LDIF contained in template using the values from the 
    dictionary `info`.
    
    The different entries of the template are matched up to keys in the `info`
    dictionary; the entries' values are the dictionary values.

    To see what keys `info` needs for your template, read the template as
    found in::

        $GIP_LOCATION/templates

    @param info: Dictionary of information to fill out for the template.
        The keys correspond to the blank entries in the template string.
    @type info: Dictionary
    @param template: Template string returned from getTemplate.
    """
    print template % info

def voList(cp, vo_map=None):
    """
    Return the list of valid VOs for this install.  This data is taken from
    the vo mapper and the blacklist / whitelist in the "vo" section of the
    config parser `cp` is applied.

    @param cp: Configuration information for this site.
    @type cp: ConfigParser
    """
    if vo_map == None:
        vo_map = VoMapper(cp)
    vos = []
    for vo in vo_map.voc:
        vo = vo.lower()
        if vo not in vos:
            vos.append(vo)
    blacklist = [i.strip() for i in cp.get("vo", "vo_blacklist").split(',')]
    whitelist = [i.strip() for i in cp.get("vo", "vo_whitelist").split(',')]
    for vo in whitelist:
        vo = vo.lower()
        if vo not in vos:
            vos.append(vo)
    for vo in blacklist:
        vo = vo.lower()
        if vo in vos:
            vos.remove(vo)
    return vos


